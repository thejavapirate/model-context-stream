import { createHmac } from "node:crypto";
import { Redis } from "ioredis";
import { nanoid } from "nanoid";
import { metrics } from "../metrics.js";
import { keys, SYSTEM_STREAMS } from "../redis/keys.js";
import type { StreamEvent } from "./events.js";
import type { Fanout } from "./fanout.js";
import type { StreamService } from "./streams.js";

export interface Webhook {
  id: string;
  stream: string;
  url: string;
  secret?: string;
  /** Optional event-type allowlist; empty/absent = all types. */
  types?: string[];
  /** "wake" = agent-scoped wake registration; absent/"webhook" = admin data webhook. */
  kind?: "webhook" | "wake";
  /** Agent identity that owns a wake registration (authorization scope for list/remove). */
  owner?: string;
  /** Wakes only: fire at most once per this many seconds (source-side debounce). */
  debounceSec?: number;
  createdBy: string;
  createdAt: string;
  disabled: boolean;
  consecutiveFailures: number;
}

/**
 * SSRF hardening for agent-registered wake URLs. Private/loopback ranges are
 * deliberately ALLOWED — self-hosted runners live on private networks (the
 * documented trust model: quota + audit events + operator egress control) —
 * but there is no legitimate use for credentials-in-URL or non-http schemes.
 */
export function validateWakeUrl(raw: string): string | null {
  let url: URL;
  try {
    url = new URL(raw);
  } catch {
    return "invalid URL";
  }
  if (url.protocol !== "http:" && url.protocol !== "https:") return "wake URLs must be http(s)";
  if (url.username || url.password) return "wake URLs must not embed credentials";
  return null;
}

/** A wake POST body: the triggering event plus the cursor anchor to catch up from. */
export interface WakeEnvelope {
  wake: { registrationId: string; stream: string; owner?: string; debounceSec?: number };
  event: StreamEvent;
  cursorAnchor: string;
}

export interface WebhookSettings {
  timeoutMs: number;
  /** Backoff between delivery attempts of ONE event. */
  attemptDelaysMs: number[];
  /** Disable the webhook after this many consecutive failed deliveries. */
  disableAfterFailures: number;
  /** While armed, full re-sync from Redis this often (insurance for missed webhook.added/removed events). */
  resyncIntervalMs: number;
}

const DEFAULT_SETTINGS: WebhookSettings = {
  timeoutMs: 10_000,
  attemptDelaysMs: [1_000, 5_000, 25_000],
  disableAfterFailures: 20,
  resyncIntervalMs: 30_000,
};

/**
 * The mirror of ingest: events on a stream → HTTP POST to an external URL.
 * Deliveries are per-webhook sequential (promise chain) so a slow endpoint
 * never floods or reorders; failures back off and eventually disable the hook.
 *
 * Control plane vs delivery plane: add/remove/list work on ANY replica (Redis
 * is the registry; changes are announced on stream://system). Delivery is
 * armed on exactly ONE replica — the coordinator-lease holder calls
 * activate()/deactivate(). That keeps failure counters single-writer and
 * preserves the at-most-once, tail-from-now delivery contract. Failover loses
 * events published during the handover gap (≤ lease TTL + a tick — the same
 * class of loss as a single-replica restart) and a stalled-but-alive leader
 * can double-deliver for ≤ one tick: receivers should dedup on event.id.
 */
export class WebhookService {
  /** Delivery cache while armed; the Redis hash is the registry of record. */
  private hooks = new Map<string, Webhook>();
  private unsubs = new Map<string, () => void>();
  private queues = new Map<string, Promise<void>>();
  /** Wake debounce timestamps (leader-memory: failover resets — at worst one early wake). */
  private lastWake = new Map<string, number>();
  private stopped = false;
  private armed = false;
  private systemUnsub?: () => void;
  private resyncTimer?: NodeJS.Timeout;

  constructor(
    private readonly redis: Redis,
    private readonly fanout: Fanout,
    private readonly streams: StreamService,
    private readonly settings: WebhookSettings = DEFAULT_SETTINGS,
  ) {}

  /** Leader-only: arm delivery for every enabled hook and keep the set in sync. */
  async activate(): Promise<void> {
    if (this.armed || this.stopped) return;
    this.armed = true;
    await this.syncFromRedis();
    // Targeted sync: add/remove on any replica announces on stream://system.
    this.systemUnsub = await this.fanout.subscribe(SYSTEM_STREAMS.system, (event) => {
      if (event.type === "webhook.added" || event.type === "webhook.removed") {
        void this.syncFromRedis().catch((err) => console.error("[webhooks] sync:", err?.message ?? err));
      }
    });
    this.resyncTimer = setInterval(
      () => void this.syncFromRedis().catch((err) => console.error("[webhooks] resync:", err?.message ?? err)),
      this.settings.resyncIntervalMs,
    );
    this.resyncTimer.unref();
  }

  /** Disarm delivery (lease lost or shutting down); drains in-flight queues. */
  async deactivate(): Promise<void> {
    if (!this.armed) return;
    this.armed = false;
    if (this.resyncTimer) clearInterval(this.resyncTimer);
    this.systemUnsub?.();
    this.systemUnsub = undefined;
    for (const unsub of this.unsubs.values()) unsub();
    this.unsubs.clear();
    await Promise.allSettled([...this.queues.values()]);
    this.queues.clear();
    this.hooks.clear();
    this.lastWake.clear();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await this.deactivate();
  }

  /** Reconcile the armed set with the Redis registry (arm new, disarm removed/disabled). */
  private async syncFromRedis(): Promise<void> {
    if (!this.armed) return;
    const raw = await this.redis.hgetall(keys.webhooks);
    const seen = new Set<string>();
    for (const json of Object.values(raw)) {
      let hook: Webhook;
      try {
        hook = JSON.parse(json) as Webhook;
      } catch {
        continue; // skip malformed record
      }
      seen.add(hook.id);
      this.hooks.set(hook.id, hook);
      if (hook.disabled) {
        this.unsubs.get(hook.id)?.();
        this.unsubs.delete(hook.id);
      } else if (!this.unsubs.has(hook.id)) {
        await this.arm(hook);
      }
    }
    for (const id of [...this.hooks.keys()]) {
      if (!seen.has(id)) {
        this.unsubs.get(id)?.();
        this.unsubs.delete(id);
        this.hooks.delete(id);
      }
    }
  }

  async add(input: {
    stream: string;
    url: string;
    secret?: string;
    types?: string[];
    createdBy: string;
    kind?: "webhook" | "wake";
    owner?: string;
    debounceSec?: number;
  }): Promise<Webhook> {
    const hook: Webhook = {
      id: `wh_${nanoid(10)}`,
      stream: input.stream,
      url: input.url,
      ...(input.secret ? { secret: input.secret } : {}),
      ...(input.types?.length ? { types: input.types } : {}),
      ...(input.kind === "wake"
        ? {
            kind: "wake" as const,
            ...(input.owner ? { owner: input.owner } : {}),
            debounceSec: Math.min(Math.max(Math.round(input.debounceSec ?? 60), 1), 3600),
          }
        : {}),
      createdBy: input.createdBy,
      createdAt: new Date().toISOString(),
      disabled: false,
      consecutiveFailures: 0,
    };
    await this.persist(hook);
    if (this.armed) {
      this.hooks.set(hook.id, hook);
      await this.arm(hook);
    }
    this.announce("webhook.added", hook.id, hook.stream, hook.kind, hook.owner);
    return hook;
  }

  async remove(id: string): Promise<boolean> {
    const existed = (await this.redis.hdel(keys.webhooks, id)) === 1;
    this.unsubs.get(id)?.();
    this.unsubs.delete(id);
    this.hooks.delete(id);
    this.lastWake.delete(id);
    if (existed) this.announce("webhook.removed", id);
    return existed;
  }

  /** Remove with ownership check: only the registering agent (or an admin) may remove a wake. */
  async removeOwned(id: string, owner: string, isAdmin: boolean): Promise<"removed" | "not_found" | "forbidden"> {
    const raw = await this.redis.hget(keys.webhooks, id);
    if (!raw) return "not_found";
    let rec: Webhook;
    try {
      rec = JSON.parse(raw) as Webhook;
    } catch {
      return "not_found";
    }
    if (!isAdmin && !(rec.kind === "wake" && rec.owner === owner)) return "forbidden";
    await this.remove(id);
    return "removed";
  }

  /** One agent's wake registrations (redacted). */
  async listByOwner(owner: string): Promise<Array<Omit<Webhook, "secret"> & { hasSecret: boolean }>> {
    return (await this.list()).filter((h) => h.kind === "wake" && h.owner === owner);
  }

  async countByOwner(owner: string): Promise<number> {
    return (await this.listByOwner(owner)).length;
  }

  /** Registry of record (works on any replica). Secrets redacted — safe to return to agents. */
  async list(): Promise<Array<Omit<Webhook, "secret"> & { hasSecret: boolean }>> {
    const raw = await this.redis.hgetall(keys.webhooks);
    const out: Array<Omit<Webhook, "secret"> & { hasSecret: boolean }> = [];
    for (const json of Object.values(raw)) {
      try {
        const { secret, ...rest } = JSON.parse(json) as Webhook;
        out.push({ ...rest, hasSecret: Boolean(secret) });
      } catch {
        /* skip malformed record */
      }
    }
    return out.sort((a, b) => a.createdAt.localeCompare(b.createdAt) || a.id.localeCompare(b.id));
  }

  /** Best-effort: a missed announcement is healed by the leader's periodic resync.
   * Also the audit trail — wake registrations carry kind + owner on stream://system. */
  private announce(
    type: "webhook.added" | "webhook.removed",
    webhookId: string,
    stream?: string,
    kind?: string,
    owner?: string,
  ): void {
    void this.streams
      .publish({
        stream: SYSTEM_STREAMS.system,
        type,
        source: "system",
        payload: {
          webhookId,
          ...(stream ? { stream } : {}),
          ...(kind ? { kind } : {}),
          ...(owner ? { owner } : {}),
        },
      })
      .catch((err) => console.error("[webhooks] announce failed:", err?.message ?? err));
  }

  private async persist(hook: Webhook): Promise<void> {
    await this.redis.hset(keys.webhooks, hook.id, JSON.stringify(hook));
  }

  private async arm(hook: Webhook): Promise<void> {
    const unsub = await this.fanout.subscribe(hook.stream, (event) => {
      const current = this.hooks.get(hook.id);
      if (!current || current.disabled) return;
      if (current.types?.length && !current.types.includes(event.type)) return;
      if (current.kind === "wake") {
        // Source-side debounce: the receiver starts an expensive session — 50 events
        // should be ONE wake. Dropped events are not lost: the woken session catches
        // up from its durable cursor.
        const windowMs = (current.debounceSec ?? 60) * 1000;
        const last = this.lastWake.get(current.id) ?? 0;
        if (Date.now() - last < windowMs) return;
        this.lastWake.set(current.id, Date.now());
      }
      this.enqueue(current.id, event);
    });
    this.unsubs.set(hook.id, unsub);
  }

  /** Per-webhook sequential queue. */
  private enqueue(hookId: string, event: StreamEvent): void {
    const prev = this.queues.get(hookId) ?? Promise.resolve();
    const next = prev.then(() => this.deliverWithRetry(hookId, event)).catch(() => {});
    this.queues.set(hookId, next);
  }

  private async deliverWithRetry(hookId: string, event: StreamEvent): Promise<void> {
    const hook = this.hooks.get(hookId);
    if (!hook || hook.disabled || this.stopped || !this.armed) return;

    let lastError = "";
    for (let attempt = 0; attempt <= this.settings.attemptDelaysMs.length; attempt++) {
      if (attempt > 0) {
        await sleep(this.settings.attemptDelaysMs[attempt - 1]!);
        if (this.stopped || !this.armed || this.hooks.get(hookId)?.disabled) return;
      }
      try {
        await this.deliver(hook, event);
        if (hook.consecutiveFailures > 0) {
          hook.consecutiveFailures = 0;
          await this.persist(hook);
        }
        return;
      } catch (err) {
        lastError = err instanceof Error ? err.message : String(err);
      }
    }

    metrics.webhookFailures.inc();
    hook.consecutiveFailures += 1;
    if (hook.consecutiveFailures >= this.settings.disableAfterFailures) {
      hook.disabled = true;
      this.unsubs.get(hook.id)?.();
      this.unsubs.delete(hook.id);
      console.error(`[webhooks] disabling ${hook.id} after ${hook.consecutiveFailures} consecutive failures`);
      void this.streams
        .publish({
          stream: SYSTEM_STREAMS.system,
          type: "webhook.disabled",
          source: "system",
          payload: { webhookId: hook.id, stream: hook.stream, url: hook.url, lastError },
        })
        .catch(() => {});
    }
    await this.persist(hook);
  }

  private async deliver(hook: Webhook, event: StreamEvent): Promise<void> {
    // Wakes get an envelope (event + cursor anchor) so the woken agent can catch up
    // exactly; data webhooks keep the raw event body.
    const body = JSON.stringify(
      hook.kind === "wake"
        ? ({
            wake: {
              registrationId: hook.id,
              stream: hook.stream,
              ...(hook.owner ? { owner: hook.owner } : {}),
              ...(hook.debounceSec ? { debounceSec: hook.debounceSec } : {}),
            },
            event,
            cursorAnchor: event.id,
          } satisfies WakeEnvelope)
        : event,
    );
    const headers: Record<string, string> = {
      "content-type": "application/json",
      "x-mcs-stream": event.stream,
      "x-mcs-event-type": event.type,
      "x-mcs-delivery": nanoid(12),
    };
    if (hook.secret) {
      headers["x-mcs-signature"] = `sha256=${createHmac("sha256", hook.secret).update(body).digest("hex")}`;
    }
    const res = await fetch(hook.url, {
      method: "POST",
      headers,
      body,
      // Never follow redirects: a 3xx could bounce the signed body to a
      // different host than the one that was registered.
      redirect: "manual",
      signal: AbortSignal.timeout(this.settings.timeoutMs),
    });
    if (res.status >= 300 && res.status < 400) throw new Error(`endpoint redirected (${res.status}) — not followed`);
    if (!res.ok) throw new Error(`endpoint returned ${res.status}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
