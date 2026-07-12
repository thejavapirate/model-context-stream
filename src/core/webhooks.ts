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
  createdBy: string;
  createdAt: string;
  disabled: boolean;
  consecutiveFailures: number;
}

export interface WebhookSettings {
  timeoutMs: number;
  /** Backoff between delivery attempts of ONE event. */
  attemptDelaysMs: number[];
  /** Disable the webhook after this many consecutive failed deliveries. */
  disableAfterFailures: number;
}

const DEFAULT_SETTINGS: WebhookSettings = {
  timeoutMs: 10_000,
  attemptDelaysMs: [1_000, 5_000, 25_000],
  disableAfterFailures: 20,
};

/**
 * The mirror of ingest: events on a stream → HTTP POST to an external URL.
 * Deliveries are per-webhook sequential (promise chain) so a slow endpoint
 * never floods or reorders; failures back off and eventually disable the hook.
 */
export class WebhookService {
  private hooks = new Map<string, Webhook>();
  private unsubs = new Map<string, () => void>();
  private queues = new Map<string, Promise<void>>();
  private stopped = false;

  constructor(
    private readonly redis: Redis,
    private readonly fanout: Fanout,
    private readonly streams: StreamService,
    private readonly settings: WebhookSettings = DEFAULT_SETTINGS,
  ) {}

  async start(): Promise<void> {
    const raw = await this.redis.hgetall(keys.webhooks);
    for (const json of Object.values(raw)) {
      try {
        const hook = JSON.parse(json) as Webhook;
        this.hooks.set(hook.id, hook);
        if (!hook.disabled) await this.arm(hook);
      } catch {
        /* skip malformed record */
      }
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    for (const unsub of this.unsubs.values()) unsub();
    this.unsubs.clear();
    await Promise.allSettled([...this.queues.values()]);
  }

  async add(input: {
    stream: string;
    url: string;
    secret?: string;
    types?: string[];
    createdBy: string;
  }): Promise<Webhook> {
    const hook: Webhook = {
      id: `wh_${nanoid(10)}`,
      stream: input.stream,
      url: input.url,
      ...(input.secret ? { secret: input.secret } : {}),
      ...(input.types?.length ? { types: input.types } : {}),
      createdBy: input.createdBy,
      createdAt: new Date().toISOString(),
      disabled: false,
      consecutiveFailures: 0,
    };
    await this.persist(hook);
    this.hooks.set(hook.id, hook);
    await this.arm(hook);
    return hook;
  }

  async remove(id: string): Promise<boolean> {
    const existed = this.hooks.delete(id);
    this.unsubs.get(id)?.();
    this.unsubs.delete(id);
    await this.redis.hdel(keys.webhooks, id);
    return existed;
  }

  /** Secrets redacted — safe to return to agents. */
  list(): Array<Omit<Webhook, "secret"> & { hasSecret: boolean }> {
    return [...this.hooks.values()].map(({ secret, ...rest }) => ({
      ...rest,
      hasSecret: Boolean(secret),
    }));
  }

  private async persist(hook: Webhook): Promise<void> {
    await this.redis.hset(keys.webhooks, hook.id, JSON.stringify(hook));
  }

  private async arm(hook: Webhook): Promise<void> {
    const unsub = await this.fanout.subscribe(hook.stream, (event) => {
      const current = this.hooks.get(hook.id);
      if (!current || current.disabled) return;
      if (current.types?.length && !current.types.includes(event.type)) return;
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
    if (!hook || hook.disabled || this.stopped) return;

    let lastError = "";
    for (let attempt = 0; attempt <= this.settings.attemptDelaysMs.length; attempt++) {
      if (attempt > 0) {
        await sleep(this.settings.attemptDelaysMs[attempt - 1]!);
        if (this.stopped || this.hooks.get(hookId)?.disabled) return;
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
    const body = JSON.stringify(event);
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
      signal: AbortSignal.timeout(this.settings.timeoutMs),
    });
    if (!res.ok) throw new Error(`endpoint returned ${res.status}`);
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
