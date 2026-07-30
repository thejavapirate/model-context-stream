import { Redis } from "ioredis";
import { keys } from "../redis/keys.js";

export interface PresenceRecord {
  sessionId: string;
  agent: string;
  connectedAt: string;
  /** ISO — refreshed on every heartbeat. */
  lastSeenAt: string;
  /** Resource URIs this session is subscribed to. */
  subscriptions: string[];
}

export interface PresenceSettings {
  /** Record TTL — a crashed replica's sessions ghost out within this. */
  ttlSec: number;
  /** Heartbeat interval; keep ≤ ttlSec/3 so liveness survives missed beats. */
  heartbeatMs: number;
}

const DEFAULT_SETTINGS: PresenceSettings = { ttlSec: 45, heartbeatMs: 15_000 };

/**
 * Fleet-global presence backed by Redis. Each replica writes TTL'd records for
 * ITS sessions; roster() reads the union across all replicas. The string key's
 * server-side TTL is the liveness authority (immune to replica clock skew);
 * the ZSET index is hygiene only and self-prunes on read.
 *
 * The roster is the source of truth — agent.connected/disconnected stream
 * events are best-effort hints: a crashed replica never emits disconnects, but
 * its sessions age out of the roster within ttlSec.
 */
export class PresenceService {
  private timer?: NodeJS.Timeout;

  constructor(
    private readonly redis: Redis,
    private readonly settings: PresenceSettings = DEFAULT_SETTINGS,
  ) {}

  /** `provider` returns THIS replica's live sessions; they are re-upserted every beat. */
  start(provider: () => PresenceRecord[]): void {
    this.timer = setInterval(() => {
      void this.upsertAll(provider()).catch((err) =>
        console.error("[presence] heartbeat:", err instanceof Error ? err.message : err),
      );
    }, this.settings.heartbeatMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
  }

  async upsert(record: PresenceRecord): Promise<void> {
    await this.upsertAll([record]);
  }

  async upsertAll(records: PresenceRecord[]): Promise<void> {
    if (records.length === 0) return;
    const expiresAt = Date.now() + this.settings.ttlSec * 1000;
    const pipe = this.redis.pipeline();
    for (const r of records) {
      pipe.set(keys.presence(r.sessionId), JSON.stringify(r), "EX", this.settings.ttlSec);
      pipe.zadd(keys.presenceIndex, expiresAt, r.sessionId);
    }
    await pipe.exec();
  }

  async remove(sessionId: string): Promise<void> {
    const pipe = this.redis.pipeline();
    pipe.del(keys.presence(sessionId));
    pipe.zrem(keys.presenceIndex, sessionId);
    await pipe.exec();
  }

  /** Fleet-wide roster, sorted by agent then sessionId. Prunes stale index entries as it reads. */
  async roster(): Promise<PresenceRecord[]> {
    await this.redis.zremrangebyscore(keys.presenceIndex, "-inf", Date.now());
    const ids = await this.redis.zrange(keys.presenceIndex, 0, -1);
    if (ids.length === 0) return [];
    const raw = await this.redis.mget(ids.map((id) => keys.presence(id)));

    const out: PresenceRecord[] = [];
    const stale: string[] = [];
    ids.forEach((id, i) => {
      const json = raw[i];
      if (!json) {
        stale.push(id); // key TTL'd out (or clock-skew over-prune healed by the next beat)
        return;
      }
      try {
        out.push(JSON.parse(json) as PresenceRecord);
      } catch {
        stale.push(id);
      }
    });
    if (stale.length > 0) await this.redis.zrem(keys.presenceIndex, ...stale);
    return out.sort((a, b) => a.agent.localeCompare(b.agent) || a.sessionId.localeCompare(b.sessionId));
  }
}
