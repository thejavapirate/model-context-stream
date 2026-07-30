import { hostname } from "node:os";
import { Redis } from "ioredis";
import { nanoid } from "nanoid";
import { keys } from "../redis/keys.js";

const CAS_EXPIRE = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) else return 0 end`;
const CAS_DELETE = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) else return 0 end`;

/** Delete `key` only if it still holds `expected`. Shared by the lease and the digest marker. */
export async function casDelete(redis: Redis, key: string, expected: string): Promise<boolean> {
  return (await redis.eval(CAS_DELETE, 1, key, expected)) === 1;
}

export interface CoordinatorSettings {
  /** Lease TTL — a crashed leader is replaced within this window. */
  ttlMs: number;
  /** Tick interval: leaders refresh, standbys attempt acquisition. Keep ≤ ttlMs/3. */
  checkMs: number;
}

const DEFAULT_SETTINGS: CoordinatorSettings = { ttlMs: 15_000, checkMs: 5_000 };

export interface CoordinatorHooks {
  /** Became leader — arm leader-only services. Errors are logged; leadership stands (armed services self-heal via resync). */
  onGain: () => Promise<void> | void;
  /** Lost leadership (lease expired/taken or clean stop) — disarm leader-only services. */
  onLose: () => Promise<void> | void;
}

/**
 * Single-key Redis lease electing ONE process fleet-wide to run work that must
 * not be duplicated across replicas (webhook delivery, digest scheduling).
 * Liveness is Redis-side (PX TTL) — immune to replica clock skew. On the usual
 * single-replica deploy the sole process acquires on boot and behavior is
 * identical to pre-lease versions.
 *
 * Failover contract: clean stop hands off within ~checkMs; a crashed leader is
 * replaced within ttlMs + checkMs. A stalled-but-alive leader disarms at its
 * next refresh (≤ checkMs), so consumers of leader-only work should tolerate a
 * brief dual-active overlap (webhook receivers: dedup on event.id).
 */
export class CoordinatorLease {
  readonly instanceId = `${hostname()}#${process.pid}#${nanoid(6)}`;
  private timer?: NodeJS.Timeout;
  private ticking = false;
  private stopped = false;
  private leader = false;

  constructor(
    private readonly redis: Redis,
    private readonly hooks: CoordinatorHooks,
    private readonly settings: CoordinatorSettings = DEFAULT_SETTINGS,
  ) {}

  get isLeader(): boolean {
    return this.leader;
  }

  start(): void {
    this.timer = setInterval(() => void this.tick(), this.settings.checkMs);
    this.timer.unref();
    void this.tick(); // immediate first attempt: single-replica boots arm without waiting a tick
  }

  async stop(): Promise<void> {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    if (this.leader) {
      this.leader = false;
      await casDelete(this.redis, keys.coordinator, this.instanceId).catch(() => {});
      await this.safeLose();
    }
  }

  private async tick(): Promise<void> {
    if (this.ticking || this.stopped) return;
    this.ticking = true;
    try {
      if (!this.leader) {
        const ok = await this.redis.set(keys.coordinator, this.instanceId, "PX", this.settings.ttlMs, "NX");
        if (ok === "OK" && !this.stopped) {
          this.leader = true;
          console.log(`[coordinator] ${this.instanceId} acquired leadership`);
          await this.safeGain();
        }
      } else {
        const refreshed = await this.redis.eval(CAS_EXPIRE, 1, keys.coordinator, this.instanceId, String(this.settings.ttlMs));
        if (refreshed !== 1 && !this.stopped && this.leader) {
          this.leader = false;
          console.warn(`[coordinator] ${this.instanceId} lost leadership`);
          await this.safeLose();
        }
      }
    } catch (err) {
      console.error("[coordinator] tick:", err instanceof Error ? err.message : err);
    } finally {
      this.ticking = false;
    }
  }

  private async safeGain(): Promise<void> {
    try {
      await this.hooks.onGain();
    } catch (err) {
      console.error("[coordinator] onGain:", err instanceof Error ? err.message : err);
    }
  }

  private async safeLose(): Promise<void> {
    try {
      await this.hooks.onLose();
    } catch (err) {
      console.error("[coordinator] onLose:", err instanceof Error ? err.message : err);
    }
  }
}
