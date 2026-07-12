import { Redis } from "ioredis";
import { keys, streamNameFromKey } from "../redis/keys.js";
import { StreamEvent, fromEntry } from "./events.js";

export type FanoutListener = (event: StreamEvent) => void;

/**
 * One blocking XREAD loop per process, fanned out to in-memory listeners.
 * Broadcast semantics: every listener sees every event on the streams it
 * watches. Delivery state (who wants what) lives in process memory next to
 * the MCP transports that consume it — not in Redis consumer groups.
 */
export class Fanout {
  private listeners = new Map<string, Set<FanoutListener>>(); // stream name -> listeners
  private lastIds = new Map<string, string>(); // stream key -> last delivered entry id
  private running = false;
  private loopPromise?: Promise<void>;

  constructor(
    private readonly blocking: Redis,
    private readonly main: Redis,
  ) {}

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loopPromise = this.loop();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.wake(); // interrupt a parked XREAD so the loop can exit
    await this.loopPromise?.catch(() => {});
  }

  /**
   * Watch a stream; resolves once the subscription is armed — every event
   * published after this resolves is guaranteed to be delivered. The cursor
   * is snapshotted here, before returning, so subscribe-then-publish can
   * never skip the published event.
   */
  async subscribe(stream: string, listener: FanoutListener): Promise<() => void> {
    let set = this.listeners.get(stream);
    const isNewStream = !set;
    if (!set) {
      set = new Set();
      this.listeners.set(stream, set);
    }
    set.add(listener);
    if (isNewStream) {
      // Tail from now; replay is the client's job via read_stream.
      const key = keys.stream(stream);
      if (!this.lastIds.has(key)) {
        try {
          this.lastIds.set(key, await this.resolveCursor(key));
        } catch {
          this.lastIds.set(key, "0-0");
        }
      }
      await this.wake();
    }
    return () => {
      const s = this.listeners.get(stream);
      if (!s) return;
      s.delete(listener);
      if (s.size === 0) {
        this.listeners.delete(stream);
        this.lastIds.delete(keys.stream(stream));
      }
    };
  }

  watchedStreams(): string[] {
    return [...this.listeners.keys()];
  }

  private async wake(): Promise<void> {
    try {
      await this.main.xadd(keys.wakeStream, "MAXLEN", "~", 8, "*", "wake", "1");
    } catch {
      /* wake is best-effort */
    }
  }

  /**
   * Resolve a cursor for streams we haven't read yet. Never pass `$` into the
   * XREAD loop: a `$` re-sent on the next iteration would skip any entries
   * that arrived while we were processing other streams' results.
   */
  private async resolveCursor(key: string): Promise<string> {
    const latest = (await this.main.xrevrange(key, "+", "-", "COUNT", 1)) as [string, string[]][];
    return latest[0]?.[0] ?? "0-0";
  }

  private async loop(): Promise<void> {
    while (this.running) {
      const streamKeys = [keys.wakeStream, ...[...this.listeners.keys()].map((s) => keys.stream(s))];
      for (const key of streamKeys) {
        if (!this.lastIds.has(key)) {
          try {
            this.lastIds.set(key, await this.resolveCursor(key));
          } catch {
            this.lastIds.set(key, "0-0");
          }
        }
      }
      const ids = streamKeys.map((k) => this.lastIds.get(k) ?? "0-0");

      let res: [string, [string, string[]][]][] | null = null;
      try {
        res = (await this.blocking.xread("BLOCK", 5000, "STREAMS", ...streamKeys, ...ids)) as
          | [string, [string, string[]][]][]
          | null;
      } catch (err) {
        if (!this.running) break;
        console.error("[fanout] xread error, backing off:", err);
        await new Promise((r) => setTimeout(r, 1000));
        continue;
      }
      if (!res) continue;

      for (const [key, entries] of res) {
        const last = entries[entries.length - 1];
        if (last) this.lastIds.set(key, last[0]);
        if (key === keys.wakeStream) continue; // control traffic only

        const stream = streamNameFromKey(key);
        const set = this.listeners.get(stream);
        if (!set || set.size === 0) continue;
        for (const [id, raw] of entries) {
          const event = fromEntry(stream, id, raw);
          for (const listener of set) {
            try {
              listener(event);
            } catch (err) {
              console.error("[fanout] listener error:", err);
            }
          }
        }
      }
    }
  }
}
