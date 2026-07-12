import { Redis } from "ioredis";
import { metrics } from "../metrics.js";
import { keys } from "../redis/keys.js";
import { StreamEvent, fromEntry, toEntryFields } from "./events.js";

export interface StreamMeta {
  name: string;
  description?: string;
  createdAt: string;
  createdBy: string;
  maxLen?: number;
  /** When set, the digest scheduler compacts this stream past this length. */
  digestThreshold?: number;
}

export interface PublishResult {
  event: StreamEvent;
  /** True when this publish created the stream (registry insert). */
  createdStream: boolean;
}

export interface ReadResult {
  events: StreamEvent[];
  /** Pass back as fromId to continue reading. */
  nextCursor: string | null;
}

const MAX_BLOCK_MS = 25_000;

export class StreamService {
  constructor(
    private readonly redis: Redis,
    private readonly defaultMaxLen: number,
    private readonly redisUrl: string,
  ) {}

  async publish(input: {
    stream: string;
    type: string;
    source: string;
    payload: Record<string, unknown>;
    correlationId?: string;
  }): Promise<PublishResult> {
    const createdStream = await this.ensureRegistered(input.stream, input.source);
    const meta = createdStream ? undefined : await this.getMeta(input.stream);
    const maxLen = meta?.maxLen ?? this.defaultMaxLen;

    const ts = new Date().toISOString();
    const fields = toEntryFields({ ...input, ts });
    const id = (await this.redis.xadd(
      keys.stream(input.stream),
      "MAXLEN",
      "~",
      maxLen,
      "*",
      ...fields,
    )) as string;

    metrics.eventsPublished.inc();

    return {
      event: {
        id,
        stream: input.stream,
        type: input.type,
        source: input.source,
        ts,
        ...(input.correlationId ? { correlationId: input.correlationId } : {}),
        payload: input.payload,
      },
      createdStream,
    };
  }

  /**
   * Read events. Precedence: fromId (exclusive cursor) > sinceMs > tail of last `limit`.
   * With blockMs and a cursor, long-polls via XREAD BLOCK on a throwaway connection.
   */
  async read(input: {
    stream: string;
    fromId?: string;
    sinceMs?: number;
    limit?: number;
    blockMs?: number;
  }): Promise<ReadResult> {
    const limit = Math.min(Math.max(input.limit ?? 50, 1), 1000);
    const key = keys.stream(input.stream);

    if (input.blockMs && input.blockMs > 0) {
      return this.blockingRead(key, input.stream, input.fromId ?? "$", limit, Math.min(input.blockMs, MAX_BLOCK_MS));
    }

    let entries: [string, string[]][];
    if (input.fromId) {
      entries = (await this.redis.xrange(key, `(${input.fromId}`, "+", "COUNT", limit)) as [string, string[]][];
    } else if (input.sinceMs) {
      entries = (await this.redis.xrange(key, `${input.sinceMs}-0`, "+", "COUNT", limit)) as [string, string[]][];
    } else {
      const rev = (await this.redis.xrevrange(key, "+", "-", "COUNT", limit)) as [string, string[]][];
      entries = rev.reverse();
    }

    const events = entries.map(([id, raw]) => fromEntry(input.stream, id, raw));
    const last = events[events.length - 1];
    return { events, nextCursor: last ? last.id : input.fromId ?? null };
  }

  private async blockingRead(
    key: string,
    stream: string,
    fromId: string,
    limit: number,
    blockMs: number,
  ): Promise<ReadResult> {
    // Throwaway connection: XREAD BLOCK parks the socket, so never use the shared one.
    const conn = new Redis(this.redisUrl, { maxRetriesPerRequest: null });
    try {
      const res = (await conn.xread("COUNT", limit, "BLOCK", blockMs, "STREAMS", key, fromId)) as
        | [string, [string, string[]][]][]
        | null;
      const entries = res?.[0]?.[1] ?? [];
      const events = entries.map(([id, raw]) => fromEntry(stream, id, raw));
      const last = events[events.length - 1];
      return { events, nextCursor: last ? last.id : fromId === "$" ? null : fromId };
    } finally {
      conn.disconnect();
    }
  }

  async listStreams(): Promise<Array<StreamMeta & { latestId: string | null; count: number }>> {
    const registry = await this.redis.hgetall(keys.streamRegistry);
    const out: Array<StreamMeta & { latestId: string | null; count: number }> = [];
    for (const [name, json] of Object.entries(registry)) {
      let meta: StreamMeta;
      try {
        meta = { name, ...JSON.parse(json) };
      } catch {
        meta = { name, createdAt: "", createdBy: "unknown" };
      }
      const key = keys.stream(name);
      const [count, latest] = await Promise.all([
        this.redis.xlen(key),
        this.redis.xrevrange(key, "+", "-", "COUNT", 1),
      ]);
      const latestEntry = (latest as [string, string[]][])[0];
      out.push({ ...meta, latestId: latestEntry ? latestEntry[0] : null, count });
    }
    return out.sort((a, b) => a.name.localeCompare(b.name));
  }

  async getMeta(name: string): Promise<StreamMeta | undefined> {
    const json = await this.redis.hget(keys.streamRegistry, name);
    if (!json) return undefined;
    try {
      return { name, ...JSON.parse(json) };
    } catch {
      return { name, createdAt: "", createdBy: "unknown" };
    }
  }

  /** Register the stream if new. Returns true when it was created. */
  async ensureRegistered(name: string, createdBy: string, description?: string): Promise<boolean> {
    const meta = { createdAt: new Date().toISOString(), createdBy, ...(description ? { description } : {}) };
    const inserted = await this.redis.hsetnx(keys.streamRegistry, name, JSON.stringify(meta));
    return inserted === 1;
  }

  /** Merge a patch into a stream's registry metadata (creates the stream if new). */
  async updateMeta(
    name: string,
    patch: Partial<Pick<StreamMeta, "description" | "maxLen" | "digestThreshold">>,
    updatedBy: string,
  ): Promise<StreamMeta> {
    await this.ensureRegistered(name, updatedBy);
    const current = (await this.getMeta(name)) ?? { name, createdAt: new Date().toISOString(), createdBy: updatedBy };
    const { name: _n, ...rest } = { ...current, ...patch };
    // Explicit nulls-by-zero: a 0 threshold/maxLen removes the setting.
    if (patch.digestThreshold === 0) delete (rest as Record<string, unknown>).digestThreshold;
    if (patch.maxLen === 0) delete (rest as Record<string, unknown>).maxLen;
    await this.redis.hset(keys.streamRegistry, name, JSON.stringify(rest));
    return { name, ...rest };
  }

  /** Trim everything up to AND INCLUDING `toId` (exact, not approximate). */
  async trimThrough(name: string, toId: string): Promise<number> {
    return this.redis.xtrim(keys.stream(name), "MINID", incrementEntryId(toId));
  }
}

/** "123-4" → "123-5": the smallest id strictly greater than the input. */
export function incrementEntryId(id: string): string {
  const [ms, seq] = id.split("-");
  return `${ms}-${BigInt(seq ?? "0") + 1n}`;
}
