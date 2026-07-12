import { Redis } from "ioredis";
import { keys, SYSTEM_STREAMS } from "../redis/keys.js";
import { StreamService } from "./streams.js";

export interface ProtocolHead {
  name: string;
  latest: number;
  updatedAt: string;
  updatedBy: string;
  changelog?: string;
}

export interface Protocol extends ProtocolHead {
  version: number;
  content: string;
}

export class ProtocolService {
  constructor(
    private readonly redis: Redis,
    private readonly streams: StreamService,
  ) {}

  /** Create or bump a protocol. Versions are immutable; head advances. */
  async put(input: {
    name: string;
    content: string;
    changelog?: string;
    updatedBy: string;
  }): Promise<{ name: string; version: number; created: boolean }> {
    const headKey = keys.protocolHead(input.name);
    const version = await this.redis.hincrby(headKey, "latest", 1);
    const created = version === 1;
    const updatedAt = new Date().toISOString();

    await this.redis
      .multi()
      .set(keys.protocolVersion(input.name, version), input.content)
      .hset(headKey, {
        updatedAt,
        updatedBy: input.updatedBy,
        ...(input.changelog ? { changelog: input.changelog } : {}),
      })
      .sadd(keys.protocolRegistry, input.name)
      .exec();

    await this.streams.publish({
      stream: SYSTEM_STREAMS.protocols,
      type: created ? "protocol.created" : "protocol.updated",
      source: input.updatedBy,
      payload: { name: input.name, version, changelog: input.changelog },
    });

    return { name: input.name, version, created };
  }

  async get(name: string, version?: number): Promise<Protocol | undefined> {
    const head = await this.getHead(name);
    if (!head) return undefined;
    const v = version ?? head.latest;
    if (v < 1 || v > head.latest) return undefined;
    const content = await this.redis.get(keys.protocolVersion(name, v));
    if (content === null) return undefined;
    return { ...head, version: v, content };
  }

  async getHead(name: string): Promise<ProtocolHead | undefined> {
    const raw = await this.redis.hgetall(keys.protocolHead(name));
    if (!raw.latest) return undefined;
    return {
      name,
      latest: Number(raw.latest),
      updatedAt: raw.updatedAt ?? "",
      updatedBy: raw.updatedBy ?? "unknown",
      ...(raw.changelog ? { changelog: raw.changelog } : {}),
    };
  }

  async list(): Promise<ProtocolHead[]> {
    const names = await this.redis.smembers(keys.protocolRegistry);
    const heads = await Promise.all(names.map((n) => this.getHead(n)));
    return heads
      .filter((h): h is ProtocolHead => h !== undefined)
      .sort((a, b) => a.name.localeCompare(b.name));
  }
}
