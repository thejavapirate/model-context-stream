import { Redis } from "ioredis";
import { keys } from "../redis/keys.js";

/**
 * Durable per-agent read cursors: server-side bookmarks keyed by
 * (agentName, stream, cursorName). Keyed by agent identity — not session —
 * so an agent that reconnects resumes exactly where it left off.
 */
export class CursorService {
  constructor(private readonly redis: Redis) {}

  private field(stream: string, cursorName: string): string {
    return `${stream}::${cursorName}`;
  }

  async get(agentName: string, stream: string, cursorName = "default"): Promise<string | undefined> {
    const id = await this.redis.hget(keys.cursors(agentName), this.field(stream, cursorName));
    return id ?? undefined;
  }

  async commit(agentName: string, stream: string, id: string, cursorName = "default"): Promise<void> {
    await this.redis.hset(keys.cursors(agentName), this.field(stream, cursorName), id);
  }

  async list(agentName: string): Promise<Array<{ stream: string; cursor: string; id: string }>> {
    const raw = await this.redis.hgetall(keys.cursors(agentName));
    return Object.entries(raw)
      .map(([field, id]) => {
        const sep = field.lastIndexOf("::");
        return {
          stream: sep === -1 ? field : field.slice(0, sep),
          cursor: sep === -1 ? "default" : field.slice(sep + 2),
          id,
        };
      })
      .sort((a, b) => a.stream.localeCompare(b.stream) || a.cursor.localeCompare(b.cursor));
  }
}
