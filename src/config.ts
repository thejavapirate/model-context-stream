import { z } from "zod";

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  REDIS_URL: z.string().default("redis://localhost:6379"),
  MCS_STREAM_MAXLEN: z.coerce.number().int().positive().default(10_000),
  MCS_TOKENS: z.string().default(""),
});

export interface TokenEntry {
  token: string;
  agentName?: string;
}

export interface Config {
  port: number;
  redisUrl: string;
  streamMaxLen: number;
  /** token -> optional bound agent name */
  tokens: Map<string, string | undefined>;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = envSchema.parse(env);

  const tokens = new Map<string, string | undefined>();
  for (const entry of parsed.MCS_TOKENS.split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const sep = trimmed.indexOf(":");
    if (sep === -1) {
      tokens.set(trimmed, undefined);
    } else {
      tokens.set(trimmed.slice(0, sep), trimmed.slice(sep + 1) || undefined);
    }
  }

  return {
    port: parsed.PORT,
    redisUrl: parsed.REDIS_URL,
    streamMaxLen: parsed.MCS_STREAM_MAXLEN,
    tokens,
  };
}
