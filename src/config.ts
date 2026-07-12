import { z } from "zod";

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  REDIS_URL: z.string().default("redis://localhost:6379"),
  MCS_STREAM_MAXLEN: z.coerce.number().int().positive().default(10_000),
  MCS_TOKENS: z.string().default(""),
});

export type TokenRole = "admin" | "agent";

export interface TokenEntry {
  agentName?: string;
  role: TokenRole;
}

export interface Config {
  port: number;
  redisUrl: string;
  streamMaxLen: number;
  /** token -> bound identity/role. Format: token[:agentName[:admin]] */
  tokens: Map<string, TokenEntry>;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const parsed = envSchema.parse(env);

  const tokens = new Map<string, TokenEntry>();
  for (const entry of parsed.MCS_TOKENS.split(",")) {
    const trimmed = entry.trim();
    if (!trimmed) continue;
    const [token, agentName, role] = trimmed.split(":");
    if (!token) continue;
    tokens.set(token, {
      ...(agentName ? { agentName } : {}),
      role: role === "admin" ? "admin" : "agent",
    });
  }

  return {
    port: parsed.PORT,
    redisUrl: parsed.REDIS_URL,
    streamMaxLen: parsed.MCS_STREAM_MAXLEN,
    tokens,
  };
}
