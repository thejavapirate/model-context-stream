import { timingSafeEqual } from "node:crypto";
import type { NextFunction, Request, Response } from "express";
import type { Config } from "../config.js";

export interface AuthedRequest extends Request {
  /** Agent name bound to the presented token, if any. */
  tokenAgent?: string;
}

function tokenMatches(presented: string, known: string): boolean {
  const a = Buffer.from(presented);
  const b = Buffer.from(known);
  if (a.length !== b.length) return false;
  return timingSafeEqual(a, b);
}

/**
 * Bearer-token guard for /mcp and /ingest. When no tokens are configured the
 * server runs open (local dev) and logs a warning at boot.
 */
export function bearerAuth(config: Config) {
  return (req: AuthedRequest, res: Response, next: NextFunction): void => {
    if (config.tokens.size === 0) {
      next();
      return;
    }

    const header = req.headers.authorization ?? "";
    const presented = header.startsWith("Bearer ") ? header.slice(7) : "";

    if (presented) {
      for (const [token, agentName] of config.tokens) {
        if (tokenMatches(presented, token)) {
          if (agentName) req.tokenAgent = agentName;
          next();
          return;
        }
      }
    }

    if (req.path.startsWith("/mcp")) {
      // JSON-RPC-shaped error so MCP clients surface it sensibly.
      res.status(401).json({
        jsonrpc: "2.0",
        error: { code: -32001, message: "Unauthorized: missing or invalid bearer token" },
        id: null,
      });
    } else {
      res.status(401).json({ error: "unauthorized" });
    }
  };
}
