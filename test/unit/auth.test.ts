import type { Request } from "express";
import { describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { bearerAuth, resolveHttpAgentName, type AuthedRequest } from "../../src/http/auth.js";

/** Minimal express-shaped request: only what auth.ts touches. */
function req(init: { headers?: Record<string, string>; path?: string } = {}): AuthedRequest {
  const headers = Object.fromEntries(Object.entries(init.headers ?? {}).map(([k, v]) => [k.toLowerCase(), v]));
  return {
    headers,
    path: init.path ?? "/mcp",
    header: (name: string) => headers[name.toLowerCase()],
  } as unknown as AuthedRequest;
}

function runAuth(config: ReturnType<typeof loadConfig>, r: AuthedRequest) {
  let status: number | undefined;
  let body: unknown;
  let nexted = false;
  const res = {
    status(code: number) {
      status = code;
      return this;
    },
    json(payload: unknown) {
      body = payload;
      return this;
    },
  };
  bearerAuth(config)(r, res as never, () => {
    nexted = true;
  });
  return { status, body, nexted };
}

const cfg = (tokens: string) => loadConfig({ REDIS_URL: "redis://x", MCS_TOKENS: tokens } as NodeJS.ProcessEnv);

describe("resolveHttpAgentName", () => {
  it("prefers the header, then the token-bound name, then anon", () => {
    expect(resolveHttpAgentName(req({ headers: { "x-agent-name": "scout" } }))).toBe("scout");
    const bound = req();
    bound.tokenAgent = "fleet-token-agent";
    expect(resolveHttpAgentName(bound)).toBe("fleet-token-agent");
    expect(resolveHttpAgentName(req())).toBe("anon");
  });

  it("ignores unexpanded client templates instead of adopting them as identities", () => {
    // Observed in production: a client sent .mcp.json's literal placeholder because
    // the env var was unset, and "${MCS_AGENT_NAME}" appeared on the presence roster.
    for (const junk of ["${MCS_AGENT_NAME}", "${MCS_AGENT_NAME:-builder}", "$MCS_AGENT_NAME"]) {
      const r = req({ headers: { "x-agent-name": junk } });
      expect(resolveHttpAgentName(r)).toBe("anon");
      r.tokenAgent = "bound";
      expect(resolveHttpAgentName(r)).toBe("bound"); // falls through to the real identity
    }
  });

  it("caps absurdly long names", () => {
    expect(resolveHttpAgentName(req({ headers: { "x-agent-name": "z".repeat(500) } }))).toHaveLength(128);
  });
});

describe("bearerAuth", () => {
  it("runs open (everyone admin) when no tokens are configured", () => {
    const r = req();
    const { nexted } = runAuth(cfg(""), r);
    expect(nexted).toBe(true);
    expect(r.tokenRole).toBe("admin");
  });

  it("accepts a valid token and binds its agent name and role", () => {
    const r = req({ headers: { authorization: "Bearer tok_ops" } });
    const { nexted } = runAuth(cfg("tok_ops:ops:admin,tok_worker:worker"), r);
    expect(nexted).toBe(true);
    expect(r.tokenAgent).toBe("ops");
    expect(r.tokenRole).toBe("admin");
  });

  it("does not grant admin to a plain agent token", () => {
    const r = req({ headers: { authorization: "Bearer tok_worker" } });
    runAuth(cfg("tok_ops:ops:admin,tok_worker:worker"), r);
    expect(r.tokenRole).toBe("agent");
  });

  it("rejects missing, malformed, wrong, and prefix-matching tokens", () => {
    const config = cfg("tok_ops:ops:admin");
    for (const headers of [
      {},
      { authorization: "tok_ops" }, // no Bearer prefix
      { authorization: "Bearer nope" },
      { authorization: "Bearer tok_op" }, // prefix of a real token
      { authorization: "Bearer tok_opsX" }, // real token plus a char
    ]) {
      const { status, nexted } = runAuth(config, req({ headers }));
      expect(nexted).toBe(false);
      expect(status).toBe(401);
    }
  });

  it("shapes the 401 as JSON-RPC for /mcp and plain JSON elsewhere", () => {
    const config = cfg("tok_ops:ops:admin");
    const mcp = runAuth(config, req({ path: "/mcp" }));
    expect((mcp.body as { jsonrpc?: string }).jsonrpc).toBe("2.0");
    const rest = runAuth(config, req({ path: "/ingest/team" }));
    expect((rest.body as { error?: string }).error).toBe("unauthorized");
  });
});
