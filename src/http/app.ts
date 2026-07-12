import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import express, { type Express, type Response } from "express";
import { nanoid } from "nanoid";
import { buildMcpServer, type Deps, type SessionCtx } from "../mcp/server.js";
import { bearerAuth, type AuthedRequest } from "./auth.js";
import { ingestHandler } from "./ingest.js";

interface LiveTransport {
  transport: StreamableHTTPServerTransport;
}

export function buildApp(deps: Deps, pingRedis: () => Promise<boolean>): Express {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  const transports = new Map<string, LiveTransport>();

  app.get("/healthz", async (_req, res) => {
    const ok = await pingRedis().catch(() => false);
    res.status(ok ? 200 : 503).json({ ok, sessions: deps.registry.all().length });
  });

  const auth = bearerAuth(deps.config);
  app.post("/ingest/:stream", auth, ingestHandler(deps.streams, deps.listChanged));

  const cleanup = (sessionId: string) => {
    transports.delete(sessionId);
    deps.listChanged.unregister(sessionId);
    deps.registry.remove(sessionId);
  };

  /** Identity precedence: X-Agent-Name header > token-bound name > MCP clientInfo > anon. */
  const resolveAgentName = (req: AuthedRequest, body: unknown): string => {
    const header = req.header("x-agent-name");
    if (header) return header.slice(0, 128);
    if (req.tokenAgent) return req.tokenAgent;
    const clientInfo = (body as { params?: { clientInfo?: { name?: string; version?: string } } })?.params
      ?.clientInfo;
    if (clientInfo?.name) return `${clientInfo.name}${clientInfo.version ? `@${clientInfo.version}` : ""}`;
    return "anon";
  };

  const handleSessionRequest = async (req: AuthedRequest, res: Response): Promise<void> => {
    const sessionId = req.header("mcp-session-id");
    const live = sessionId ? transports.get(sessionId) : undefined;

    if (live && sessionId) {
      deps.registry.touch(sessionId);
      await live.transport.handleRequest(req, res, req.body);
      return;
    }

    // New sessions start with an initialize POST and no session header.
    if (req.method === "POST" && !sessionId && isInitializeRequest(req.body)) {
      const ctx: SessionCtx = { agentName: resolveAgentName(req, req.body) };
      const mcp = buildMcpServer(deps, ctx);

      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => nanoid(),
        onsessioninitialized: (id) => {
          ctx.sessionId = id;
          if (ctx.agentName === "anon") ctx.agentName = `anon-${id.slice(0, 6)}`;
          transports.set(id, { transport });
          deps.registry.add({
            id,
            agentName: ctx.agentName,
            sendUpdated: (uri) => mcp.server.sendResourceUpdated({ uri }),
            close: () => void transport.close().catch(() => {}),
          });
          deps.listChanged.register(id, async () => mcp.sendResourceListChanged());
          console.log(`[mcp] session ${id} connected (${ctx.agentName})`);
        },
        onsessionclosed: (id) => {
          console.log(`[mcp] session ${id} closed`);
          cleanup(id);
        },
      });
      transport.onclose = () => {
        if (transport.sessionId) cleanup(transport.sessionId);
      };

      await mcp.connect(transport);
      await transport.handleRequest(req, res, req.body);
      return;
    }

    res.status(sessionId ? 404 : 400).json({
      jsonrpc: "2.0",
      error: {
        code: -32000,
        message: sessionId ? "Unknown or expired session" : "Bad request: expected initialize or mcp-session-id",
      },
      id: null,
    });
  };

  app.post("/mcp", auth, handleSessionRequest);
  app.get("/mcp", auth, handleSessionRequest);
  app.delete("/mcp", auth, handleSessionRequest);

  return app;
}
