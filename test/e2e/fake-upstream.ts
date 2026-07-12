/** A minimal upstream MCP server over Streamable HTTP, for federation tests. */
import type { Server } from "node:http";
import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { isInitializeRequest } from "@modelcontextprotocol/sdk/types.js";
import express from "express";
import { nanoid } from "nanoid";
import { z } from "zod";

export interface FakeUpstream {
  url: string;
  /** Register an extra tool on all live sessions (fires tools/list_changed). */
  addShoutTool(): void;
  close(): Promise<void>;
}

export async function startFakeUpstream(): Promise<FakeUpstream> {
  const app = express();
  app.use(express.json({ limit: "1mb" }));

  const transports = new Map<string, StreamableHTTPServerTransport>();
  const liveServers = new Set<McpServer>();
  let shoutEnabled = false;

  const buildServer = (): McpServer => {
    const mcp = new McpServer({ name: "fake-upstream", version: "0.0.1" }, { capabilities: { tools: { listChanged: true } } });
    mcp.registerTool(
      "echo",
      {
        description: "Echo the input text back",
        inputSchema: { text: z.string() },
      },
      async ({ text }) => ({ content: [{ type: "text" as const, text: `echo: ${text}` }] }),
    );
    if (shoutEnabled) registerShout(mcp);
    return mcp;
  };

  const registerShout = (mcp: McpServer) => {
    mcp.registerTool(
      "shout",
      { description: "Uppercase the input", inputSchema: { text: z.string() } },
      async ({ text }) => ({ content: [{ type: "text" as const, text: text.toUpperCase() }] }),
    );
  };

  const handle = async (req: express.Request, res: express.Response) => {
    const sessionId = req.header("mcp-session-id");
    const existing = sessionId ? transports.get(sessionId) : undefined;
    if (existing) {
      await existing.handleRequest(req, res, req.body);
      return;
    }
    if (req.method === "POST" && isInitializeRequest(req.body)) {
      const mcp = buildServer();
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => nanoid(),
        onsessioninitialized: (id) => {
          transports.set(id, transport);
          liveServers.add(mcp);
        },
        onsessionclosed: (id) => {
          transports.delete(id);
          liveServers.delete(mcp);
        },
      });
      transport.onclose = () => liveServers.delete(mcp);
      await mcp.connect(transport);
      await transport.handleRequest(req, res, req.body);
      return;
    }
    res.status(400).json({ jsonrpc: "2.0", error: { code: -32000, message: "bad request" }, id: null });
  };

  app.post("/mcp", handle);
  app.get("/mcp", handle);
  app.delete("/mcp", handle);

  const server: Server = await new Promise((resolve) => {
    const s = app.listen(0, () => resolve(s));
  });
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("no port");

  return {
    url: `http://127.0.0.1:${address.port}/mcp`,
    addShoutTool() {
      shoutEnabled = true;
      // Registering on a connected server auto-fires tools/list_changed.
      for (const mcp of liveServers) registerShout(mcp);
    },
    async close() {
      for (const t of transports.values()) await t.close().catch(() => {});
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
}
