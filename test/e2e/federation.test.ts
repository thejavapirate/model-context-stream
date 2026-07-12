import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ToolListChangedNotificationSchema } from "@modelcontextprotocol/sdk/types.js";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import type { Server } from "node:http";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { CursorService } from "../../src/core/cursors.js";
import { Fanout } from "../../src/core/fanout.js";
import { ProtocolService } from "../../src/core/protocols.js";
import { StreamService } from "../../src/core/streams.js";
import { TaskService } from "../../src/core/tasks.js";
import { WebhookService } from "../../src/core/webhooks.js";
import { buildApp } from "../../src/http/app.js";
import { FederationManager } from "../../src/mcp/federation.js";
import { ListChangedNotifier } from "../../src/mcp/notifier.js";
import { SessionRegistry } from "../../src/mcp/sessions.js";
import { createRedis, type RedisConnections } from "../../src/redis/client.js";
import { startFakeUpstream, type FakeUpstream } from "./fake-upstream.js";

const ADMIN_TOKEN = "tok_admin";
const AGENT_TOKEN = "tok_agent";

let container: StartedRedisContainer;
let redis: RedisConnections;
let fanout: Fanout;
let registry: SessionRegistry;
let listChanged: ListChangedNotifier;
let toolsChanged: ListChangedNotifier;
let webhooks: WebhookService;
let federation: FederationManager;
let tasks: TaskService;
let server: Server;
let baseUrl: string;
let upstream: FakeUpstream;

let admin: Client;
let agent: Client;
let agentToolListChanges = 0;

async function connect(token: string, name: string): Promise<Client> {
  const transport = new StreamableHTTPClientTransport(new URL(`${baseUrl}/mcp`), {
    requestInit: { headers: { Authorization: `Bearer ${token}`, "X-Agent-Name": name } },
  });
  const client = new Client({ name, version: "0.0.1" }, { capabilities: {} });
  await client.connect(transport);
  await new Promise((r) => setTimeout(r, 200));
  return client;
}

async function toolNames(client: Client): Promise<string[]> {
  const res = await client.listTools();
  return res.tools.map((t) => t.name);
}

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 10_000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 100));
  }
  throw new Error(`timed out waiting for ${what}`);
}

function toolJson<T>(result: { content?: unknown }): T {
  const content = result.content as Array<{ type: string; text?: string }>;
  return JSON.parse(content.find((c) => c.type === "text")!.text!) as T;
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  const config = loadConfig({
    REDIS_URL: url,
    MCS_TOKENS: `${ADMIN_TOKEN}:boss:admin,${AGENT_TOKEN}:worker`,
  } as NodeJS.ProcessEnv);
  redis = createRedis(url);

  const streams = new StreamService(redis.main, config.streamMaxLen, url);
  tasks = new TaskService(redis.main, streams);
  const protocols = new ProtocolService(redis.main, streams);
  const cursors = new CursorService(redis.main);
  fanout = new Fanout(redis.blocking, redis.main);
  fanout.start();
  registry = new SessionRegistry(fanout, streams);
  listChanged = new ListChangedNotifier();
  toolsChanged = new ListChangedNotifier();
  webhooks = new WebhookService(redis.main, fanout, streams);
  await webhooks.start();
  federation = new FederationManager(redis.main, config, toolsChanged);
  await federation.start();

  const app = buildApp(
    { config, streams, tasks, protocols, cursors, webhooks, federation, registry, listChanged, toolsChanged },
    async () => true,
  );
  server = app.listen(0);
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  baseUrl = `http://127.0.0.1:${address.port}`;

  upstream = await startFakeUpstream();

  admin = await connect(ADMIN_TOKEN, "boss");
  agent = await connect(AGENT_TOKEN, "worker");
  agent.setNotificationHandler(ToolListChangedNotificationSchema, () => {
    agentToolListChanges += 1;
  });
});

afterAll(async () => {
  await Promise.allSettled([admin?.close(), agent?.close()]);
  await upstream?.close();
  await federation?.stop();
  await webhooks?.stop();
  registry?.stop();
  listChanged?.close();
  toolsChanged?.close();
  server?.close();
  await fanout?.stop();
  await redis?.quit();
  await container?.stop();
}, 60_000);

describe.skipIf(!process.env.RUN_E2E)("MCP tool federation", () => {
  it("rejects add_upstream from non-admin sessions", async () => {
    const res = await agent.callTool({
      name: "add_upstream",
      arguments: { name: "sneaky", url: upstream.url },
    });
    expect(res.isError).toBe(true);
    expect(JSON.stringify(res.content)).toContain("admin");
  });

  it("federates an upstream: proxy tools appear on existing sessions and calls pass through", async () => {
    const res = await admin.callTool({
      name: "add_upstream",
      arguments: { name: "fake", url: upstream.url },
    });
    expect(res.isError).toBeFalsy();
    const view = toolJson<{ status: string; toolCount: number }>(res);
    expect(view.status).toBe("connected");
    expect(view.toolCount).toBe(1);

    // Existing (pre-add) sessions see the proxy tool + got a list_changed.
    await waitFor(async () => (await toolNames(agent)).includes("fake__echo"), "fake__echo visible to agent");
    await waitFor(() => agentToolListChanges >= 1, "tools/list_changed at agent");

    const call = await agent.callTool({ name: "fake__echo", arguments: { text: "hello fleet" } });
    expect(call.isError).toBeFalsy();
    expect(JSON.stringify(call.content)).toContain("echo: hello fleet");
  });

  it("shows the upstream's real input schema on the proxy tool", async () => {
    const res = await agent.listTools();
    const proxy = res.tools.find((t) => t.name === "fake__echo")!;
    const schema = proxy.inputSchema as { properties?: Record<string, unknown>; required?: string[] };
    expect(schema.properties).toHaveProperty("text");
    expect(schema.required).toContain("text");
  });

  it("reconciles dynamically when the upstream adds a tool", async () => {
    upstream.addShoutTool();
    await waitFor(async () => (await toolNames(agent)).includes("fake__shout"), "fake__shout appears");
    const call = await agent.callTool({ name: "fake__shout", arguments: { text: "quiet" } });
    expect(JSON.stringify(call.content)).toContain("QUIET");
  });

  it("registers proxies on sessions that connect after federation", async () => {
    const late = await connect(AGENT_TOKEN, "latecomer");
    try {
      const names = await toolNames(late);
      expect(names).toContain("fake__echo");
      expect(names).toContain("fake__shout");
    } finally {
      await late.close();
    }
  });

  it("blocks self-federation on the configured port", async () => {
    // The guard compares against config.port (3000 default in this test env).
    const direct = await admin.callTool({
      name: "add_upstream",
      arguments: { name: "loop", url: "http://localhost:3000/mcp" },
    });
    expect(direct.isError).toBe(true);
    expect(JSON.stringify(direct.content)).toContain("itself");
  });

  it("removes an upstream and its proxies everywhere", async () => {
    const res = await admin.callTool({ name: "remove_upstream", arguments: { name: "fake" } });
    expect(res.isError).toBeFalsy();
    await waitFor(async () => {
      const names = await toolNames(agent);
      return !names.some((n) => n.startsWith("fake__"));
    }, "fake__* gone from agent");

    const list = toolJson<{ upstreams: unknown[] }>(await agent.callTool({ name: "list_upstreams", arguments: {} }));
    expect(list.upstreams).toHaveLength(0);
  });
});
