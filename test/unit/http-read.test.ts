import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import request from "supertest";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { CursorService } from "../../src/core/cursors.js";
import { Fanout } from "../../src/core/fanout.js";
import { PresenceService } from "../../src/core/presence.js";
import { ProtocolService } from "../../src/core/protocols.js";
import { StreamService } from "../../src/core/streams.js";
import { TaskService } from "../../src/core/tasks.js";
import { WebhookService } from "../../src/core/webhooks.js";
import { buildApp } from "../../src/http/app.js";
import { FederationManager } from "../../src/mcp/federation.js";
import { ListChangedNotifier } from "../../src/mcp/notifier.js";
import { SessionRegistry } from "../../src/mcp/sessions.js";
import type { Express } from "express";

const BOUND_TOKEN = "tok_bound"; // bound to agent "reader"
const BARE_TOKEN = "tok_bare"; // no agent binding

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let streams: StreamService;
let fanout: Fanout;
let app: Express;

interface ReadBody {
  events: Array<{ id: string; type: string; payload: Record<string, unknown> }>;
  nextCursor: string | null;
  cursor?: string;
  committed?: string | null;
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });

  const config = loadConfig({
    REDIS_URL: url,
    MCS_TOKENS: `${BOUND_TOKEN}:reader,${BARE_TOKEN}`,
  } as NodeJS.ProcessEnv);
  streams = new StreamService(main, 1000, url);
  const tasks = new TaskService(main, streams);
  const protocols = new ProtocolService(main, streams);
  const cursors = new CursorService(main);
  fanout = new Fanout(blocking, main);
  fanout.start();
  const presence = new PresenceService(main);
  const registry = new SessionRegistry(fanout, streams, presence);
  const listChanged = new ListChangedNotifier();
  const toolsChanged = new ListChangedNotifier();
  const webhooks = new WebhookService(main, fanout, streams);
  const federation = new FederationManager(main, config, toolsChanged);

  app = buildApp(
    { config, streams, tasks, protocols, cursors, webhooks, federation, registry, presence, listChanged, toolsChanged },
    async () => true,
  );

  for (let i = 0; i < 5; i++) {
    await streams.publish({ stream: "r-seed", type: "seed.item", source: "t", payload: { i } });
  }
});

afterAll(async () => {
  await fanout?.stop();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

const authed = (path: string, token = BOUND_TOKEN) =>
  request(app).get(path).set("Authorization", `Bearer ${token}`);

describe("GET /streams/:stream", () => {
  it("requires a bearer token when tokens are configured", async () => {
    await request(app).get("/streams/r-seed").expect(401);
    await authed("/streams/r-seed").expect(200);
  });

  it("tail-reads the most recent events by default", async () => {
    const res = await authed("/streams/r-seed").expect(200);
    const body = res.body as ReadBody;
    expect(body.events).toHaveLength(5);
    expect(body.events[4]!.payload.i).toBe(4);
    expect(body.nextCursor).toBe(body.events[4]!.id);
    expect(body.cursor).toBeUndefined(); // no named-cursor fields without ?cursor
  });

  it("pages exclusively with from=", async () => {
    const first = (await authed("/streams/r-seed?limit=2").expect(200)).body as ReadBody;
    // Tail read returns the LAST 2; page from the very first event instead.
    const all = (await authed("/streams/r-seed").expect(200)).body as ReadBody;
    const page = (await authed(`/streams/r-seed?from=${all.events[0]!.id}&limit=2`).expect(200)).body as ReadBody;
    expect(page.events).toHaveLength(2);
    expect(page.events[0]!.id).toBe(all.events[1]!.id); // exclusive of `from`
    expect(first.events).toHaveLength(2);
  });

  it("runs the two-phase cursor+commit resume flow", async () => {
    const s = "r-cursor";
    for (let i = 0; i < 3; i++) await streams.publish({ stream: s, type: "seq", source: "t", payload: { i } });

    const first = (await authed(`/streams/${s}?cursor=proc&commit=true`).expect(200)).body as ReadBody;
    expect(first.events).toHaveLength(3);
    expect(first.cursor).toBe("proc");
    expect(first.committed).toBe(first.nextCursor);

    const idle = (await authed(`/streams/${s}?cursor=proc&commit=true`).expect(200)).body as ReadBody;
    expect(idle.events).toHaveLength(0);
    expect(idle.committed).toBeNull(); // nothing read → nothing committed

    await streams.publish({ stream: s, type: "seq", source: "t", payload: { i: 3 } });
    const resumed = (await authed(`/streams/${s}?cursor=proc&commit=true`).expect(200)).body as ReadBody;
    expect(resumed.events).toHaveLength(1);
    expect(resumed.events[0]!.payload.i).toBe(3);
  });

  it("scopes cursors per agent identity", async () => {
    const s = "r-scoped";
    await streams.publish({ stream: s, type: "x", source: "t", payload: {} });
    await authed(`/streams/${s}?cursor=me&commit=true`).expect(200);
    // Different X-Agent-Name = different cursor namespace: replays from the start.
    const other = (
      await authed(`/streams/${s}?cursor=me&commit=true`).set("X-Agent-Name", "someone-else").expect(200)
    ).body as ReadBody;
    expect(other.events).toHaveLength(1);
  });

  it("rejects invalid input with 400s", async () => {
    await authed("/streams/r-seed?limit=999").expect(400);
    await authed("/streams/r-seed?from=garbage").expect(400);
    await authed("/streams/r-seed?cursor=a&from=1-1").expect(400);
    await authed("/streams/%20bad%20name").expect(400);
    // cursor without any identity: bare token + no X-Agent-Name = anon
    const res = await authed("/streams/r-seed?cursor=me", BARE_TOKEN).expect(400);
    expect((res.body as { error: string }).error).toMatch(/identity/);
  });

  it("returns 200 with no events for a nonexistent stream (tool parity)", async () => {
    const body = (await authed("/streams/never-written").expect(200)).body as ReadBody;
    expect(body.events).toHaveLength(0);
    expect(body.nextCursor).toBeNull();
  });

  it("long-polls with blockMs and returns early when an event lands", async () => {
    const s = "r-block";
    await streams.publish({ stream: s, type: "old", source: "t", payload: {} });
    const started = Date.now();
    const pending = authed(`/streams/${s}?blockMs=5000`).expect(200);
    setTimeout(() => void streams.publish({ stream: s, type: "fresh", source: "t", payload: {} }), 200);
    const body = (await pending).body as ReadBody;
    // blockMs parks on the tail ($): only the event published mid-poll arrives.
    expect(body.events.map((e) => e.type)).toEqual(["fresh"]);
    expect(Date.now() - started).toBeLessThan(4000); // returned early, not at timeout
  });
});
