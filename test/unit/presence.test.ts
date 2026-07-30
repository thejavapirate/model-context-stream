import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Fanout } from "../../src/core/fanout.js";
import { PresenceService, type PresenceRecord } from "../../src/core/presence.js";
import { StreamService } from "../../src/core/streams.js";
import { SessionRegistry } from "../../src/mcp/sessions.js";
import { keys } from "../../src/redis/keys.js";

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let url: string;

const FAST = { ttlSec: 2, heartbeatMs: 400 };

function rec(sessionId: string, agent: string, subscriptions: string[] = []): PresenceRecord {
  const now = new Date().toISOString();
  return { sessionId, agent, connectedAt: now, lastSeenAt: now, subscriptions };
}

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timed out waiting for ${what}`);
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
});

afterAll(async () => {
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

describe("PresenceService", () => {
  it("roundtrips records through the fleet roster, sorted by agent", async () => {
    const p = new PresenceService(main, FAST);
    await p.upsert(rec("s-b", "beta", ["stream://team"]));
    await p.upsert(rec("s-a", "alpha"));

    const roster = await p.roster();
    expect(roster.map((r) => r.agent)).toEqual(["alpha", "beta"]);
    expect(roster[1]!.subscriptions).toEqual(["stream://team"]);

    await p.remove("s-a");
    await p.remove("s-b");
    expect(await p.roster()).toEqual([]);
    expect(await main.zcard(keys.presenceIndex)).toBe(0);
  });

  it("two replicas see the union of each other's sessions", async () => {
    const a = new PresenceService(main, FAST);
    const other = new Redis(url);
    const b = new PresenceService(other, FAST);
    try {
      await a.upsert(rec("s-1", "alice"));
      await b.upsert(rec("s-2", "bob"));

      for (const view of [await a.roster(), await b.roster()]) {
        expect(view.map((r) => r.sessionId)).toEqual(["s-1", "s-2"]);
      }
    } finally {
      await a.remove("s-1");
      await b.remove("s-2");
      other.disconnect();
    }
  });

  it("a crashed replica's sessions ghost out via TTL while heartbeated ones survive", async () => {
    const alive = new PresenceService(main, FAST);
    const crashed = new PresenceService(main, FAST);

    const kept = rec("s-alive", "survivor");
    await alive.upsert(kept);
    await crashed.upsert(rec("s-dead", "victim")); // never heartbeated: simulated crash

    alive.start(() => [kept]);
    try {
      await waitFor(
        async () => (await alive.roster()).map((r) => r.sessionId).join() === "s-alive",
        "crashed session to expire",
      );
      // Survivor outlived multiple TTLs thanks to the heartbeat; index self-pruned.
      await new Promise((r) => setTimeout(r, FAST.ttlSec * 1000 + 500));
      expect((await alive.roster()).map((r) => r.sessionId)).toEqual(["s-alive"]);
      expect(await main.zcard(keys.presenceIndex)).toBe(1);
    } finally {
      alive.stop();
      await alive.remove("s-alive");
    }
  });

  it("skips and prunes malformed records", async () => {
    const p = new PresenceService(main, FAST);
    await main.set(keys.presence("s-bad"), "{not json", "EX", 60);
    await main.zadd(keys.presenceIndex, Date.now() + 60_000, "s-bad");

    expect((await p.roster()).some((r) => r.sessionId === "s-bad")).toBe(false);
    expect(await main.zscore(keys.presenceIndex, "s-bad")).toBeNull();
  });

  it("SessionRegistry writes through: add/subscribe/remove reflected in the roster", async () => {
    const streams = new StreamService(main, 1000, url);
    const fanout = new Fanout(blocking, main);
    fanout.start();
    const presence = new PresenceService(main, FAST);
    const registry = new SessionRegistry(fanout, streams, presence);
    try {
      registry.add({ id: "sess-1", agentName: "worker", sendUpdated: async () => {}, close: () => {} });
      await waitFor(async () => (await presence.roster()).some((r) => r.sessionId === "sess-1"), "session visible");

      await registry.subscribeUri("sess-1", "stream://team");
      await waitFor(
        async () =>
          (await presence.roster()).find((r) => r.sessionId === "sess-1")?.subscriptions.includes("stream://team") ??
          false,
        "subscription visible in roster",
      );

      registry.remove("sess-1");
      await waitFor(async () => !(await presence.roster()).some((r) => r.sessionId === "sess-1"), "session removed");
    } finally {
      registry.stop();
      await fanout.stop();
    }
  });
});
