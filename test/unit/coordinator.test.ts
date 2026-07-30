import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";
import { casDelete, CoordinatorLease } from "../../src/core/coordinator.js";
import { keys } from "../../src/redis/keys.js";

let container: StartedRedisContainer;
let main: Redis;

const FAST = { ttlMs: 600, checkMs: 150 };

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`timed out waiting for ${what}`);
}

function makeLease(events: string[], tag: string): CoordinatorLease {
  return new CoordinatorLease(
    main,
    {
      onGain: () => {
        events.push(`${tag}:gain`);
      },
      onLose: () => {
        events.push(`${tag}:lose`);
      },
    },
    FAST,
  );
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  main = new Redis(container.getConnectionUrl());
});

beforeEach(async () => {
  await main.del(keys.coordinator);
});

afterAll(async () => {
  main?.disconnect();
  await container?.stop();
});

describe("CoordinatorLease", () => {
  it("elects exactly one leader between two contenders", async () => {
    const events: string[] = [];
    const a = makeLease(events, "a");
    const b = makeLease(events, "b");
    a.start();
    b.start();
    try {
      await waitFor(() => a.isLeader || b.isLeader, "a leader to emerge");
      // Let several ticks pass: leadership must not flap or duplicate.
      await new Promise((r) => setTimeout(r, FAST.checkMs * 3));
      expect([a.isLeader, b.isLeader].filter(Boolean)).toHaveLength(1);
      expect(events.filter((e) => e.endsWith(":gain"))).toHaveLength(1);
      expect(events.filter((e) => e.endsWith(":lose"))).toHaveLength(0);
    } finally {
      await a.stop();
      await b.stop();
    }
  });

  it("hands off to the standby on clean stop", async () => {
    const events: string[] = [];
    const a = makeLease(events, "a");
    a.start();
    await waitFor(() => a.isLeader, "a to lead");
    const b = makeLease(events, "b");
    b.start();
    try {
      await a.stop();
      expect(events).toContain("a:lose");
      await waitFor(() => b.isLeader, "b to take over after clean stop");
      expect(events).toContain("b:gain");
    } finally {
      await b.stop();
    }
  });

  it("detects a stolen/expired lease and fires onLose", async () => {
    const events: string[] = [];
    const a = makeLease(events, "a");
    a.start();
    await waitFor(() => a.isLeader, "a to lead");
    // Simulate expiry/theft: replace the lease value out from under a.
    await main.set(keys.coordinator, "someone-else", "PX", 60_000);
    try {
      await waitFor(() => !a.isLeader, "a to notice lost leadership");
      expect(events).toContain("a:lose");
    } finally {
      await a.stop();
      // a.stop() must not delete a lease it no longer owns.
      expect(await main.get(keys.coordinator)).toBe("someone-else");
    }
  });

  it("casDelete only deletes when the value matches", async () => {
    await main.set("cas-key", "mine");
    expect(await casDelete(main, "cas-key", "theirs")).toBe(false);
    expect(await main.get("cas-key")).toBe("mine");
    expect(await casDelete(main, "cas-key", "mine")).toBe(true);
    expect(await main.get("cas-key")).toBeNull();
  });
});
