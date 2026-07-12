import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { StreamService } from "../../src/core/streams.js";
import { keys } from "../../src/redis/keys.js";

let container: StartedRedisContainer;
let redis: Redis;
let streams: StreamService;

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  redis = new Redis(container.getConnectionUrl());
  streams = new StreamService(redis, 100, container.getConnectionUrl());
});

afterAll(async () => {
  redis?.disconnect();
  await container?.stop();
});

describe("StreamService", () => {
  it("publishes and reads back the tail", async () => {
    const pub = await streams.publish({
      stream: "unit-basic",
      type: "test.event",
      source: "tester",
      payload: { n: 1 },
      correlationId: "cid-1",
    });
    expect(pub.createdStream).toBe(true);
    expect(pub.event.id).toMatch(/^\d+-\d+$/);

    const { events, nextCursor } = await streams.read({ stream: "unit-basic" });
    expect(events).toHaveLength(1);
    expect(events[0]).toMatchObject({
      type: "test.event",
      source: "tester",
      correlationId: "cid-1",
      payload: { n: 1 },
    });
    expect(nextCursor).toBe(pub.event.id);
  });

  it("pages forward with an exclusive cursor", async () => {
    const ids: string[] = [];
    for (let i = 0; i < 5; i++) {
      const { event } = await streams.publish({
        stream: "unit-cursor",
        type: "seq",
        source: "tester",
        payload: { i },
      });
      ids.push(event.id);
    }

    const first = await streams.read({ stream: "unit-cursor", limit: 2 });
    expect(first.events.map((e) => e.payload.i)).toEqual([3, 4]); // tail read

    const fromStart = await streams.read({ stream: "unit-cursor", fromId: ids[1], limit: 10 });
    expect(fromStart.events.map((e) => e.payload.i)).toEqual([2, 3, 4]); // exclusive of ids[1]
    expect(fromStart.nextCursor).toBe(ids[4]);

    const empty = await streams.read({ stream: "unit-cursor", fromId: ids[4] });
    expect(empty.events).toHaveLength(0);
    expect(empty.nextCursor).toBe(ids[4]); // cursor stable when nothing new
  });

  it("reads by sinceMs", async () => {
    const before = Date.now() - 60_000;
    await streams.publish({ stream: "unit-since", type: "x", source: "t", payload: {} });
    const res = await streams.read({ stream: "unit-since", sinceMs: before });
    expect(res.events.length).toBeGreaterThanOrEqual(1);
    const future = await streams.read({ stream: "unit-since", sinceMs: Date.now() + 60_000 });
    expect(future.events).toHaveLength(0);
  });

  it("trims streams approximately at maxLen", async () => {
    for (let i = 0; i < 500; i++) {
      await streams.publish({ stream: "unit-trim", type: "bulk", source: "t", payload: { i } });
    }
    const len = await redis.xlen(keys.stream("unit-trim"));
    expect(len).toBeLessThan(500); // approximate trim kicked in around maxLen=100
  });

  it("registers streams once and lists them with counts", async () => {
    const again = await streams.publish({ stream: "unit-basic", type: "x", source: "t", payload: {} });
    expect(again.createdStream).toBe(false);

    const list = await streams.listStreams();
    const names = list.map((s) => s.name);
    expect(names).toContain("unit-basic");
    const basic = list.find((s) => s.name === "unit-basic")!;
    expect(basic.count).toBeGreaterThanOrEqual(2);
    expect(basic.latestId).toBe(again.event.id);
  });

  it("long-polls with blockMs and wakes on publish", async () => {
    const tail = await streams.read({ stream: "unit-block" }).catch(() => ({ nextCursor: null }));
    const pending = streams.read({
      stream: "unit-block",
      fromId: tail.nextCursor ?? undefined,
      blockMs: 5000,
    });
    await new Promise((r) => setTimeout(r, 100));
    await streams.publish({ stream: "unit-block", type: "wake", source: "t", payload: { hello: true } });
    const res = await pending;
    expect(res.events).toHaveLength(1);
    expect(res.events[0]!.type).toBe("wake");
  });
});
