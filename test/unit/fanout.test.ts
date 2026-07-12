import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { StreamEvent } from "../../src/core/events.js";
import { Fanout } from "../../src/core/fanout.js";
import { StreamService } from "../../src/core/streams.js";

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let streams: StreamService;
let fanout: Fanout;

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
  streams = new StreamService(main, 1000, url);
  fanout = new Fanout(blocking, main);
  fanout.start();
});

afterAll(async () => {
  await fanout?.stop();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

function waitForEvent(register: (cb: (e: StreamEvent) => void) => void, timeoutMs = 5000): Promise<StreamEvent> {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for fanout delivery")), timeoutMs);
    register((e) => {
      clearTimeout(timer);
      resolve(e);
    });
  });
}

describe("Fanout", () => {
  it("delivers published events to subscribers quickly (wake interrupts a parked XREAD)", async () => {
    const received: StreamEvent[] = [];
    let resolveFirst!: (e: StreamEvent) => void;
    const first = new Promise<StreamEvent>((r) => (resolveFirst = r));

    await fanout.subscribe("fan-basic", (e) => {
      received.push(e);
      if (received.length === 1) resolveFirst(e);
    });

    // subscribe() resolves only once armed; publish immediately after.
    const started = Date.now();
    await streams.publish({ stream: "fan-basic", type: "ping", source: "t", payload: { a: 1 } });
    const event = await Promise.race([
      first,
      new Promise<never>((_, rej) => setTimeout(() => rej(new Error("no delivery in 5s")), 5000)),
    ]);

    expect(event.type).toBe("ping");
    expect(event.payload).toEqual({ a: 1 });
    expect(Date.now() - started).toBeLessThan(5000);
  });

  it("only delivers events published after subscription (tail semantics)", async () => {
    await streams.publish({ stream: "fan-tail", type: "before", source: "t", payload: {} });

    const events: StreamEvent[] = [];
    await fanout.subscribe("fan-tail", (e) => events.push(e));
    // Give the wake a moment to re-arm the XREAD with the new stream.
    await new Promise((r) => setTimeout(r, 300));

    await streams.publish({ stream: "fan-tail", type: "after", source: "t", payload: {} });
    await new Promise((r) => setTimeout(r, 500));

    expect(events.map((e) => e.type)).toEqual(["after"]);
  });

  it("stops delivering after unsubscribe", async () => {
    const events: StreamEvent[] = [];
    const unsub = await fanout.subscribe("fan-unsub", (e) => events.push(e));
    await new Promise((r) => setTimeout(r, 300));

    await streams.publish({ stream: "fan-unsub", type: "one", source: "t", payload: {} });
    await new Promise((r) => setTimeout(r, 500));
    unsub();
    await streams.publish({ stream: "fan-unsub", type: "two", source: "t", payload: {} });
    await new Promise((r) => setTimeout(r, 500));

    expect(events.map((e) => e.type)).toEqual(["one"]);
  });

  it("fans one stream out to multiple listeners", async () => {
    const a: string[] = [];
    const b: string[] = [];
    await fanout.subscribe("fan-multi", (e) => a.push(e.type));
    await fanout.subscribe("fan-multi", (e) => b.push(e.type));
    await new Promise((r) => setTimeout(r, 300));

    await streams.publish({ stream: "fan-multi", type: "broadcast", source: "t", payload: {} });
    await new Promise((r) => setTimeout(r, 500));

    expect(a).toEqual(["broadcast"]);
    expect(b).toEqual(["broadcast"]);
  });
});
