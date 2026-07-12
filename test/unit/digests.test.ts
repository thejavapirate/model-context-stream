import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DigestScheduler } from "../../src/core/digests.js";
import { Fanout } from "../../src/core/fanout.js";
import { ProtocolService } from "../../src/core/protocols.js";
import { StreamService } from "../../src/core/streams.js";
import { TaskService } from "../../src/core/tasks.js";
import { keys, SYSTEM_STREAMS } from "../../src/redis/keys.js";

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let streams: StreamService;
let tasks: TaskService;
let protocols: ProtocolService;
let fanout: Fanout;
let scheduler: DigestScheduler;

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
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
  streams = new StreamService(main, 100_000, url);
  tasks = new TaskService(main, streams);
  protocols = new ProtocolService(main, streams);
  fanout = new Fanout(blocking, main);
  fanout.start();
  scheduler = new DigestScheduler(main, streams, tasks, protocols, fanout, {
    sweepIntervalMs: 3_600_000, // sweeps are driven manually in tests
    markerTtlSec: 3600,
  });
  await scheduler.start();
});

afterAll(async () => {
  scheduler?.stop();
  await fanout?.stop();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

describe("DigestScheduler", () => {
  it("seeds the stream-digest protocol at boot", async () => {
    const p = await protocols.get("stream-digest");
    expect(p).toBeDefined();
    expect(p!.content).toContain("publish_event");
    expect(p!.content).toContain("digestEventId");
  });

  it("creates one digest task when a stream passes its threshold, and compacts on completion", async () => {
    const stream = "dig-basic";
    for (let i = 0; i < 20; i++) {
      await streams.publish({ stream, type: "work.item", source: "t", payload: { i } });
    }
    await streams.updateMeta(stream, { digestThreshold: 10 }, "admin");

    await scheduler.sweep();
    const marker = await main.get(keys.digestOpen(stream));
    expect(marker).toBeTruthy();

    // Idempotent: a second sweep must not create a second task.
    await scheduler.sweep();
    const digestTasks = (await tasks.list({ limit: 100 })).filter(
      (t) => t.payload?.kind === "stream.digest" && t.payload?.stream === stream,
    );
    expect(digestTasks).toHaveLength(1);
    const task = digestTasks[0]!;
    const toId = task.payload!.toId as string;

    // Play the digesting agent: claim, publish the digest event, complete.
    const claim = await tasks.claim({ taskId: task.id, agent: "digester" });
    expect(claim.claimed).toBe(true);
    const digest = await streams.publish({
      stream,
      type: "stream.digest",
      source: "digester",
      payload: { coversToId: toId, summary: "20 work items processed, none failed" },
    });
    await tasks.complete({ taskId: task.id, agent: "digester", result: { digestEventId: digest.event.id } });

    // The fanout-driven settle should trim through toId and clear the marker.
    await waitFor(async () => (await main.get(keys.digestOpen(stream))) === null, "marker cleared");
    const remaining = await main.xlen(keys.stream(stream));
    expect(remaining).toBeLessThan(20); // old range trimmed
    const events = await streams.read({ stream, limit: 100 });
    expect(events.events.some((e) => e.type === "stream.digest")).toBe(true); // digest survives

    const sys = await streams.read({ stream: SYSTEM_STREAMS.system, limit: 100 });
    expect(sys.events.some((e) => e.type === "stream.compacted" && e.payload.stream === stream)).toBe(true);
  });

  it("does not trim when the digest event id is bogus", async () => {
    const stream = "dig-bogus";
    for (let i = 0; i < 12; i++) {
      await streams.publish({ stream, type: "x", source: "t", payload: { i } });
    }
    await streams.updateMeta(stream, { digestThreshold: 8 }, "admin");
    await scheduler.sweep();

    const task = (await tasks.list({ limit: 100 })).find(
      (t) => t.payload?.kind === "stream.digest" && t.payload?.stream === stream,
    )!;
    await tasks.claim({ taskId: task.id, agent: "liar" });
    await tasks.complete({ taskId: task.id, agent: "liar", result: { digestEventId: "9999999999999-0" } });

    await waitFor(async () => (await main.get(keys.digestOpen(stream))) === null, "marker cleared");
    expect(await main.xlen(keys.stream(stream))).toBe(12); // untouched
  });

  it("clears the marker when the digest task fails so the next sweep retries", async () => {
    const stream = "dig-fail";
    for (let i = 0; i < 12; i++) {
      await streams.publish({ stream, type: "x", source: "t", payload: { i } });
    }
    await streams.updateMeta(stream, { digestThreshold: 8 }, "admin");
    await scheduler.sweep();

    const task = (await tasks.list({ limit: 100 })).find(
      (t) => t.payload?.kind === "stream.digest" && t.payload?.stream === stream,
    )!;
    await tasks.claim({ taskId: task.id, agent: "quitter" });
    await tasks.fail({ taskId: task.id, agent: "quitter", error: "cannot summarize" });

    await waitFor(async () => (await main.get(keys.digestOpen(stream))) === null, "marker cleared after fail");
    await scheduler.sweep(); // retries with a fresh task
    const open = await main.get(keys.digestOpen(stream));
    expect(open).toBeTruthy();
    expect(open).not.toBe(task.id);
  });

  it("never digests reserved system streams", async () => {
    await streams.updateMeta(SYSTEM_STREAMS.tasks, { digestThreshold: 4 }, "admin");
    await scheduler.sweep();
    expect(await main.get(keys.digestOpen(SYSTEM_STREAMS.tasks))).toBeNull();
  });
});
