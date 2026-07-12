import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { StreamService } from "../../src/core/streams.js";
import { TaskError, TaskService } from "../../src/core/tasks.js";
import { keys, SYSTEM_STREAMS } from "../../src/redis/keys.js";

let container: StartedRedisContainer;
let redis: Redis;
let streams: StreamService;
let tasks: TaskService;

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  redis = new Redis(container.getConnectionUrl());
  streams = new StreamService(redis, 1000, container.getConnectionUrl());
  tasks = new TaskService(redis, streams);
});

afterAll(async () => {
  redis?.disconnect();
  await container?.stop();
});

describe("TaskService", () => {
  it("walks the happy path: create → claim → progress → complete", async () => {
    const task = await tasks.create({ title: "write tests", createdBy: "alice" });
    expect(task.status).toBe("pending");

    const claim = await tasks.claim({ taskId: task.id, agent: "bob" });
    expect(claim.claimed).toBe(true);
    if (!claim.claimed) throw new Error("unreachable");
    expect(claim.task.claimedBy).toBe("bob");
    expect(claim.task.leaseExpiresAt).toBeDefined();

    const progressed = await tasks.progress({ taskId: task.id, agent: "bob", message: "halfway", percent: 50 });
    expect(progressed.status).toBe("in_progress");
    expect(progressed.progressPercent).toBe(50);

    const done = await tasks.complete({ taskId: task.id, agent: "bob", result: { ok: true } });
    expect(done.status).toBe("completed");
    expect(done.result).toEqual({ ok: true });
  });

  it("claims have exactly one winner under concurrency", async () => {
    const task = await tasks.create({ title: "contested", createdBy: "alice" });
    const outcomes = await Promise.all(
      Array.from({ length: 25 }, (_, i) => tasks.claim({ taskId: task.id, agent: `agent-${i}` })),
    );
    const winners = outcomes.filter((o) => o.claimed);
    expect(winners).toHaveLength(1);
    const losers = outcomes.filter((o) => !o.claimed);
    expect(losers).toHaveLength(24);
    for (const loser of losers) {
      expect(loser).toMatchObject({ claimed: false, reason: "conflict" });
    }
  });

  it("pops highest priority first when claiming without an id", async () => {
    // Isolate: drain any pending tasks left by earlier tests.
    while ((await tasks.claim({ agent: "drain" })).claimed) {
      /* drain */
    }
    const low = await tasks.create({ title: "low", createdBy: "a", priority: 3 });
    const high = await tasks.create({ title: "high", createdBy: "a", priority: 1 });

    const first = await tasks.claim({ agent: "picker" });
    if (!first.claimed) throw new Error("expected a claim");
    expect(first.task.id).toBe(high.id);

    const second = await tasks.claim({ agent: "picker" });
    if (!second.claimed) throw new Error("expected a claim");
    expect(second.task.id).toBe(low.id);

    const empty = await tasks.claim({ agent: "picker" });
    expect(empty).toMatchObject({ claimed: false, reason: "queue_empty" });
  });

  it("rejects mutations from non-claimants", async () => {
    const task = await tasks.create({ title: "mine", createdBy: "a" });
    await tasks.claim({ taskId: task.id, agent: "owner" });
    await expect(tasks.complete({ taskId: task.id, agent: "thief" })).rejects.toThrow(TaskError);
    await expect(tasks.progress({ taskId: task.id, agent: "thief", message: "hi" })).rejects.toThrow(/claimed by owner/);
  });

  it("reaps expired leases back to pending and counts the attempt", async () => {
    const task = await tasks.create({ title: "flaky agent", createdBy: "a" });
    await tasks.claim({ taskId: task.id, agent: "crasher", leaseSeconds: 300 });

    // Simulate lease expiry without waiting: rewrite the expiry into the past.
    const past = Date.now() - 5_000;
    await redis.hset(keys.task(task.id), "leaseExpiresAt", past);
    await redis.zadd(keys.tasksLeases, past, task.id);

    const released = await tasks.reapExpired();
    expect(released).toContainEqual({ taskId: task.id, previousAgent: "crasher" });

    const after = (await tasks.get(task.id))!;
    expect(after.status).toBe("pending");
    expect(after.claimedBy).toBeUndefined();
    expect(after.attempts).toBe(1);

    // And it is claimable again.
    const reclaim = await tasks.claim({ taskId: task.id, agent: "recoverer" });
    expect(reclaim.claimed).toBe(true);
  });

  it("does not reap a task whose heartbeat won the race", async () => {
    const task = await tasks.create({ title: "alive", createdBy: "a" });
    await tasks.claim({ taskId: task.id, agent: "worker", leaseSeconds: 300 });

    // Stale zset entry, but the hash says the lease is fresh (heartbeat landed).
    await redis.zadd(keys.tasksLeases, Date.now() - 5_000, task.id);

    const released = await tasks.reapExpired();
    expect(released.find((r) => r.taskId === task.id)).toBeUndefined();
    const after = (await tasks.get(task.id))!;
    expect(after.status).toBe("claimed");
    expect(after.claimedBy).toBe("worker");
  });

  it("requeues failed tasks when asked", async () => {
    const task = await tasks.create({ title: "retryable", createdBy: "a" });
    await tasks.claim({ taskId: task.id, agent: "w" });
    const failed = await tasks.fail({ taskId: task.id, agent: "w", error: "boom", requeue: true });
    expect(failed.status).toBe("pending");
    expect(failed.attempts).toBe(1);
    expect(failed.error).toBe("boom");
  });

  it("emits lifecycle events on the system tasks stream", async () => {
    const before = await streams.read({ stream: SYSTEM_STREAMS.tasks, limit: 1 });
    const task = await tasks.create({ title: "observable", createdBy: "a" });
    await tasks.claim({ taskId: task.id, agent: "w" });
    await tasks.complete({ taskId: task.id, agent: "w" });

    const res = await streams.read({
      stream: SYSTEM_STREAMS.tasks,
      fromId: before.nextCursor ?? undefined,
      limit: 100,
    });
    const types = res.events.filter((e) => e.payload.taskId === task.id).map((e) => e.type);
    expect(types).toEqual(["task.created", "task.claimed", "task.completed"]);
  });
});
