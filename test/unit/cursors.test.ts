import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { CursorService } from "../../src/core/cursors.js";

let container: StartedRedisContainer;
let redis: Redis;
let cursors: CursorService;

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  redis = new Redis(container.getConnectionUrl());
  cursors = new CursorService(redis);
});

afterAll(async () => {
  redis?.disconnect();
  await container?.stop();
});

describe("CursorService", () => {
  it("commits and resumes", async () => {
    expect(await cursors.get("alice", "deploys")).toBeUndefined();
    await cursors.commit("alice", "deploys", "111-0");
    expect(await cursors.get("alice", "deploys")).toBe("111-0");
    await cursors.commit("alice", "deploys", "222-0");
    expect(await cursors.get("alice", "deploys")).toBe("222-0");
  });

  it("isolates cursors per agent and per name", async () => {
    await cursors.commit("alice", "shared", "100-0", "reader");
    await cursors.commit("bob", "shared", "200-0", "reader");
    await cursors.commit("alice", "shared", "300-0", "writer");

    expect(await cursors.get("alice", "shared", "reader")).toBe("100-0");
    expect(await cursors.get("bob", "shared", "reader")).toBe("200-0");
    expect(await cursors.get("alice", "shared", "writer")).toBe("300-0");
    expect(await cursors.get("alice", "shared")).toBeUndefined(); // "default" untouched
  });

  it("lists an agent's cursors", async () => {
    await cursors.commit("carol", "a", "1-0");
    await cursors.commit("carol", "b", "2-0", "custom");
    const list = await cursors.list("carol");
    expect(list).toEqual([
      { stream: "a", cursor: "default", id: "1-0" },
      { stream: "b", cursor: "custom", id: "2-0" },
    ]);
  });
});
