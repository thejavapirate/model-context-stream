import { execFile } from "node:child_process";
import type { Server } from "node:http";
import { promisify } from "node:util";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
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

const run = promisify(execFile);
const TOKEN = "tok_fleetkit";
const CATCHUP = new URL("../../fleet-kit/mcs-catchup.mjs", import.meta.url).pathname;
const STANDBY = new URL("../../fleet-kit/mcs-standby.mjs", import.meta.url).pathname;

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let streams: StreamService;
let fanout: Fanout;
let server: Server;
let baseUrl: string;

/** Run a fleet-kit script; capture stdout + exit code (they never throw on failure). */
async function script(path: string, env: Record<string, string>): Promise<{ out: string; code: number }> {
  try {
    const { stdout } = await run(process.execPath, [path], {
      env: { ...process.env, MCS_URL: baseUrl, MCS_TOKEN: TOKEN, ...env },
    });
    return { out: stdout.trim(), code: 0 };
  } catch (err) {
    const e = err as { stdout?: string; code?: number };
    return { out: (e.stdout ?? "").trim(), code: e.code ?? -1 };
  }
}

const publish = (stream: string, type: string, payload: Record<string, unknown> = {}) =>
  streams.publish({ stream, type, source: "tester", payload });

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
  const config = loadConfig({ REDIS_URL: url, MCS_TOKENS: `${TOKEN}:kit-agent` } as NodeJS.ProcessEnv);
  streams = new StreamService(main, 1000, url);
  const tasks = new TaskService(main, streams);
  fanout = new Fanout(blocking, main);
  fanout.start();
  const presence = new PresenceService(main);
  const app = buildApp(
    {
      config,
      streams,
      tasks,
      protocols: new ProtocolService(main, streams),
      cursors: new CursorService(main),
      webhooks: new WebhookService(main, fanout, streams),
      federation: new FederationManager(main, config, new ListChangedNotifier()),
      registry: new SessionRegistry(fanout, streams, presence),
      presence,
      listChanged: new ListChangedNotifier(),
      toolsChanged: new ListChangedNotifier(),
    },
    async () => true,
  );
  server = app.listen(0);
  const addr = server.address();
  if (addr === null || typeof addr === "string") throw new Error("no port");
  baseUrl = `http://127.0.0.1:${addr.port}`;
});

afterAll(async () => {
  server?.close();
  await fanout?.stop();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

describe("mcs-catchup (turn-boundary hook)", () => {
  const env = { MCS_AGENT_NAME: "catcher", MCS_FOLLOW: "kit-catch" };

  it("--init fast-forwards past retained history so the first prompt isn't flooded", async () => {
    for (let i = 0; i < 3; i++) await publish("kit-catch", "old.event", { i });
    const init = await script(CATCHUP, { ...env, MCS_AGENT_NAME: "catcher" });
    expect(init.code).toBe(0);

    const first = await script(CATCHUP, env); // pre-init this would dump the history
    expect(first.out).toBe("");
  });

  it("injects a digest of new events, then goes quiet (cursor advanced)", async () => {
    await publish("kit-catch", "ci.build.failed", { repo: "api" });
    await publish("kit-catch", "task.created", { title: "fix it" });

    const digest = await script(CATCHUP, env);
    expect(digest.out).toContain("2 new events on stream://kit-catch");
    expect(digest.out).toContain("ci.build.failed");
    expect(digest.out).toContain("task.created");

    expect((await script(CATCHUP, env)).out).toBe(""); // at-least-once, not repeatedly
  });

  it("paces a backlog and points at the rest", async () => {
    for (let i = 0; i < 10; i++) await publish("kit-catch", "seq.item", { i });
    const paced = await script(CATCHUP, { ...env, MCS_HOOK_MAX_EVENTS: "3" });
    expect(paced.out).toContain("3 new events");
    expect(paced.out).toContain("more waiting");
  });

  it("never breaks a prompt: a dead server prints nothing and exits 0", async () => {
    const dead = await script(CATCHUP, { ...env, MCS_URL: "http://127.0.0.1:9" });
    expect(dead).toEqual({ out: "", code: 0 });
  });
});

describe("mcs-standby (idle self-resume hook)", () => {
  const env = { MCS_AGENT_NAME: "sleeper", MCS_FOLLOW: "kit-standby", MCS_STANDBY_MAX_MS: "3000" };

  it("drains retained history silently — stale events never re-wake a session", async () => {
    for (let i = 0; i < 3; i++) await publish("kit-standby", "task.created", { i });
    const quiet = await script(STANDBY, env); // fresh cursor + backlog
    expect(quiet).toEqual({ out: "", code: 0 });
  });

  it("re-wakes (exit 2) with a digest when an actionable event arrives mid-standby", async () => {
    const pending = script(STANDBY, { ...env, MCS_STANDBY_MAX_MS: "8000" });
    setTimeout(() => void publish("kit-standby", "task.created", { late: true }), 600);
    const woke = await pending;
    expect(woke.code).toBe(2);
    expect(woke.out).toContain("task.created");
  });

  it("stays asleep for chatter, unless the type allowlist opts in", async () => {
    const chatter = script(STANDBY, { ...env, MCS_STANDBY_MAX_MS: "4000" });
    setTimeout(() => void publish("kit-standby", "agent.status", { msg: "noted" }), 500);
    expect((await chatter).code).toBe(0); // not actionable → no paid session

    const all = script(STANDBY, { ...env, MCS_STANDBY_MAX_MS: "8000", MCS_STANDBY_TYPES: "*" });
    setTimeout(() => void publish("kit-standby", "agent.status", { msg: "again" }), 600);
    expect((await all).code).toBe(2);
  });

  it("ignores its own events (no self-wake loops)", async () => {
    const selfTalk = script(STANDBY, { ...env, MCS_AGENT_NAME: "tester", MCS_STANDBY_MAX_MS: "4000" });
    setTimeout(() => void publish("kit-standby", "task.created", { mine: true }), 500); // source: "tester"
    expect((await selfTalk).code).toBe(0);
  });
});
