import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import type { Server } from "node:http";
import { afterAll, beforeAll, describe, it } from "vitest";
import { loadConfig } from "../../src/config.js";
import { Fanout } from "../../src/core/fanout.js";
import { ProtocolService } from "../../src/core/protocols.js";
import { StreamService } from "../../src/core/streams.js";
import { TaskService } from "../../src/core/tasks.js";
import { buildApp } from "../../src/http/app.js";
import { ListChangedNotifier } from "../../src/mcp/notifier.js";
import { SessionRegistry } from "../../src/mcp/sessions.js";
import { createRedis, type RedisConnections } from "../../src/redis/client.js";
import { runSmokeChecks } from "../../scripts/smoke-checks.js";

const TOKEN = "tok_e2e";

let container: StartedRedisContainer;
let redis: RedisConnections;
let fanout: Fanout;
let registry: SessionRegistry;
let listChanged: ListChangedNotifier;
let tasks: TaskService;
let server: Server;
let baseUrl: string;

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();

  // config.port is unused here — the test binds an ephemeral port directly.
  const config = loadConfig({ REDIS_URL: url, MCS_TOKENS: `${TOKEN}:e2e-token-agent` } as NodeJS.ProcessEnv);
  redis = createRedis(url);

  const streams = new StreamService(redis.main, config.streamMaxLen, url);
  tasks = new TaskService(redis.main, streams);
  const protocols = new ProtocolService(redis.main, streams);

  fanout = new Fanout(redis.blocking, redis.main);
  fanout.start();
  registry = new SessionRegistry(fanout);
  listChanged = new ListChangedNotifier();
  tasks.startReaper();

  const app = buildApp({ config, streams, tasks, protocols, registry, listChanged }, async () => {
    return (await redis.main.ping()) === "PONG";
  });
  server = app.listen(0);
  const address = server.address();
  if (address === null || typeof address === "string") throw new Error("no port");
  baseUrl = `http://127.0.0.1:${address.port}`;
});

afterAll(async () => {
  tasks?.stopReaper();
  registry?.stop();
  listChanged?.close();
  server?.close();
  await fanout?.stop();
  await redis?.quit();
  await container?.stop();
});

describe.skipIf(!process.env.RUN_E2E)("end-to-end smoke", () => {
  it("passes the full two-client smoke: notify, claim race, ingest", async () => {
    await runSmokeChecks({ baseUrl, token: TOKEN }, () => {});
  }, 60_000);
});
