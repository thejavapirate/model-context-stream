import { loadConfig } from "./config.js";
import { CursorService } from "./core/cursors.js";
import { DigestScheduler } from "./core/digests.js";
import { Fanout } from "./core/fanout.js";
import { ProtocolService } from "./core/protocols.js";
import { StreamService } from "./core/streams.js";
import { TaskService } from "./core/tasks.js";
import { WebhookService } from "./core/webhooks.js";
import { buildApp } from "./http/app.js";
import { initRuntimeGauges } from "./metrics.js";
import { FederationManager } from "./mcp/federation.js";
import { ListChangedNotifier } from "./mcp/notifier.js";
import { SessionRegistry } from "./mcp/sessions.js";
import { createRedis } from "./redis/client.js";

async function main(): Promise<void> {
  const config = loadConfig();
  if (config.tokens.size === 0) {
    console.warn("[boot] MCS_TOKENS is empty — running WITHOUT auth (local dev only; every session is admin)");
  }

  const redis = createRedis(config.redisUrl);
  await redis.main.ping();

  const streams = new StreamService(redis.main, config.streamMaxLen, config.redisUrl);
  const tasks = new TaskService(redis.main, streams);
  const protocols = new ProtocolService(redis.main, streams);
  const cursors = new CursorService(redis.main);

  const fanout = new Fanout(redis.blocking, redis.main);
  fanout.start();

  const registry = new SessionRegistry(fanout, streams);
  registry.startIdleSweeper();
  const listChanged = new ListChangedNotifier();
  const toolsChanged = new ListChangedNotifier();

  const webhooks = new WebhookService(redis.main, fanout, streams);
  await webhooks.start();

  const federation = new FederationManager(redis.main, config, toolsChanged);
  await federation.start();

  const digests = new DigestScheduler(redis.main, streams, tasks, protocols, fanout);
  await digests.start();

  tasks.startReaper();
  initRuntimeGauges({ sessions: registry, tasks });

  const app = buildApp(
    { config, streams, tasks, protocols, cursors, webhooks, federation, registry, listChanged, toolsChanged },
    async () => (await redis.main.ping()) === "PONG",
  );

  const server = app.listen(config.port, () => {
    console.log(`[boot] model-context-stream listening on :${config.port}`);
    console.log(`[boot] MCP endpoint:    POST/GET/DELETE /mcp`);
    console.log(`[boot] ingest endpoint: POST /ingest/:stream`);
    console.log(`[boot] observability:   GET /healthz, GET /metrics`);
  });

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[shutdown] ${signal} received, draining…`);
    server.close();
    digests.stop();
    tasks.stopReaper();
    await federation.stop();
    await webhooks.stop();
    registry.stop();
    listChanged.close();
    toolsChanged.close();
    await fanout.stop();
    await redis.quit();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

main().catch((err) => {
  console.error("[boot] fatal:", err);
  process.exit(1);
});
