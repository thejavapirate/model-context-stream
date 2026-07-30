import { loadConfig } from "./config.js";
import { CoordinatorLease } from "./core/coordinator.js";
import { CursorService } from "./core/cursors.js";
import { DigestScheduler } from "./core/digests.js";
import { Fanout } from "./core/fanout.js";
import { PresenceService } from "./core/presence.js";
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

  // Redis being briefly unreachable must not crash-loop the process: serve HTTP
  // immediately, connect in the background, and let /healthz (which does a real
  // round-trip) gate traffic via readiness probes. Introspecting the MCP surface —
  // initialize, tools/list — needs no Redis at all.
  const redis = createRedis(config.redisUrl);
  const redisReady = waitForRedis(redis.main);

  const streams = new StreamService(redis.main, config.streamMaxLen, config.redisUrl);
  const tasks = new TaskService(redis.main, streams);
  const protocols = new ProtocolService(redis.main, streams);
  const cursors = new CursorService(redis.main);

  const fanout = new Fanout(redis.blocking, redis.main);
  fanout.start();

  const presence = new PresenceService(redis.main);
  const registry = new SessionRegistry(fanout, streams, presence);
  registry.startIdleSweeper();
  presence.start(() => registry.snapshotPresence());
  const listChanged = new ListChangedNotifier();
  const toolsChanged = new ListChangedNotifier();

  const webhooks = new WebhookService(redis.main, fanout, streams);

  const federation = new FederationManager(redis.main, config, toolsChanged);

  const digests = new DigestScheduler(redis.main, streams, tasks, protocols, fanout);

  // Leader-only work: webhook delivery + digest scheduling run on exactly one
  // replica fleet-wide. On a single-replica deploy this process acquires the
  // lease on boot, so behavior matches pre-lease versions.
  const coordinator = new CoordinatorLease(redis.main, {
    onGain: async () => {
      await webhooks.activate();
      await digests.start();
    },
    onLose: async () => {
      digests.stop();
      await webhooks.deactivate();
    },
  });
  // Everything below needs a live Redis, so arm it once the connection is up
  // (immediately in the normal case; after N retries if Redis lags the server).
  void redisReady.then(() => {
    void federation.start().catch((err) => console.error("[boot] federation:", err?.message ?? err));
    coordinator.start();
    // All replicas reap: the Lua pops expired leases atomically, so concurrent
    // reapers get disjoint result sets and task.expired fires exactly once.
    tasks.startReaper();
  });
  initRuntimeGauges({ sessions: registry, tasks, presence, coordinator });

  const app = buildApp(
    { config, streams, tasks, protocols, cursors, webhooks, federation, registry, presence, listChanged, toolsChanged },
    async () => (await redis.main.ping()) === "PONG",
    coordinator,
  );

  const server = app.listen(config.port, () => {
    console.log(`[boot] model-context-stream listening on :${config.port}`);
    console.log(`[boot] MCP endpoint:    POST/GET/DELETE /mcp`);
    console.log(`[boot] ingest endpoint: POST /ingest/:stream`);
    console.log(`[boot] read endpoint:   GET /streams/:stream`);
    console.log(`[boot] observability:   GET /healthz, GET /metrics`);
  });

  const shutdown = async (signal: string): Promise<void> => {
    console.log(`[shutdown] ${signal} received, draining…`);
    server.close();
    await coordinator.stop(); // releases the lease + disarms webhooks/digests via onLose
    tasks.stopReaper();
    await federation.stop();
    await webhooks.stop();
    registry.stop();
    presence.stop();
    listChanged.close();
    toolsChanged.close();
    await fanout.stop();
    await redis.quit();
    process.exit(0);
  };
  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));
}

/** Resolve once Redis answers a PING, retrying forever with capped backoff. */
async function waitForRedis(redis: { ping(): Promise<string> }): Promise<void> {
  for (let attempt = 0; ; attempt++) {
    try {
      await redis.ping();
      if (attempt > 0) console.log("[boot] redis reachable — arming Redis-backed services");
      return;
    } catch (err) {
      if (attempt === 0) {
        console.warn(
          `[boot] redis unreachable (${err instanceof Error ? err.message : err}) — serving HTTP anyway; ` +
            "/healthz stays unhealthy and Redis-backed work is armed once it connects",
        );
      }
      await new Promise((r) => setTimeout(r, Math.min(1000 * 2 ** attempt, 15_000)));
    }
  }
}

main().catch((err) => {
  console.error("[boot] fatal:", err);
  process.exit(1);
});
