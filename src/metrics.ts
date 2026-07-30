import { collectDefaultMetrics, Counter, Gauge, Registry } from "prom-client";
import type { CoordinatorLease } from "./core/coordinator.js";
import type { PresenceService } from "./core/presence.js";
import type { TaskService } from "./core/tasks.js";
import type { SessionRegistry } from "./mcp/sessions.js";

export const registry = new Registry();
collectDefaultMetrics({ register: registry });

/** Incremented from the hot paths; scrape-time gauges are wired in initRuntimeGauges. */
export const metrics = {
  eventsPublished: new Counter({
    name: "mcs_events_published_total",
    help: "Events appended to context streams (tools + ingest + system)",
    registers: [registry],
  }),
  webhookFailures: new Counter({
    name: "mcs_webhook_failed_deliveries_total",
    help: "Webhook deliveries that exhausted all retry attempts",
    registers: [registry],
  }),
  compactions: new Counter({
    name: "mcs_streams_compacted_total",
    help: "Digest-driven stream compactions applied",
    registers: [registry],
  }),
};

/** Wire scrape-time gauges to live services. Call once at boot. */
export function initRuntimeGauges(deps: {
  sessions: SessionRegistry;
  tasks: TaskService;
  presence: PresenceService;
  coordinator: CoordinatorLease;
}): void {
  new Gauge({
    name: "mcs_connected_sessions",
    help: "MCP sessions connected to THIS replica",
    registers: [registry],
    collect() {
      this.set(deps.sessions.all().length);
    },
  });
  new Gauge({
    name: "mcs_presence_sessions",
    help: "Fleet-wide live MCP sessions (Redis-backed presence, all replicas)",
    registers: [registry],
    async collect() {
      this.set((await deps.presence.roster()).length);
    },
  });
  new Gauge({
    name: "mcs_coordinator_is_leader",
    help: "1 when this replica holds the coordinator lease (webhooks + digest scheduling)",
    registers: [registry],
    collect() {
      this.set(deps.coordinator.isLeader ? 1 : 0);
    },
  });
  new Gauge({
    name: "mcs_tasks",
    help: "Tasks by status",
    labelNames: ["status"],
    registers: [registry],
    async collect() {
      const summary = await deps.tasks.queueSummary();
      for (const [status, count] of Object.entries(summary.counts)) {
        this.set({ status }, count);
      }
    },
  });
}
