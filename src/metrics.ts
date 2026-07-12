import { collectDefaultMetrics, Counter, Gauge, Registry } from "prom-client";
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
export function initRuntimeGauges(deps: { sessions: SessionRegistry; tasks: TaskService }): void {
  new Gauge({
    name: "mcs_connected_sessions",
    help: "Currently connected MCP sessions",
    registers: [registry],
    collect() {
      this.set(deps.sessions.all().length);
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
