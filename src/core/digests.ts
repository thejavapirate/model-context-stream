import { Redis } from "ioredis";
import { metrics } from "../metrics.js";
import { keys, SYSTEM_STREAMS } from "../redis/keys.js";
import type { Fanout } from "./fanout.js";
import type { ProtocolService } from "./protocols.js";
import type { StreamService } from "./streams.js";
import type { TaskService } from "./tasks.js";

export interface DigestSettings {
  sweepIntervalMs: number;
  /** Open-digest marker TTL — a stuck/unclaimed digest retries after this. */
  markerTtlSec: number;
}

const DEFAULT_SETTINGS: DigestSettings = {
  sweepIntervalMs: 60_000,
  markerTtlSec: 3600,
};

const DIGEST_PROTOCOL = "stream-digest";
const RESERVED = new Set<string>(Object.values(SYSTEM_STREAMS));

/**
 * Agent-driven stream compaction. When a stream with a digestThreshold grows
 * past it, the scheduler creates a task on our own queue; a connected agent
 * claims it, summarizes the old range into a `stream.digest` event, and
 * completes the task with the digest event id — then the scheduler trims the
 * summarized range. The server never calls an LLM; the fleet maintains its
 * own memory.
 */
export class DigestScheduler {
  private timer?: NodeJS.Timeout;
  private unsub?: () => void;
  /** stream -> open digest taskId (mirror of the Redis markers). */
  private open = new Map<string, string>();

  constructor(
    private readonly redis: Redis,
    private readonly streams: StreamService,
    private readonly tasks: TaskService,
    private readonly protocols: ProtocolService,
    private readonly fanout: Fanout,
    private readonly settings: DigestSettings = DEFAULT_SETTINGS,
  ) {}

  async start(): Promise<void> {
    await this.seedProtocol();
    await this.recover();
    this.unsub = await this.fanout.subscribe(SYSTEM_STREAMS.tasks, (event) => {
      if (event.type === "task.completed" || event.type === "task.failed" || event.type === "task.expired") {
        const taskId = event.payload.taskId;
        if (typeof taskId === "string") void this.onTaskSettled(taskId).catch(() => {});
      }
    });
    this.timer = setInterval(() => void this.sweep().catch((err) => console.error("[digests] sweep:", err)), this.settings.sweepIntervalMs);
    this.timer.unref();
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer);
    this.unsub?.();
  }

  /** One pass: create digest tasks for any over-threshold stream without one. */
  async sweep(): Promise<void> {
    for (const meta of await this.streams.listStreams()) {
      if (!meta.digestThreshold || meta.digestThreshold < 4 || RESERVED.has(meta.name)) continue;
      if (meta.count <= meta.digestThreshold) continue;
      if (await this.redis.get(keys.digestOpen(meta.name))) continue;

      // Keep the newest half-threshold; digest everything older.
      const keep = Math.floor(meta.digestThreshold / 2);
      const digestCount = meta.count - keep;
      const range = (await this.redis.xrange(keys.stream(meta.name), "-", "+", "COUNT", digestCount)) as [
        string,
        string[],
      ][];
      const last = range[range.length - 1];
      if (!last) continue;
      const toId = last[0];

      const task = await this.tasks.create({
        title: `Digest stream "${meta.name}" (${digestCount} events up to ${toId})`,
        description:
          `Compact the oldest ${digestCount} events of stream "${meta.name}" into one digest event. ` +
          `Follow the "${DIGEST_PROTOCOL}" protocol. Range: from the beginning through ${toId} (inclusive).`,
        protocol: DIGEST_PROTOCOL,
        priority: 3,
        payload: { kind: "stream.digest", stream: meta.name, toId, eventCount: digestCount },
        createdBy: "system",
      });
      await this.redis.set(keys.digestOpen(meta.name), task.id, "EX", this.settings.markerTtlSec);
      this.open.set(meta.name, task.id);
    }
  }

  /** Rebuild the in-memory marker map after a restart; settle anything already done. */
  private async recover(): Promise<void> {
    for (const meta of await this.streams.listStreams()) {
      const taskId = await this.redis.get(keys.digestOpen(meta.name));
      if (!taskId) continue;
      this.open.set(meta.name, taskId);
      await this.onTaskSettled(taskId);
    }
  }

  private async onTaskSettled(taskId: string): Promise<void> {
    const entry = [...this.open.entries()].find(([, id]) => id === taskId);
    if (!entry) return;
    const [stream] = entry;

    const task = await this.tasks.get(taskId);
    if (!task) {
      await this.clearMarker(stream);
      return;
    }
    if (task.status === "failed" || (task.status === "pending" && task.attempts > 0)) {
      // Failed or lease-expired back to pending: drop the marker so the next
      // sweep re-evaluates (the same task may still be picked up meanwhile).
      await this.clearMarker(stream);
      return;
    }
    if (task.status !== "completed") return; // still in flight

    const toId = typeof task.payload?.toId === "string" ? task.payload.toId : undefined;
    const digestEventId = typeof task.result?.digestEventId === "string" ? task.result.digestEventId : undefined;
    if (!toId || !digestEventId) {
      console.error(`[digests] task ${taskId} completed without digestEventId/toId — not trimming`);
      await this.clearMarker(stream);
      return;
    }

    // Verify the digest event actually exists before destroying history.
    const found = (await this.redis.xrange(keys.stream(stream), digestEventId, digestEventId)) as [string, string[]][];
    if (found.length === 0) {
      console.error(`[digests] digest event ${digestEventId} not found in ${stream} — not trimming`);
      await this.clearMarker(stream);
      return;
    }

    const removed = await this.streams.trimThrough(stream, toId);
    metrics.compactions.inc();
    await this.clearMarker(stream);
    await this.streams.publish({
      stream: SYSTEM_STREAMS.system,
      type: "stream.compacted",
      source: "system",
      payload: { stream, toId, removed, digestEventId, taskId },
    });
  }

  private async clearMarker(stream: string): Promise<void> {
    this.open.delete(stream);
    await this.redis.del(keys.digestOpen(stream));
  }

  private async seedProtocol(): Promise<void> {
    if (await this.protocols.getHead(DIGEST_PROTOCOL)) return;
    await this.protocols.put({
      name: DIGEST_PROTOCOL,
      updatedBy: "system",
      changelog: "seeded at boot",
      content: `# Stream digest protocol

You claimed a stream-digest task. Its payload contains \`stream\`, \`toId\`, and \`eventCount\`.

1. Read the full range: call \`read_stream\` with \`{stream, limit: 1000}\` repeatedly (page with
   \`fromId\`) until you have every event with id ≤ \`toId\`.
2. Write a digest: a concise but faithful summary of what happened — key decisions, findings,
   completed work, unresolved threads. Preserve anything a future agent would need; drop noise.
3. Publish exactly one event to the SAME stream:
   \`publish_event {stream, type: "stream.digest", payload: {coversToId: <toId>, eventCount, summary: "<your digest>"}}\`
4. Complete the task with the id returned by publish_event:
   \`complete_task {taskId, result: {digestEventId: "<id>"}}\`

The server verifies the digest event exists, then trims the summarized range. If you cannot
complete the digest, fail the task with a reason instead of guessing.`,
    });
  }
}
