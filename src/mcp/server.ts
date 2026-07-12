import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SubscribeRequestSchema, UnsubscribeRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import type { Config } from "../config.js";
import { eventTypeSchema, streamNameSchema } from "../core/events.js";
import type { ProtocolService } from "../core/protocols.js";
import type { StreamService } from "../core/streams.js";
import { TaskError, type TaskService, type TaskStatus } from "../core/tasks.js";
import type { ListChangedNotifier } from "./notifier.js";
import type { SessionRegistry } from "./sessions.js";
import { uris } from "./uris.js";

export interface Deps {
  config: Config;
  streams: StreamService;
  tasks: TaskService;
  protocols: ProtocolService;
  registry: SessionRegistry;
  listChanged: ListChangedNotifier;
}

/**
 * Mutable per-session context. `sessionId` is assigned by the transport's
 * onsessioninitialized callback — always set before any tool/resource call,
 * since `initialize` precedes everything else on the wire.
 */
export interface SessionCtx {
  sessionId?: string;
  agentName: string;
}

function json(data: unknown) {
  return { content: [{ type: "text" as const, text: JSON.stringify(data, null, 2) }] };
}

function errorResult(message: string) {
  return { content: [{ type: "text" as const, text: `Error: ${message}` }], isError: true as const };
}

const taskStatusSchema = z.enum(["pending", "claimed", "in_progress", "completed", "failed"]);
const protocolNameSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/);

/** Build a per-session McpServer wired to the shared services. */
export function buildMcpServer(deps: Deps, ctx: SessionCtx): McpServer {
  const { streams, tasks, protocols, registry, listChanged } = deps;

  const mcp = new McpServer(
    { name: "model-context-stream", version: "0.1.0" },
    {
      capabilities: {
        resources: { subscribe: true, listChanged: true },
        tools: {},
        prompts: { listChanged: true },
      },
    },
  );

  const requireSession = () => {
    if (!ctx.sessionId) throw new Error("session not initialized");
    return ctx.sessionId;
  };

  // ── Subscriptions (we own the handlers; the fanout registry is ours) ──────
  mcp.server.setRequestHandler(SubscribeRequestSchema, async (req) => {
    const ok = await registry.subscribeUri(requireSession(), req.params.uri);
    if (!ok) throw new Error(`resource is not subscribable: ${req.params.uri}`);
    return {};
  });
  mcp.server.setRequestHandler(UnsubscribeRequestSchema, async (req) => {
    registry.unsubscribeUri(requireSession(), req.params.uri);
    return {};
  });

  // ── Stream tools ──────────────────────────────────────────────────────────
  mcp.registerTool(
    "publish_event",
    {
      title: "Publish an event to a context stream",
      description:
        "Append an event to a named context stream. Creates the stream on first publish. " +
        "Every agent subscribed to stream://{stream} is notified. Use dot-namespaced types " +
        "like 'code.refactor.started' or 'finding.discovered'.",
      inputSchema: {
        stream: streamNameSchema,
        type: eventTypeSchema,
        payload: z.record(z.unknown()).default({}),
        correlationId: z.string().max(256).optional(),
      },
    },
    async ({ stream, type, payload, correlationId }) => {
      const res = await streams.publish({
        stream,
        type,
        payload: payload ?? {},
        source: ctx.agentName,
        ...(correlationId ? { correlationId } : {}),
      });
      if (res.createdStream) listChanged.enqueue();
      return json({ id: res.event.id, stream, ts: res.event.ts });
    },
  );

  mcp.registerTool(
    "read_stream",
    {
      title: "Read events from a context stream",
      description:
        "Read events from a stream. Use fromId (exclusive cursor from a previous read) to page " +
        "forward, sinceMs for time-based replay, or neither for the most recent events. " +
        "Set blockMs (max 25000) to long-poll for new events when your client does not support " +
        "resource subscriptions.",
      inputSchema: {
        stream: streamNameSchema,
        fromId: z.string().optional(),
        sinceMs: z.number().int().positive().optional(),
        limit: z.number().int().min(1).max(1000).default(50),
        blockMs: z.number().int().min(0).max(25_000).optional(),
      },
    },
    async (input) => json(await streams.read(input)),
  );

  mcp.registerTool(
    "list_streams",
    {
      title: "List context streams",
      description: "List all registered context streams with metadata, event count, and latest entry id.",
      inputSchema: {},
    },
    async () => json({ streams: await streams.listStreams() }),
  );

  // ── Task tools ────────────────────────────────────────────────────────────
  mcp.registerTool(
    "create_task",
    {
      title: "Create a task on the shared queue",
      description:
        "Add a task to the shared work queue. Any connected agent can claim it. " +
        "Priority 1 is highest, 3 lowest. Optionally reference a protocol the claimant should follow.",
      inputSchema: {
        title: z.string().min(1).max(512),
        description: z.string().max(8192).optional(),
        protocol: protocolNameSchema.optional(),
        priority: z.number().int().min(1).max(3).default(2),
        payload: z.record(z.unknown()).optional(),
        correlationId: z.string().max(256).optional(),
      },
    },
    async (input) => json(await tasks.create({ ...input, createdBy: ctx.agentName })),
  );

  mcp.registerTool(
    "claim_task",
    {
      title: "Claim a task (atomic, exactly one winner)",
      description:
        "Claim a specific task by id, or omit taskId to pop the highest-priority pending task. " +
        "Claims are leases: call update_task_progress at least every leaseSeconds or the task is " +
        "released back to the queue.",
      inputSchema: {
        taskId: z.string().optional(),
        leaseSeconds: z.number().int().min(10).max(3600).default(300),
      },
    },
    async ({ taskId, leaseSeconds }) => {
      const outcome = await tasks.claim({ ...(taskId ? { taskId } : {}), agent: ctx.agentName, leaseSeconds });
      return json(outcome);
    },
  );

  mcp.registerTool(
    "update_task_progress",
    {
      title: "Report task progress (extends your lease)",
      description:
        "Record progress on a task you claimed. Doubles as the lease heartbeat — every call " +
        "extends your lease by the original leaseSeconds.",
      inputSchema: {
        taskId: z.string(),
        message: z.string().min(1).max(4096),
        percent: z.number().min(0).max(100).optional(),
      },
    },
    async (input) => {
      try {
        return json(await tasks.progress({ ...input, agent: ctx.agentName }));
      } catch (err) {
        if (err instanceof TaskError) return errorResult(err.message);
        throw err;
      }
    },
  );

  mcp.registerTool(
    "complete_task",
    {
      title: "Complete a task you claimed",
      description: "Mark a claimed task completed, with an optional structured result.",
      inputSchema: {
        taskId: z.string(),
        result: z.record(z.unknown()).optional(),
      },
    },
    async (input) => {
      try {
        return json(await tasks.complete({ ...input, agent: ctx.agentName }));
      } catch (err) {
        if (err instanceof TaskError) return errorResult(err.message);
        throw err;
      }
    },
  );

  mcp.registerTool(
    "fail_task",
    {
      title: "Fail a task you claimed",
      description: "Mark a claimed task failed. Set requeue=true to put it back on the queue for retry.",
      inputSchema: {
        taskId: z.string(),
        error: z.string().min(1).max(8192),
        requeue: z.boolean().default(false),
      },
    },
    async (input) => {
      try {
        return json(await tasks.fail({ ...input, agent: ctx.agentName }));
      } catch (err) {
        if (err instanceof TaskError) return errorResult(err.message);
        throw err;
      }
    },
  );

  mcp.registerTool(
    "release_task",
    {
      title: "Release a task back to the queue",
      description: "Give up a task you claimed without failing it; it returns to pending for another agent.",
      inputSchema: {
        taskId: z.string(),
        reason: z.string().max(4096).optional(),
      },
    },
    async (input) => {
      try {
        return json(await tasks.release({ ...input, agent: ctx.agentName }));
      } catch (err) {
        if (err instanceof TaskError) return errorResult(err.message);
        throw err;
      }
    },
  );

  mcp.registerTool(
    "list_tasks",
    {
      title: "List tasks",
      description: "List tasks on the shared queue, optionally filtered by status.",
      inputSchema: {
        status: taskStatusSchema.optional(),
        limit: z.number().int().min(1).max(500).default(50),
      },
    },
    async ({ status, limit }) =>
      json({ tasks: await tasks.list({ ...(status ? { status: status as TaskStatus } : {}), limit }) }),
  );

  // ── Protocol tools ────────────────────────────────────────────────────────
  mcp.registerTool(
    "list_protocols",
    {
      title: "List protocols",
      description: "List all protocols (versioned playbooks/SOPs) with their latest version numbers.",
      inputSchema: {},
    },
    async () => json({ protocols: await protocols.list() }),
  );

  mcp.registerTool(
    "get_protocol",
    {
      title: "Get a protocol",
      description: "Fetch a protocol's markdown body — the latest version, or a pinned version.",
      inputSchema: {
        name: protocolNameSchema,
        version: z.number().int().min(1).optional(),
      },
    },
    async ({ name, version }) => {
      const p = await protocols.get(name, version);
      if (!p) return errorResult(`protocol ${name}${version ? ` v${version}` : ""} not found`);
      return json(p);
    },
  );

  mcp.registerTool(
    "put_protocol",
    {
      title: "Create or update a protocol",
      description:
        "Write a new version of a protocol (versions are immutable; this bumps the head). " +
        "All agents subscribed to protocol://{name} are notified.",
      inputSchema: {
        name: protocolNameSchema,
        content: z.string().min(1).max(262_144),
        changelog: z.string().max(2048).optional(),
      },
    },
    async ({ name, content, changelog }) => {
      const res = await protocols.put({ name, content, updatedBy: ctx.agentName, ...(changelog ? { changelog } : {}) });
      listChanged.enqueue();
      return json(res);
    },
  );

  mcp.registerTool(
    "whoami",
    {
      title: "Who am I",
      description: "Return this session's identity, connection time, and active subscriptions.",
      inputSchema: {},
    },
    async () => {
      const session = ctx.sessionId ? registry.get(ctx.sessionId) : undefined;
      return json({
        agent: ctx.agentName,
        sessionId: ctx.sessionId ?? null,
        connectedAt: session?.connectedAt ?? null,
        subscriptions: session ? [...session.subscriptions.keys()] : [],
      });
    },
  );

  // ── Resources ─────────────────────────────────────────────────────────────
  // Registration order matters: templates are matched first-registered-first,
  // and simple {name} expansion matches greedily across '?' and '/'.
  mcp.registerResource(
    "stream-replay",
    new ResourceTemplate("stream://{name}{?from}", { list: undefined }),
    {
      title: "Context stream (replay from cursor)",
      description: "Events after the given entry id. Use the previous read's last event id as `from`.",
      mimeType: "application/json",
    },
    async (uri, variables) => {
      const name = String(variables.name);
      const from = variables.from ? String(variables.from) : undefined;
      const res = await streams.read({ stream: name, ...(from ? { fromId: from } : {}), limit: 200 });
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: JSON.stringify({ stream: name, ...res }, null, 2),
          },
        ],
      };
    },
  );

  mcp.registerResource(
    "stream",
    new ResourceTemplate("stream://{name}", {
      list: async () => ({
        resources: (await streams.listStreams()).map((s) => ({
          uri: uris.stream(s.name),
          name: `stream: ${s.name}`,
          description: `${s.description ?? "context stream"} (${s.count} events)`,
          mimeType: "application/json",
        })),
      }),
    }),
    {
      title: "Context stream",
      description: "The most recent events on a context stream. Subscribe for live updates.",
      mimeType: "application/json",
    },
    async (uri, variables) => {
      // Defensive: greedy template match can swallow a query string.
      const name = String(variables.name).split("?")[0]!;
      const { events, nextCursor } = await streams.read({ stream: name, limit: 50 });
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: JSON.stringify({ stream: name, latestId: nextCursor, count: events.length, events }, null, 2),
          },
        ],
      };
    },
  );

  mcp.registerResource(
    "tasks-queue",
    uris.tasksQueue,
    {
      title: "Task queue board",
      description: "Counts by status plus pending and active task cards. Subscribe for live updates.",
      mimeType: "application/json",
    },
    async (uri) => ({
      contents: [
        {
          uri: uri.toString(),
          mimeType: "application/json",
          text: JSON.stringify(await tasks.queueSummary(), null, 2),
        },
      ],
    }),
  );

  mcp.registerResource(
    "task",
    new ResourceTemplate("task://{id}", { list: undefined }),
    {
      title: "Task",
      description: "A single task's full record. Subscribe for live lifecycle updates.",
      mimeType: "application/json",
    },
    async (uri, variables) => {
      const task = await tasks.get(String(variables.id));
      if (!task) throw new Error(`task ${String(variables.id)} not found`);
      return {
        contents: [{ uri: uri.toString(), mimeType: "application/json", text: JSON.stringify(task, null, 2) }],
      };
    },
  );

  mcp.registerResource(
    "protocol-version",
    new ResourceTemplate("protocol://{name}/v{version}", { list: undefined }),
    {
      title: "Protocol (pinned version)",
      description: "An immutable pinned version of a protocol.",
      mimeType: "text/markdown",
    },
    async (uri, variables) => {
      const p = await protocols.get(String(variables.name), Number(variables.version));
      if (!p) throw new Error(`protocol ${String(variables.name)} v${String(variables.version)} not found`);
      return { contents: [{ uri: uri.toString(), mimeType: "text/markdown", text: p.content }] };
    },
  );

  mcp.registerResource(
    "protocol",
    new ResourceTemplate("protocol://{name}", {
      list: async () => ({
        resources: (await protocols.list()).map((p) => ({
          uri: uris.protocol(p.name),
          name: `protocol: ${p.name}`,
          description: `v${p.latest}, updated ${p.updatedAt} by ${p.updatedBy}`,
          mimeType: "text/markdown",
        })),
      }),
    }),
    {
      title: "Protocol",
      description: "The latest version of a protocol (playbook/SOP). Subscribe for updates.",
      mimeType: "text/markdown",
    },
    async (uri, variables) => {
      const name = String(variables.name).split("?")[0]!;
      const p = await protocols.get(name);
      if (!p) throw new Error(`protocol ${name} not found`);
      return { contents: [{ uri: uri.toString(), mimeType: "text/markdown", text: p.content }] };
    },
  );

  // ── Prompts ───────────────────────────────────────────────────────────────
  mcp.registerPrompt(
    "follow_protocol",
    {
      title: "Follow a protocol",
      description: "Frame a protocol as an SOP you are executing right now.",
      argsSchema: { name: z.string() },
    },
    async ({ name }) => {
      const p = await protocols.get(name);
      if (!p) throw new Error(`protocol ${name} not found`);
      return {
        messages: [
          {
            role: "user" as const,
            content: {
              type: "text" as const,
              text:
                `You are executing the shared operating procedure "${p.name}" (version ${p.version}). ` +
                `Follow it precisely; if a step cannot be completed, publish a blocking event rather than improvising.\n\n---\n\n${p.content}`,
            },
          },
        ],
      };
    },
  );

  mcp.registerPrompt(
    "catch_up",
    {
      title: "Catch up on a stream",
      description: "Digest of recent events on a context stream, for a freshly started agent.",
      argsSchema: { stream: z.string(), sinceMs: z.string().optional() },
    },
    async ({ stream, sinceMs }) => {
      const res = await streams.read({
        stream,
        ...(sinceMs ? { sinceMs: Number(sinceMs) } : {}),
        limit: 100,
      });
      const lines = res.events.map(
        (e) => `- [${e.ts}] ${e.type} (from ${e.source}${e.correlationId ? `, cid=${e.correlationId}` : ""}): ${JSON.stringify(e.payload)}`,
      );
      return {
        messages: [
          {
            role: "user" as const,
            content: {
              type: "text" as const,
              text:
                `Catch up on the "${stream}" context stream before continuing your work. ` +
                `${res.events.length} recent event(s):\n\n${lines.join("\n") || "(no events)"}\n\n` +
                `Resume reading from cursor ${res.nextCursor ?? "(start)"} via the read_stream tool.`,
            },
          },
        ],
      };
    },
  );

  return mcp;
}
