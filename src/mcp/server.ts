import { McpServer, ResourceTemplate } from "@modelcontextprotocol/sdk/server/mcp.js";
import { SubscribeRequestSchema, UnsubscribeRequestSchema } from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import type { Config } from "../config.js";
import type { CursorService } from "../core/cursors.js";
import { eventTypeSchema, streamNameSchema } from "../core/events.js";
import type { ProtocolService } from "../core/protocols.js";
import type { StreamService } from "../core/streams.js";
import { TaskError, type TaskService, type TaskStatus } from "../core/tasks.js";
import type { WebhookService } from "../core/webhooks.js";
import type { FederationManager } from "./federation.js";
import type { ListChangedNotifier } from "./notifier.js";
import type { SessionRegistry } from "./sessions.js";
import { uris } from "./uris.js";

export interface Deps {
  config: Config;
  streams: StreamService;
  tasks: TaskService;
  protocols: ProtocolService;
  cursors: CursorService;
  webhooks: WebhookService;
  federation: FederationManager;
  registry: SessionRegistry;
  listChanged: ListChangedNotifier;
  /** Coalesced tools/list_changed (federation registers/removes proxy tools). */
  toolsChanged: ListChangedNotifier;
}

/**
 * Mutable per-session context. `sessionId` is assigned by the transport's
 * onsessioninitialized callback — always set before any tool/resource call,
 * since `initialize` precedes everything else on the wire.
 */
export interface SessionCtx {
  sessionId?: string;
  agentName: string;
  /** True when authenticated with an admin-role token (or auth is disabled). */
  isAdmin: boolean;
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
  const { streams, tasks, protocols, cursors, webhooks, federation, registry, listChanged } = deps;

  const requireAdmin = (): ReturnType<typeof errorResult> | undefined => {
    if (ctx.isAdmin) return undefined;
    return errorResult(`this tool requires an admin token (agent ${ctx.agentName} is not admin)`);
  };

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
        "resource subscriptions. Alternatively pass cursor (a named durable cursor) to resume " +
        "where you last committed — set commit=true to auto-advance it after this read.",
      inputSchema: {
        stream: streamNameSchema,
        fromId: z.string().optional(),
        sinceMs: z.number().int().positive().optional(),
        limit: z.number().int().min(1).max(1000).default(50),
        blockMs: z.number().int().min(0).max(25_000).optional(),
        cursor: z.string().max(64).optional(),
        commit: z.boolean().default(false),
      },
    },
    async ({ cursor, commit, ...input }) => {
      let fromId = input.fromId;
      if (cursor && !fromId) {
        // First resume on a fresh cursor replays from the start of retained history.
        fromId = (await cursors.get(ctx.agentName, input.stream, cursor)) ?? "0-0";
      }
      const res = await streams.read({ ...input, ...(fromId ? { fromId } : {}) });
      let committed: string | undefined;
      if (cursor && commit && res.nextCursor && res.events.length > 0) {
        await cursors.commit(ctx.agentName, input.stream, res.nextCursor, cursor);
        committed = res.nextCursor;
      }
      return json({ ...res, ...(cursor ? { cursor, committed: committed ?? null } : {}) });
    },
  );

  mcp.registerTool(
    "commit_cursor",
    {
      title: "Commit a durable cursor",
      description:
        "Persist your read position on a stream under a named cursor (per-agent, survives " +
        "reconnects). Use after successfully processing events for at-least-once semantics.",
      inputSchema: {
        stream: streamNameSchema,
        id: z.string().min(1),
        cursor: z.string().max(64).default("default"),
      },
    },
    async ({ stream, id, cursor }) => {
      await cursors.commit(ctx.agentName, stream, id, cursor);
      return json({ stream, cursor, id });
    },
  );

  mcp.registerTool(
    "list_cursors",
    {
      title: "List my durable cursors",
      description: "List this agent's saved stream cursors.",
      inputSchema: {},
    },
    async () => json({ agent: ctx.agentName, cursors: await cursors.list(ctx.agentName) }),
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
    "configure_stream",
    {
      title: "Configure a stream (admin)",
      description:
        "Set per-stream retention (maxLen) and compaction (digestThreshold — when the stream " +
        "grows past it, a digest task is created for an agent to summarize old events; 0 " +
        "disables). Admin only.",
      inputSchema: {
        stream: streamNameSchema,
        description: z.string().max(1024).optional(),
        maxLen: z.number().int().min(0).max(1_000_000).optional(),
        digestThreshold: z.number().int().min(0).max(1_000_000).optional(),
      },
    },
    async ({ stream, ...patch }) => {
      const denied = requireAdmin();
      if (denied) return denied;
      return json(await streams.updateMeta(stream, patch, ctx.agentName));
    },
  );

  // ── Webhook tools (admin) ─────────────────────────────────────────────────
  mcp.registerTool(
    "add_webhook",
    {
      title: "Add an outbound webhook (admin)",
      description:
        "POST every event on a stream to an external URL (optionally filtered by event types, " +
        "HMAC-signed with X-MCS-Signature when a secret is set). Admin only.",
      inputSchema: {
        stream: streamNameSchema,
        url: z.string().url().max(2048),
        secret: z.string().max(256).optional(),
        types: z.array(eventTypeSchema).max(32).optional(),
      },
    },
    async ({ stream, url, secret, types }) => {
      const denied = requireAdmin();
      if (denied) return denied;
      const hook = await webhooks.add({
        stream,
        url,
        ...(secret ? { secret } : {}),
        ...(types ? { types } : {}),
        createdBy: ctx.agentName,
      });
      const { secret: _redacted, ...safe } = hook;
      return json({ ...safe, hasSecret: Boolean(secret) });
    },
  );

  mcp.registerTool(
    "remove_webhook",
    {
      title: "Remove an outbound webhook (admin)",
      description: "Delete a webhook by id. Admin only.",
      inputSchema: { id: z.string() },
    },
    async ({ id }) => {
      const denied = requireAdmin();
      if (denied) return denied;
      const removed = await webhooks.remove(id);
      return removed ? json({ removed: id }) : errorResult(`webhook ${id} not found`);
    },
  );

  mcp.registerTool(
    "list_webhooks",
    {
      title: "List outbound webhooks (admin)",
      description: "List configured webhooks (secrets redacted). Admin only.",
      inputSchema: {},
    },
    async () => {
      const denied = requireAdmin();
      if (denied) return denied;
      return json({ webhooks: webhooks.list() });
    },
  );

  // ── Federation tools ──────────────────────────────────────────────────────
  mcp.registerTool(
    "add_upstream",
    {
      title: "Federate an upstream MCP server (admin)",
      description:
        "Connect this server to an upstream MCP server and re-expose its tools to every " +
        "connected agent, namespaced as {name}__{tool}. Admin only.",
      inputSchema: {
        name: z.string().regex(/^[a-zA-Z][a-zA-Z0-9-]{0,31}$/, "letters/digits/hyphens, no underscores"),
        url: z.string().url().max(2048),
        token: z.string().max(512).optional(),
      },
    },
    async ({ name, url, token }) => {
      const denied = requireAdmin();
      if (denied) return denied;
      try {
        return json(await federation.addUpstream({ name, url, ...(token ? { token } : {}), addedBy: ctx.agentName }));
      } catch (err) {
        return errorResult(err instanceof Error ? err.message : String(err));
      }
    },
  );

  mcp.registerTool(
    "remove_upstream",
    {
      title: "Remove a federated upstream (admin)",
      description: "Disconnect an upstream MCP server and remove its proxied tools from every session. Admin only.",
      inputSchema: { name: z.string() },
    },
    async ({ name }) => {
      const denied = requireAdmin();
      if (denied) return denied;
      const removed = await federation.removeUpstream(name);
      return removed ? json({ removed: name }) : errorResult(`upstream ${name} not found`);
    },
  );

  mcp.registerTool(
    "list_upstreams",
    {
      title: "List federated upstream MCP servers",
      description: "Show federated upstreams with connection status and tool counts (tokens never shown).",
      inputSchema: {},
    },
    async () => json({ upstreams: federation.listUpstreams() }),
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
    "agents-online",
    uris.agentsOnline,
    {
      title: "Online agents",
      description:
        "Live roster: every connected agent, when it connected, what it's subscribed to, and " +
        "which tasks it currently holds. Subscribe for presence changes.",
      mimeType: "application/json",
    },
    async (uri) => {
      const active = await tasks.list({ limit: 500 });
      const roster = registry.all().map((s) => ({
        agent: s.agentName,
        sessionId: s.id,
        connectedAt: s.connectedAt,
        lastSeenAt: new Date(s.lastSeenAt).toISOString(),
        subscriptions: [...s.subscriptions.keys()],
        claimedTasks: active
          .filter((t) => (t.status === "claimed" || t.status === "in_progress") && t.claimedBy === s.agentName)
          .map((t) => ({ id: t.id, title: t.title, status: t.status })),
      }));
      return {
        contents: [
          {
            uri: uri.toString(),
            mimeType: "application/json",
            text: JSON.stringify({ count: roster.length, agents: roster }, null, 2),
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
