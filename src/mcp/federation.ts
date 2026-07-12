import os from "node:os";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import type { McpServer, RegisteredTool } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ToolListChangedNotificationSchema } from "@modelcontextprotocol/sdk/types.js";
import { Redis } from "ioredis";
import * as z4 from "zod/v4";
import type { Config } from "../config.js";
import { keys } from "../redis/keys.js";
import type { ListChangedNotifier } from "./notifier.js";

export interface UpstreamRecord {
  name: string;
  url: string;
  token?: string;
  addedBy: string;
  addedAt: string;
}

export interface UpstreamView {
  name: string;
  url: string;
  status: "connecting" | "connected" | "degraded";
  toolCount: number;
  lastError?: string;
  addedBy: string;
  addedAt: string;
}

interface UpstreamTool {
  description?: string;
  inputSchema: unknown;
  /** Serialized schema+description for cheap change detection. */
  fingerprint: string;
}

interface UpstreamRuntime {
  record: UpstreamRecord;
  client?: Client;
  status: "connecting" | "connected" | "degraded";
  lastError?: string;
  tools: Map<string, UpstreamTool>;
  backoffMs: number;
  reconnectTimer?: NodeJS.Timeout;
  reconcileChain: Promise<void>;
  reconcileDebounce?: NodeJS.Timeout;
}

/** One MCP session's set of registered proxy-tool handles. */
export interface Attachment {
  mcp: McpServer;
  handles: Map<string, RegisteredTool>;
}

const NAME_RE = /^[a-zA-Z][a-zA-Z0-9-]{0,31}$/; // no underscores: `{up}__{tool}` stays unambiguous
const RESERVED_NAMES = new Set(["mcs", "self", "local", "admin", "upstream", "system"]);
const SEP = "__";
const BACKOFF_START_MS = 1_000;
const BACKOFF_CAP_MS = 60_000;
const CALL_TIMEOUT_MS = 30_000;

function toolError(message: string) {
  return { content: [{ type: "text" as const, text: `Error: ${message}` }], isError: true as const };
}

/**
 * MCP tool federation: connect to upstream MCP servers (one shared client
 * each) and re-expose their tools as namespaced proxies `{upstream}__{tool}`
 * on every live and future session. The server becomes the fleet's single
 * connection point — install a tool source once, every agent gets it.
 */
export class FederationManager {
  private upstreams = new Map<string, UpstreamRuntime>();
  private attachments = new Set<Attachment>();
  private stopped = false;

  constructor(
    private readonly redis: Redis,
    private readonly config: Config,
    private readonly toolsChanged: ListChangedNotifier,
  ) {}

  async start(): Promise<void> {
    const raw = await this.redis.hgetall(keys.upstreams);
    for (const json of Object.values(raw)) {
      try {
        const record = JSON.parse(json) as UpstreamRecord;
        const rt = this.makeRuntime(record);
        this.upstreams.set(record.name, rt);
        void this.connect(rt);
      } catch {
        /* skip malformed record */
      }
    }
  }

  async stop(): Promise<void> {
    this.stopped = true;
    for (const rt of this.upstreams.values()) {
      if (rt.reconnectTimer) clearTimeout(rt.reconnectTimer);
      if (rt.reconcileDebounce) clearTimeout(rt.reconcileDebounce);
      await rt.client?.close().catch(() => {});
    }
    this.upstreams.clear();
  }

  // ── Admin surface ──────────────────────────────────────────────────────────

  async addUpstream(input: { name: string; url: string; token?: string; addedBy: string }): Promise<UpstreamView> {
    if (!NAME_RE.test(input.name)) {
      throw new Error(`invalid upstream name (letters, digits, hyphens; no underscores): ${input.name}`);
    }
    if (RESERVED_NAMES.has(input.name)) throw new Error(`upstream name is reserved: ${input.name}`);
    if (this.upstreams.has(input.name)) throw new Error(`upstream already exists: ${input.name}`);
    if (this.isSelfUrl(input.url)) throw new Error("refusing to federate this server with itself");

    const record: UpstreamRecord = {
      name: input.name,
      url: input.url,
      ...(input.token ? { token: input.token } : {}),
      addedBy: input.addedBy,
      addedAt: new Date().toISOString(),
    };
    await this.redis.hset(keys.upstreams, record.name, JSON.stringify(record));
    const rt = this.makeRuntime(record);
    this.upstreams.set(record.name, rt);

    // Give the first connect ~5s so the caller sees a real status; failures
    // don't roll back — the background retry keeps going.
    await Promise.race([this.connect(rt), sleep(5_000)]);
    return this.view(rt);
  }

  async removeUpstream(name: string): Promise<boolean> {
    const rt = this.upstreams.get(name);
    await this.redis.hdel(keys.upstreams, name);
    if (!rt) return false;

    this.upstreams.delete(name); // delete first: guards stale reconnects/reconciles
    if (rt.reconnectTimer) clearTimeout(rt.reconnectTimer);
    if (rt.reconcileDebounce) clearTimeout(rt.reconcileDebounce);
    await rt.client?.close().catch(() => {});

    for (const att of this.attachments) {
      for (const [fullName, handle] of att.handles) {
        if (fullName.startsWith(`${name}${SEP}`)) {
          try {
            handle.remove(); // auto-fires list_changed → coalesced by our reroute
          } catch {
            /* session may be closing */
          }
          att.handles.delete(fullName);
        }
      }
    }
    return true;
  }

  listUpstreams(): UpstreamView[] {
    return [...this.upstreams.values()].map((rt) => this.view(rt)).sort((a, b) => a.name.localeCompare(b.name));
  }

  // ── Session attachment ─────────────────────────────────────────────────────

  /** Register all current proxy tools on a session's McpServer. */
  attach(mcp: McpServer): Attachment {
    const att: Attachment = { mcp, handles: new Map() };
    this.attachments.add(att);
    for (const rt of this.upstreams.values()) {
      for (const [toolName, tool] of rt.tools) {
        this.registerProxy(att, rt.record.name, toolName, tool);
      }
    }
    return att;
  }

  detach(att: Attachment): void {
    // The session is going away — no need to .remove() from a dying server.
    this.attachments.delete(att);
  }

  // ── Internals ──────────────────────────────────────────────────────────────

  private makeRuntime(record: UpstreamRecord): UpstreamRuntime {
    return {
      record,
      status: "connecting",
      tools: new Map(),
      backoffMs: BACKOFF_START_MS,
      reconcileChain: Promise.resolve(),
    };
  }

  private view(rt: UpstreamRuntime): UpstreamView {
    return {
      name: rt.record.name,
      url: rt.record.url,
      status: rt.status,
      toolCount: rt.tools.size,
      ...(rt.lastError ? { lastError: rt.lastError } : {}),
      addedBy: rt.record.addedBy,
      addedAt: rt.record.addedAt,
    };
  }

  private isSelfUrl(url: string): boolean {
    try {
      const u = new URL(url);
      const port = u.port ? Number(u.port) : u.protocol === "https:" ? 443 : 80;
      if (port !== this.config.port) return false;
      const host = u.hostname.toLowerCase();
      return (
        host === "localhost" ||
        host === "::1" ||
        host.startsWith("127.") ||
        host === os.hostname().toLowerCase()
      );
    } catch {
      return false;
    }
  }

  private async connect(rt: UpstreamRuntime): Promise<void> {
    if (this.stopped || !this.isLive(rt)) return;
    rt.status = "connecting";
    try {
      const transport = new StreamableHTTPClientTransport(new URL(rt.record.url), {
        requestInit: rt.record.token ? { headers: { Authorization: `Bearer ${rt.record.token}` } } : {},
      });
      const client = new Client(
        { name: "model-context-stream-federation", version: "0.2.0" },
        { capabilities: {} },
      );
      client.setNotificationHandler(ToolListChangedNotificationSchema, () => {
        if (rt.reconcileDebounce) clearTimeout(rt.reconcileDebounce);
        rt.reconcileDebounce = setTimeout(() => this.scheduleReconcile(rt), 250);
        rt.reconcileDebounce.unref();
      });
      transport.onclose = () => {
        if (this.stopped || !this.isLive(rt)) return;
        rt.status = "degraded";
        rt.lastError = "connection closed";
        this.scheduleReconnect(rt);
      };

      await client.connect(transport);
      rt.client = client;
      rt.status = "connected";
      rt.lastError = undefined;
      rt.backoffMs = BACKOFF_START_MS;
      await this.reconcileNow(rt);
    } catch (err) {
      rt.status = "degraded";
      rt.lastError = err instanceof Error ? err.message : String(err);
      this.scheduleReconnect(rt);
    }
  }

  private isLive(rt: UpstreamRuntime): boolean {
    return this.upstreams.get(rt.record.name) === rt;
  }

  private scheduleReconnect(rt: UpstreamRuntime): void {
    if (this.stopped || !this.isLive(rt) || rt.reconnectTimer) return;
    const jitter = Math.random() * 0.3 + 0.85;
    const delay = Math.min(rt.backoffMs * jitter, BACKOFF_CAP_MS);
    rt.backoffMs = Math.min(rt.backoffMs * 2, BACKOFF_CAP_MS);
    rt.reconnectTimer = setTimeout(() => {
      rt.reconnectTimer = undefined;
      void this.connect(rt);
    }, delay);
    rt.reconnectTimer.unref();
  }

  /** Serialize reconciles per upstream. */
  private scheduleReconcile(rt: UpstreamRuntime): void {
    rt.reconcileChain = rt.reconcileChain.then(() => this.reconcileNow(rt)).catch(() => {});
  }

  private async reconcileNow(rt: UpstreamRuntime): Promise<void> {
    if (!this.isLive(rt) || !rt.client || rt.status !== "connected") return;
    let listed: Array<{ name: string; description?: string; inputSchema: unknown }>;
    try {
      listed = [];
      let cursor: string | undefined;
      do {
        const page = await rt.client.listTools(cursor ? { cursor } : {});
        listed.push(...(page.tools as typeof listed));
        cursor = page.nextCursor;
      } while (cursor);
    } catch (err) {
      rt.lastError = `listTools failed: ${err instanceof Error ? err.message : String(err)}`;
      return;
    }
    if (!this.isLive(rt)) return;

    const seen = new Set<string>();
    for (const tool of listed) {
      seen.add(tool.name);
      const fingerprint = JSON.stringify({ d: tool.description, s: tool.inputSchema });
      const existing = rt.tools.get(tool.name);
      if (existing && existing.fingerprint === fingerprint) continue;

      const def: UpstreamTool = {
        ...(tool.description ? { description: tool.description } : {}),
        inputSchema: tool.inputSchema,
        fingerprint,
      };
      rt.tools.set(tool.name, def);
      for (const att of this.attachments) {
        // Changed tool: remove + re-register (update() would re-run schema conversion).
        const fullName = `${rt.record.name}${SEP}${tool.name}`;
        const prior = att.handles.get(fullName);
        if (prior) {
          try {
            prior.remove();
          } catch {
            /* ignore */
          }
          att.handles.delete(fullName);
        }
        this.registerProxy(att, rt.record.name, tool.name, def);
      }
    }
    for (const toolName of [...rt.tools.keys()]) {
      if (seen.has(toolName)) continue;
      rt.tools.delete(toolName);
      const fullName = `${rt.record.name}${SEP}${toolName}`;
      for (const att of this.attachments) {
        const handle = att.handles.get(fullName);
        if (handle) {
          try {
            handle.remove();
          } catch {
            /* ignore */
          }
          att.handles.delete(fullName);
        }
      }
    }
  }

  private registerProxy(att: Attachment, upstreamName: string, toolName: string, tool: UpstreamTool): void {
    const fullName = `${upstreamName}${SEP}${toolName}`;
    if (att.handles.has(fullName)) return;
    try {
      const handle = att.mcp.registerTool(
        fullName,
        {
          title: `${toolName} (via ${upstreamName})`,
          description: `[federated from ${upstreamName}] ${tool.description ?? ""}`.trim(),
          inputSchema: passthroughSchema(tool.inputSchema),
        },
        async (args: unknown) => {
          const rt = this.upstreams.get(upstreamName);
          if (!rt?.client || rt.status !== "connected") {
            return toolError(`upstream ${upstreamName} is unavailable (${rt?.status ?? "removed"})`);
          }
          try {
            const result = await rt.client.callTool(
              { name: toolName, arguments: (args ?? {}) as Record<string, unknown> },
              undefined,
              { timeout: CALL_TIMEOUT_MS },
            );
            return result as ReturnType<typeof toolError>;
          } catch (err) {
            return toolError(
              `upstream ${upstreamName} call failed: ${err instanceof Error ? err.message : String(err)}`,
            );
          }
        },
      );
      att.handles.set(fullName, handle);
    } catch (err) {
      console.error(`[federation] failed to register proxy ${fullName}:`, err);
    }
  }
}

/**
 * Permissive passthrough schema carrying the upstream's real JSON schema as
 * metadata — tools/list shows the true shape; validation stays loose (the
 * upstream re-validates on its side).
 */
function passthroughSchema(inputSchema: unknown): z4.ZodType {
  try {
    if (inputSchema && typeof inputSchema === "object") {
      return z4.looseObject({}).meta(inputSchema as Record<string, unknown>);
    }
  } catch {
    /* fall through */
  }
  return z4.looseObject({});
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
