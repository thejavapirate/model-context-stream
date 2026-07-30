/**
 * End-to-end smoke checks, shared by scripts/smoke.ts (against a running
 * server) and test/e2e/smoke.test.ts (in-process). Exercises the full loop:
 * two MCP clients, live notifications, the claim race, and HTTP ingest.
 */
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { ResourceUpdatedNotificationSchema } from "@modelcontextprotocol/sdk/types.js";
import { nanoid } from "nanoid";

export interface SmokeTarget {
  /** e.g. http://localhost:3000 — no trailing slash */
  baseUrl: string;
  token?: string;
}

interface AgentHandle {
  client: Client;
  notifications: string[]; // updated-resource URIs, in arrival order
  close(): Promise<void>;
}

function headers(target: SmokeTarget, agentName: string): Record<string, string> {
  return {
    ...(target.token ? { Authorization: `Bearer ${target.token}` } : {}),
    "X-Agent-Name": agentName,
  };
}

async function connectAgent(target: SmokeTarget, agentName: string): Promise<AgentHandle> {
  const transport = new StreamableHTTPClientTransport(new URL(`${target.baseUrl}/mcp`), {
    requestInit: { headers: headers(target, agentName) },
  });
  const client = new Client({ name: agentName, version: "0.0.1" }, { capabilities: {} });
  await client.connect(transport);

  const notifications: string[] = [];
  client.setNotificationHandler(ResourceUpdatedNotificationSchema, (n) => {
    notifications.push(n.params.uri);
  });

  // Give the standalone GET SSE stream a beat to establish server-side.
  await sleep(250);
  return { client, notifications, close: () => client.close() };
}

function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function waitFor(check: () => boolean, what: string, timeoutMs = 5000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return;
    await sleep(50);
  }
  throw new Error(`timed out after ${timeoutMs}ms waiting for: ${what}`);
}

function toolJson<T = Record<string, unknown>>(result: Awaited<ReturnType<Client["callTool"]>>): T {
  const content = result.content as Array<{ type: string; text?: string }>;
  const text = content.find((c) => c.type === "text")?.text;
  if (!text) throw new Error("tool returned no text content");
  return JSON.parse(text) as T;
}

export async function runSmokeChecks(target: SmokeTarget, log: (msg: string) => void = console.log): Promise<void> {
  const run = nanoid(6);
  const streamName = `smoke-${run}`;
  const streamUri = `stream://${streamName}`;

  const a = await connectAgent(target, "smoke-agent-a");
  const b = await connectAgent(target, "smoke-agent-b");

  try {
    // ── Check 1: publish → live resources/updated notification → replay ────
    log(`[1/7] live notification on ${streamUri}`);
    await b.client.subscribeResource({ uri: streamUri });
    await b.client.subscribeResource({ uri: "tasks://queue" });

    const cid = `cid-${run}`;
    await a.client.callTool({
      name: "publish_event",
      arguments: { stream: streamName, type: "smoke.ping", payload: { hello: "world" }, correlationId: cid },
    });

    await waitFor(() => b.notifications.includes(streamUri), `B notified for ${streamUri}`);

    const read = toolJson<{ events: Array<{ type: string; correlationId?: string; source: string }> }>(
      await b.client.callTool({ name: "read_stream", arguments: { stream: streamName } }),
    );
    const ping = read.events.find((e) => e.type === "smoke.ping");
    if (!ping) throw new Error("B could not read A's event back");
    if (ping.correlationId !== cid) throw new Error(`correlationId mismatch: ${ping.correlationId} != ${cid}`);
    if (ping.source !== "smoke-agent-a") throw new Error(`source mismatch: ${ping.source}`);
    log("      ok: B was notified and replayed A's event (cid + source intact)");

    // ── Check 2: claim race — exactly one winner ────────────────────────────
    log("[2/7] task claim race");
    const created = toolJson<{ id: string }>(
      await a.client.callTool({
        name: "create_task",
        arguments: { title: `smoke task ${run}`, correlationId: cid },
      }),
    );

    const [claimA, claimB] = await Promise.all([
      a.client.callTool({ name: "claim_task", arguments: { taskId: created.id } }),
      b.client.callTool({ name: "claim_task", arguments: { taskId: created.id } }),
    ]);
    const outcomes = [toolJson<{ claimed: boolean }>(claimA), toolJson<{ claimed: boolean }>(claimB)];
    const winners = outcomes.filter((o) => o.claimed);
    if (winners.length !== 1) throw new Error(`expected exactly 1 claim winner, got ${winners.length}`);

    const winner = outcomes[0]!.claimed ? a : b;
    await winner.client.callTool({
      name: "complete_task",
      arguments: { taskId: created.id, result: { smoke: true } },
    });

    await waitFor(() => b.notifications.includes("tasks://queue"), "B notified on tasks://queue");

    const lifecycle = toolJson<{ events: Array<{ type: string; payload: { taskId?: string } }> }>(
      await b.client.callTool({ name: "read_stream", arguments: { stream: "tasks", limit: 200 } }),
    );
    const types = lifecycle.events.filter((e) => e.payload.taskId === created.id).map((e) => e.type);
    for (const expected of ["task.created", "task.claimed", "task.completed"]) {
      if (!types.includes(expected)) throw new Error(`missing ${expected} on stream://tasks (saw: ${types.join(", ")})`);
    }
    log("      ok: exactly one winner; full lifecycle visible on stream://tasks");

    // ── Check 3: HTTP ingest → subscribed agent notified ────────────────────
    log("[3/7] HTTP ingest");
    const beforeIngest = b.notifications.filter((u) => u === streamUri).length;
    const res = await fetch(`${target.baseUrl}/ingest/${streamName}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers(target, "ci") },
      body: JSON.stringify({ type: "ci.build.passed", payload: { run } }),
    });
    if (res.status !== 201) throw new Error(`ingest returned ${res.status}: ${await res.text()}`);

    await waitFor(
      () => b.notifications.filter((u) => u === streamUri).length > beforeIngest,
      "B notified after ingest",
    );
    const afterIngest = toolJson<{ events: Array<{ type: string; source: string }> }>(
      await b.client.callTool({ name: "read_stream", arguments: { stream: streamName } }),
    );
    const ingested = afterIngest.events.find((e) => e.type === "ci.build.passed");
    if (!ingested) throw new Error("ingested event not readable");
    if (!ingested.source.startsWith("ingest:")) throw new Error(`ingest source mis-stamped: ${ingested.source}`);
    log("      ok: external event ingested, fanned out, source stamped");

    // ── Check 4: presence — agents://online roster ──────────────────────────
    log("[4/7] presence roster");
    const roster = await b.client.readResource({ uri: "agents://online" });
    const rosterJson = JSON.parse(
      (roster.contents as Array<{ text?: string }>)[0]!.text ?? "{}",
    ) as { agents: Array<{ agent: string }> };
    for (const name of ["smoke-agent-a", "smoke-agent-b"]) {
      if (!rosterJson.agents.some((x) => x.agent === name)) {
        throw new Error(`agents://online missing ${name}`);
      }
    }
    log("      ok: both agents visible on agents://online");

    // ── Check 5: durable cursors ────────────────────────────────────────────
    log("[5/7] durable cursors");
    const cursorStream = `${streamName}-cur`;
    for (let i = 0; i < 3; i++) {
      await a.client.callTool({
        name: "publish_event",
        arguments: { stream: cursorStream, type: "seq.item", payload: { i } },
      });
    }
    const firstRead = toolJson<{ events: unknown[]; committed: string | null }>(
      await b.client.callTool({
        name: "read_stream",
        arguments: { stream: cursorStream, cursor: "proc", commit: true },
      }),
    );
    if (firstRead.events.length !== 3) throw new Error(`cursor first read: expected 3, got ${firstRead.events.length}`);
    if (!firstRead.committed) throw new Error("cursor did not commit");

    for (let i = 3; i < 5; i++) {
      await a.client.callTool({
        name: "publish_event",
        arguments: { stream: cursorStream, type: "seq.item", payload: { i } },
      });
    }
    const secondRead = toolJson<{ events: Array<{ payload: { i: number } }> }>(
      await b.client.callTool({
        name: "read_stream",
        arguments: { stream: cursorStream, cursor: "proc", commit: true },
      }),
    );
    if (secondRead.events.length !== 2) {
      throw new Error(`cursor resume: expected exactly the 2 new events, got ${secondRead.events.length}`);
    }
    if (secondRead.events[0]!.payload.i !== 3) throw new Error("cursor resumed at the wrong position");
    log("      ok: cursor committed, resumed exactly at the right offset");

    // ── Check 6: stateless REST catch-up (works on any replica, no session) ─
    log("[6/7] REST catch-up read");
    const restStream = `${streamName}-rest`;
    const restHeaders = headers(target, "smoke-rest");
    const pub = await fetch(`${target.baseUrl}/ingest/${restStream}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...restHeaders },
      body: JSON.stringify({ type: "rest.ping", payload: { run } }),
    });
    if (pub.status !== 201) throw new Error(`REST seed ingest returned ${pub.status}: ${await pub.text()}`);

    const restUrl = `${target.baseUrl}/streams/${restStream}?cursor=rest&commit=true`;
    const first = await fetch(restUrl, { headers: restHeaders });
    if (first.status !== 200) throw new Error(`REST read returned ${first.status}: ${await first.text()}`);
    const firstJson = (await first.json()) as { events: unknown[]; committed: string | null };
    if (firstJson.events.length !== 1) throw new Error(`REST read: expected 1 event, got ${firstJson.events.length}`);
    if (!firstJson.committed) throw new Error("REST read did not commit the cursor");

    const second = await fetch(restUrl, { headers: restHeaders });
    const secondJson = (await second.json()) as { events: unknown[] };
    if (secondJson.events.length !== 0) {
      throw new Error(`REST cursor did not advance: second read got ${secondJson.events.length} events`);
    }
    log("      ok: REST read + durable cursor advanced statelessly");

    // ── Check 7: wake registration lifecycle (agent-scoped, no admin) ───────
    log("[7/7] wake registration lifecycle");
    const wake = toolJson<{ id: string; kind: string; owner: string }>(
      await a.client.callTool({
        name: "register_wake",
        arguments: { stream: `${streamName}-wake`, url: "http://127.0.0.1:9/wake", debounceSec: 60 },
      }),
    );
    if (wake.kind !== "wake") throw new Error(`register_wake kind: ${wake.kind}`);
    const wakes = toolJson<{ wakes: Array<{ id: string }> }>(await a.client.callTool({ name: "list_wakes", arguments: {} }));
    if (!wakes.wakes.some((w) => w.id === wake.id)) throw new Error("list_wakes missing new registration");
    const gone = toolJson<{ removed?: string }>(
      await a.client.callTool({ name: "remove_wake", arguments: { id: wake.id } }),
    );
    if (gone.removed !== wake.id) throw new Error("remove_wake failed");
    const after = toolJson<{ wakes: Array<{ id: string }> }>(await a.client.callTool({ name: "list_wakes", arguments: {} }));
    if (after.wakes.some((w) => w.id === wake.id)) throw new Error("wake survived removal");
    log("      ok: register → list → remove, owner-scoped, no admin token needed");

    log("smoke: ALL CHECKS PASSED");
  } finally {
    await Promise.allSettled([a.close(), b.close()]);
  }
}
