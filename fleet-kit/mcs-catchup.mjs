#!/usr/bin/env node
/**
 * fleet-kit: turn-boundary catch-up for Claude Code.
 *
 * Wired as a UserPromptSubmit hook, this prints a compact digest of fleet
 * events that arrived since this agent's last prompt — stdout is injected into
 * the model's context, so the agent simply *knows* what the fleet did, without
 * being asked to check. Prints nothing when nothing happened (zero noise), and
 * stays silent on any error (a broken server must never break your prompt).
 *
 * Positioning is durable and server-side: a named cursor per agent identity,
 * advanced only past events actually delivered (at-least-once, paced at
 * MCS_HOOK_MAX_EVENTS per prompt).
 *
 * One-time setup per agent identity (skips retained history):
 *   MCS_TOKEN=... MCS_AGENT_NAME=<you> node fleet-kit/mcs-catchup.mjs --init
 */

const BASE = (process.env.MCS_URL ?? "http://localhost:3000").replace(/\/$/, "");
const TOKEN = process.env.MCS_TOKEN ?? "";
const AGENT = process.env.MCS_AGENT_NAME ?? "";
const FOLLOW = (process.env.MCS_FOLLOW ?? "team")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);
const CURSOR = process.env.MCS_HOOK_CURSOR ?? "hook";
const MAX = Math.max(1, Number(process.env.MCS_HOOK_MAX_EVENTS ?? 8) || 8);
const INIT = process.argv.includes("--init");

const headers = {
  ...(TOKEN ? { authorization: `Bearer ${TOKEN}` } : {}),
  ...(AGENT ? { "x-agent-name": AGENT } : {}),
};

async function read(stream, limit) {
  const url = `${BASE}/streams/${encodeURIComponent(stream)}?cursor=${encodeURIComponent(CURSOR)}&commit=true&limit=${limit}`;
  const res = await fetch(url, { headers, signal: AbortSignal.timeout(2500) });
  if (!res.ok) throw new Error(`GET ${stream}: ${res.status} ${await res.text().catch(() => "")}`);
  return res.json();
}

function fmt(e) {
  const ts = typeof e.ts === "string" ? e.ts.slice(11, 19) : "";
  let payload = JSON.stringify(e.payload ?? {});
  if (payload === "{}") payload = "";
  if (payload.length > 110) payload = payload.slice(0, 107) + "…";
  return `  • ${e.type}${payload ? " " + payload : ""} (${e.source}${ts ? " " + ts + "Z" : ""})`;
}

if (INIT) {
  // Fast-forward the cursor to each stream's tail so the first hooked prompt
  // isn't flooded with retained history.
  try {
    for (const stream of FOLLOW) {
      let skipped = 0;
      for (let page = 0; page < 60; page++) {
        const r = await read(stream, 200);
        skipped += r.events.length;
        if (r.events.length < 200) break;
      }
      console.log(`[context-stream] cursor "${CURSOR}" initialized at tail of stream://${stream} (${skipped} historical events skipped)`);
    }
  } catch (err) {
    console.error(`[context-stream] init failed: ${err.message}`);
    console.error(`  Check MCS_URL/MCS_TOKEN, and note cursors need an identity (MCS_AGENT_NAME or an agent-bound token).`);
    process.exit(1);
  }
  process.exit(0);
}

// Hook mode: parallel reads, silent on any failure, ALWAYS exit 0 —
// a non-zero exit from a UserPromptSubmit hook can block the user's prompt.
const results = await Promise.allSettled(FOLLOW.map(async (s) => ({ stream: s, r: await read(s, MAX) })));
const out = [];
for (const res of results) {
  if (res.status !== "fulfilled") continue;
  const { stream, r } = res.value;
  if (!Array.isArray(r.events) || r.events.length === 0) continue;
  out.push(`[context-stream] ${r.events.length} new event${r.events.length === 1 ? "" : "s"} on stream://${stream}:`);
  for (const e of r.events) out.push(fmt(e));
  if (r.events.length >= MAX) {
    out.push(`  (more waiting — read_stream {stream: "${stream}", cursor: "${CURSOR}", commit: true})`);
  }
}
if (out.length > 0) console.log(out.join("\n"));
process.exit(0);
