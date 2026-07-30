#!/usr/bin/env node
/**
 * fleet-kit standby: self-resuming idle sessions for Claude Code.
 *
 * Wired as a Stop hook with asyncRewake, this runs AFTER the agent goes idle:
 * it long-polls followed streams (blockMs) and, when a NEW event from someone
 * else arrives, prints a digest and exits 2 — which re-wakes the model with
 * that digest as context. No human keystroke needed.
 *
 * Loop safety: events whose source is THIS agent are ignored (an agent can
 * never re-wake itself with its own publishes), and one standby window runs
 * per stop (MCS_STANDBY_MAX_MS, default 10 min) — after that the session
 * genuinely sleeps and tier 3 (register_wake + wake-runner) takes over.
 *
 * Cost control: waking N idle agents for one informational event costs N paid
 * sessions. MCS_STANDBY_TYPES is an allowlist of event types that may re-wake a
 * session — default is actionable events only (work appearing, review wanted,
 * a lease expiring). Chatter (agent.status, build.milestone, finding.*) is seen
 * at the next engagement via the catch-up hook instead. Set to "*" for all.
 *
 * Env: MCS_URL, MCS_TOKEN, MCS_AGENT_NAME (also the self-filter), MCS_FOLLOW,
 *      MCS_STANDBY_CURSOR ("standby"), MCS_STANDBY_MAX_MS (600000),
 *      MCS_HOOK_MAX_EVENTS (8), MCS_STANDBY_TYPES (see above).
 * Hook timeout must exceed MCS_STANDBY_MAX_MS (e.g. timeout: 630 for 10 min).
 */

const BASE = (process.env.MCS_URL ?? "http://localhost:3000").replace(/\/$/, "");
const TOKEN = process.env.MCS_TOKEN ?? "";
const AGENT = process.env.MCS_AGENT_NAME ?? "";
const FOLLOW = (process.env.MCS_FOLLOW ?? "team").split(",").map((s) => s.trim()).filter(Boolean);
const CURSOR = process.env.MCS_STANDBY_CURSOR ?? "standby";
const MAX_MS = Math.max(30_000, Number(process.env.MCS_STANDBY_MAX_MS ?? 600_000) || 600_000);
const MAX_EVENTS = Math.max(1, Number(process.env.MCS_HOOK_MAX_EVENTS ?? 8) || 8);
const TYPES_RAW = (process.env.MCS_STANDBY_TYPES ?? "task.created,task.expired,review.requested").trim();
const TYPES = TYPES_RAW === "*" ? null : new Set(TYPES_RAW.split(",").map((s) => s.trim()).filter(Boolean));
/** Only actionable events justify spending a session; chatter waits for the catch-up hook. */
const wakeworthy = (e) => e.source !== AGENT && (TYPES === null || TYPES.has(e.type));

const headers = {
  ...(TOKEN ? { authorization: `Bearer ${TOKEN}` } : {}),
  ...(AGENT ? { "x-agent-name": AGENT } : {}),
};

async function poll(stream, blockMs, limit = MAX_EVENTS) {
  const url = `${BASE}/streams/${encodeURIComponent(stream)}?cursor=${encodeURIComponent(CURSOR)}&commit=true&limit=${limit}&blockMs=${blockMs}`;
  const res = await fetch(url, { headers, signal: AbortSignal.timeout(blockMs + 5000) });
  if (!res.ok) throw new Error(`${res.status}`);
  return res.json();
}

function fmt(e) {
  const ts = typeof e.ts === "string" ? e.ts.slice(11, 19) : "";
  let payload = JSON.stringify(e.payload ?? {});
  if (payload.length > 110) payload = payload.slice(0, 107) + "…";
  return `  • ${e.type} ${payload} (${e.source}${ts ? " " + ts + "Z" : ""})`;
}

const deadline = Date.now() + MAX_MS;
try {
  // Standby means "watch for activity AFTER this idle began": drain the cursor
  // to each stream's tail SILENTLY first, so retained history / mid-turn
  // backlog never re-wakes the session with stale events. (Fleet finding
  // 2026-07-30, credit night-shift + lead + builder-3: a fresh cursor replayed
  // history and caused rapid stale re-wakes.) Mid-turn arrivals remain the
  // catch-up hook's job at the next engagement.
  for (const stream of FOLLOW) {
    for (let page = 0; page < 60; page++) {
      const r = await poll(stream, 0, 200);
      if (!Array.isArray(r.events) || r.events.length < 200) break;
    }
  }
  while (Date.now() < deadline) {
    // Long-poll the first stream; sweep the rest non-blocking each round.
    const results = [];
    for (let i = 0; i < FOLLOW.length; i++) {
      const blockMs = i === 0 ? Math.min(25_000, Math.max(1000, deadline - Date.now())) : 0;
      try {
        const r = await poll(FOLLOW[i], blockMs);
        const fresh = (r.events ?? []).filter(wakeworthy); // never self-wake; actionable only
        if (fresh.length > 0) results.push({ stream: FOLLOW[i], events: fresh });
      } catch {
        /* transient — keep standing by */
      }
    }
    if (results.length > 0) {
      const out = ["[context-stream] actionable fleet activity while you were idle — resume and act if it concerns you:"];
      for (const { stream, events } of results) {
        out.push(`stream://${stream}:`);
        for (const e of events) out.push(fmt(e));
      }
      console.log(out.join("\n"));
      process.exit(2); // asyncRewake: wake the model with the digest above
    }
  }
} catch {
  /* fall through to silent sleep */
}
process.exit(0); // standby window over — session sleeps for real (wakes are tier 3)
