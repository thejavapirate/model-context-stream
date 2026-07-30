#!/usr/bin/env node
/**
 * fleet-kit wake-runner: turns signed wake webhooks into headless agent sessions.
 *
 * Receives the server's wake envelope (see register_wake), verifies the HMAC,
 * applies hard budget guards — every wake is a PAID session — and spawns a
 * headless agent that catches up from its durable cursor and acts.
 *
 * Guards, in order:
 *   1. WAKE_DISABLED=1        kill switch → 503
 *   2. HMAC (WAKE_SECRET)     bad/missing signature → 401
 *   3. WAKE_MAX_PER_HOUR      sliding window (default 6) → 429
 *   4. one session per owner  wake while busy → 200 drop (the running session
 *                             sees the events through its cursor anyway)
 *   5. WAKE_TIMEOUT_SEC       runaway session killed (default 600)
 *
 * Env: WAKE_PORT (8377), WAKE_SECRET, WAKE_CWD (workspace for the session),
 *      WAKE_CMD (optional shell command; gets WAKE_PROMPT/WAKE_STREAM/WAKE_TYPE/
 *      WAKE_EVENT_ID/WAKE_OWNER env vars — e.g. 'claude -p --model haiku "$WAKE_PROMPT"';
 *      default: spawn `claude -p <prompt>` directly, no shell),
 *      MCS_URL + MCS_TOKEN (optional: announce agent.woke/agent.slept on stream://agents).
 */

import { createHmac, timingSafeEqual } from "node:crypto";
import { createServer } from "node:http";
import { spawn } from "node:child_process";

const PORT = Number(process.env.WAKE_PORT ?? 8377);
const SECRET = process.env.WAKE_SECRET ?? "";
const CWD = process.env.WAKE_CWD ?? process.cwd();
const MAX_PER_HOUR = Math.max(1, Number(process.env.WAKE_MAX_PER_HOUR ?? 6) || 6);
const TIMEOUT_SEC = Math.max(30, Number(process.env.WAKE_TIMEOUT_SEC ?? 600) || 600);
const CMD_TEMPLATE = process.env.WAKE_CMD ?? "";
const MCS_URL = (process.env.MCS_URL ?? "").replace(/\/$/, "");
const MCS_TOKEN = process.env.MCS_TOKEN ?? "";

const firedAt = [];            // epoch ms of recent spawns (sliding-window cap)
const liveByOwner = new Map(); // owner -> { child, since }

const log = (...args) => console.log(new Date().toISOString(), "[wake-runner]", ...args);

function verifySignature(body, sig) {
  if (!SECRET) return true; // explicitly unsecured — local dev only
  if (typeof sig !== "string") return false;
  const expected = `sha256=${createHmac("sha256", SECRET).update(body).digest("hex")}`;
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  return a.length === b.length && timingSafeEqual(a, b);
}

/** Best-effort observability: wake activity becomes followable fleet context. */
function announce(type, payload) {
  if (!MCS_URL || !MCS_TOKEN) return;
  fetch(`${MCS_URL}/ingest/agents`, {
    method: "POST",
    headers: { authorization: `Bearer ${MCS_TOKEN}`, "content-type": "application/json" },
    body: JSON.stringify({ type, payload }),
    signal: AbortSignal.timeout(2000),
  }).catch(() => {});
}

function buildPrompt(envelope) {
  const { wake, event } = envelope;
  return (
    `You were woken because stream "${wake.stream}" received event ${event.type} (id ${event.id}). ` +
    `Catch up first: read_stream {stream: "${wake.stream}", cursor: "wake", commit: true}. ` +
    `Act on what you find — if the events point at queued work, claim it with claim_task and complete it. ` +
    `Then finish cleanly.`
  );
}

function wake(envelope) {
  const owner = envelope.wake.owner ?? "default";
  const prompt = buildPrompt(envelope);
  const env = {
    ...process.env,
    WAKE_PROMPT: prompt,
    WAKE_STREAM: envelope.wake.stream,
    WAKE_TYPE: envelope.event.type,
    WAKE_EVENT_ID: envelope.event.id,
    WAKE_OWNER: owner,
  };
  const child = CMD_TEMPLATE
    ? spawn(CMD_TEMPLATE, { shell: true, cwd: CWD, env, stdio: ["ignore", "inherit", "inherit"] })
    : spawn("claude", ["-p", prompt], { cwd: CWD, env, stdio: ["ignore", "inherit", "inherit"] });

  liveByOwner.set(owner, { child, since: Date.now() });
  firedAt.push(Date.now());
  log(`WOKE ${owner} for ${envelope.event.type} on ${envelope.wake.stream} (pid ${child.pid})`);
  announce("agent.woke", { agent: owner, stream: envelope.wake.stream, eventId: envelope.event.id });

  const killer = setTimeout(() => {
    log(`TIMEOUT ${owner} after ${TIMEOUT_SEC}s — killing pid ${child.pid}`);
    child.kill("SIGKILL");
  }, TIMEOUT_SEC * 1000);
  killer.unref();

  child.on("exit", (code, signal) => {
    clearTimeout(killer);
    const entry = liveByOwner.get(owner);
    liveByOwner.delete(owner);
    const secs = entry ? Math.round((Date.now() - entry.since) / 1000) : 0;
    log(`SLEPT ${owner} (${signal ? `signal ${signal}` : `exit ${code}`} after ${secs}s)`);
    announce("agent.slept", { agent: owner, ...(signal ? { signal } : { exitCode: code }) });
  });
}

const server = createServer((req, res) => {
  const respond = (status, obj) => {
    res.statusCode = status;
    res.setHeader("content-type", "application/json");
    res.end(JSON.stringify(obj));
  };

  if (req.method === "GET") {
    const hourAgo = Date.now() - 3_600_000;
    return respond(200, {
      ok: process.env.WAKE_DISABLED !== "1",
      live: [...liveByOwner.keys()],
      wokenLastHour: firedAt.filter((t) => t > hourAgo).length,
      maxPerHour: MAX_PER_HOUR,
    });
  }
  if (req.method !== "POST") return respond(405, { error: "POST wakes here" });

  let body = "";
  req.on("data", (c) => {
    body += c;
    if (body.length > 1_048_576) req.destroy();
  });
  req.on("end", () => {
    if (process.env.WAKE_DISABLED === "1") return respond(503, { error: "wake runner disabled (kill switch)" });
    if (!verifySignature(body, req.headers["x-mcs-signature"])) {
      log("REJECTED: bad signature");
      return respond(401, { error: "bad signature" });
    }
    let envelope;
    try {
      envelope = JSON.parse(body);
    } catch {
      return respond(400, { error: "invalid JSON" });
    }
    if (!envelope?.wake?.stream || !envelope?.event?.id) return respond(400, { error: "not a wake envelope" });

    const hourAgo = Date.now() - 3_600_000;
    while (firedAt.length > 0 && firedAt[0] < hourAgo) firedAt.shift();
    if (firedAt.length >= MAX_PER_HOUR) {
      log(`RATE-CAPPED: ${firedAt.length} wakes in the last hour (max ${MAX_PER_HOUR})`);
      return respond(429, { error: `rate cap: ${MAX_PER_HOUR}/hour` });
    }

    const owner = envelope.wake.owner ?? "default";
    if (liveByOwner.has(owner)) {
      log(`DROPPED wake for ${owner}: session already live (it will catch up via its cursor)`);
      return respond(200, { dropped: "busy", owner });
    }

    wake(envelope);
    respond(202, { woken: owner });
  });
});

server.listen(PORT, () => {
  log(`listening on :${PORT} | cwd=${CWD} | cap=${MAX_PER_HOUR}/h | timeout=${TIMEOUT_SEC}s | hmac=${SECRET ? "on" : "OFF (dev)"}`);
  if (!SECRET) log("WARNING: WAKE_SECRET unset — any POST can start a paid session. Set it outside local dev.");
});
