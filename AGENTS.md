# AGENTS.md — working on model-context-stream

You are working on a **living, event-driven MCP server**: Redis-Streams-backed context streams,
a task queue with atomic claims, versioned protocols, MCP tool federation, outbound webhooks,
and agent presence — served over MCP Streamable HTTP so fleets of AI agents stay mutually
context-aware. Read `README.md` for the product story; this file is how to *work on it*.

## Setup from a blank clone

```sh
npm install                      # Node >= 20
docker compose up -d redis       # local Redis for `npm run dev`
npm run dev                      # tsx watch, http://localhost:3000
```

Docker must be running for tests (unit tests boot real Redis via testcontainers — mocks get
XADD/XREAD/Lua semantics wrong and are deliberately not used).

## Commands

| Command | What it does |
|---|---|
| `npm run typecheck` | `tsc --noEmit` — run after every change |
| `npm test` | Unit suites (`test/unit/*`) — real Redis per suite via testcontainers |
| `npm run test:e2e` | In-process end-to-end: 5-check smoke + federation vs a fake upstream |
| `npm run smoke` | Smoke an already-running server (`MCS_URL`, `MCS_TOKEN` env) |
| `docker compose up -d --build` | Full stack (server + Redis, AOF persistence) |
| `helm lint deploy/helm/model-context-stream` | Chart validation |

**Definition of done for any nontrivial change:** typecheck → `npm test` → `npm run test:e2e` →
`docker compose up -d --build && MCS_URL=http://localhost:3000 MCS_TOKEN=<token> npm run smoke`.
The smoke script is the executable definition of "the product works".

## Architecture map

| Path | Responsibility |
|---|---|
| `src/redis/keys.ts` | **Every** Redis key + reserved system stream names. Never inline a key string elsewhere. |
| `src/core/events.ts` | The `StreamEvent` envelope. Entry ID = replay cursor; `source` is server-stamped. |
| `src/core/streams.ts` | Publish/read/registry/trim. Approximate `XTRIM MAXLEN ~` on publish; exact `trimThrough` for digests. |
| `src/core/tasks.ts` | Task state machine. Claims/mutations/reaping are **Lua scripts** — atomicity lives there, not in JS. |
| `src/core/fanout.ts` | ONE blocking XREAD loop per process → in-memory listeners. `subscribe()` is async: it snapshots the cursor before resolving (this fixed a real missed-event race — keep it). The `__wake__` control stream interrupts parked reads. |
| `src/core/{cursors,webhooks,digests,protocols}.ts` | Durable per-agent cursors; outbound HTTP with HMAC + auto-disable; agent-driven compaction; versioned playbooks. |
| `src/mcp/server.ts` | The entire MCP surface — every tool/resource/prompt, one `McpServer` **per session**. The API contract lives here. |
| `src/mcp/federation.ts` | Upstream MCP clients + namespaced `{upstream}__{tool}` proxies on every session. |
| `src/mcp/sessions.ts` | Session registry, URI→fanout subscription mapping, presence events. |
| `src/mcp/notifier.ts` | Debounce/coalesce for `resources/updated` (200ms/1s) and `list_changed` (500ms). |
| `src/http/app.ts` | Streamable HTTP session lifecycle, auth, `/ingest/:stream`, `/healthz`, `/metrics`. |
| `test/e2e/fake-upstream.ts` | Minimal upstream MCP server for federation tests — reuse it. |

## Hard rules

- **`@modelcontextprotocol/sdk` stays pinned `^1`.** The repo's v2 beta is a different package
  layout. All SDK imports are confined to `src/mcp/` and the http/session layer — keep it that way.
- **ESM with NodeNext:** relative imports need the `.js` suffix (`from "./streams.js"`), even in
  TypeScript. One-off scripts outside the project need `.mts` extension for top-level await.
- **zod stays `^3.25`** — `src/mcp/federation.ts` uses the `zod/v4` subpath for the schema
  passthrough shim. Bumping or downgrading breaks federation's tool-schema fidelity.
- **New Redis keys go in `redis/keys.ts`; new reserved streams in `SYSTEM_STREAMS`.**
- **Anything mutating multi-key task state must be a Lua script** (exactly-one-winner claims are
  the product's core guarantee — `test/unit/tasks.test.ts` has the 25-way race test that proves it).
- **Never destroy stream history without verification** — see how `DigestScheduler.onTaskSettled`
  checks the digest event exists before `trimThrough`.
- Admin-gated tools follow the `requireAdmin()` pattern in `src/mcp/server.ts`; token roles are
  parsed in `src/config.ts` (`token[:agentName[:admin]]`).
- MCP sessions are **in-process state** — anything you build must assume single replica or
  sticky sessions (documented in the Helm chart).

## Testing yourself against the live server

`.mcp.json` in the repo root auto-configures Claude Code sessions started in this directory:

```sh
docker compose up -d    # MCS_TOKENS from .env, or the tok_agent/tok_ops dev defaults
MCS_AGENT_NAME=<your-name> claude    # you are now an agent on the fleet
```

Useful first calls: `whoami`, `list_streams`, `read_stream {stream: "team"}`, `list_tasks`,
resource `agents://online`. If you claim a task, heartbeat with `update_task_progress` or your
lease expires (~5 min) and the task returns to the queue.

## Gotchas that have bitten before

- `Fanout.subscribe` must be awaited — a fire-and-forget subscribe can miss an immediate publish.
- SDK resource templates match greedily across `?` and `/`; registration ORDER in
  `src/mcp/server.ts` matters (`{?from}` template before the plain one). Test with
  `node_modules/.../shared/uriTemplate.js` before changing URI schemes.
- `XTRIM MAXLEN ~` trims in ~100-entry macro-nodes — don't write tests expecting exact lengths.
- The e2e boot in `test/e2e/*.test.ts` must wire ALL `Deps` (config, streams, tasks, protocols,
  cursors, webhooks, federation, registry, listChanged, toolsChanged) — TypeScript will tell you.
- Blocking Redis reads need their own connection (`redis/client.ts` gives you `main` vs
  `blocking`; `read_stream blockMs` uses throwaway connections).
