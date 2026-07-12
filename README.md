# model-context-stream

A **living, event-driven MCP server**. Agents connect over the Model Context Protocol and follow
**model context streams** — append-only event logs. When any agent (or an external system) publishes
an event, every subscribed agent gets an MCP `resources/updated` notification and pulls the new
context. Add a shared **task queue** (atomic claims, leases) and versioned **protocols** (playbooks),
and a fleet of agents stays mutually context-aware in real time.

📖 **The story and design rationale:** [A Living, Breathing MCP Server](https://medium.com/@thejavapirate/a-living-breathing-mcp-server-fda569a64edc)

```
  agent A ──publish──▶ ┌───────────────────────┐ ──notify──▶ agent B
  agent C ◀──notify──  │  model-context-stream │ ◀──claim── agent D
  CI/webhooks ─ingest─▶│  (MCP + Redis Streams)│
                       └───────────────────────┘
```

## Why

- **Coordination without collisions** — agents announce work on streams; the task queue guarantees
  exactly-one-claimant via atomic Redis Lua claims with crash-safe leases.
- **Shared situational awareness** — a monitoring webhook publishes once; every following agent knows.
- **Replayable context** — streams are append-only logs: a fresh agent replays recent events and is
  caught up (event sourcing for agent context).
- **Living SOPs** — update a protocol once; every subscribed agent is notified and follows the new version.

## Quick start

**No clone needed** — prebuilt multi-arch images ship on GHCR:

```sh
mkdir mcs && cd mcs
curl -sO https://raw.githubusercontent.com/thejavapirate/model-context-stream/main/docker-compose.yml
MCS_TOKENS="tok_ops:ops:admin,tok_agent:fleet" docker compose up -d --no-build
curl -s localhost:3000/healthz
```

Or on Kubernetes, straight from the OCI registry:

```sh
helm install mcs oci://ghcr.io/thejavapirate/charts/model-context-stream \
  --set auth.tokens="tok_ops:ops:admin,tok_agent:fleet"
```

From a clone (builds locally):

```sh
cp .env.example .env          # set MCS_TOKENS
docker compose up -d --build
curl -s localhost:3000/healthz
```

Connect any MCP client to `http://localhost:3000/mcp` (Streamable HTTP) with
`Authorization: Bearer <token>`. Try it interactively:

```sh
npx @modelcontextprotocol/inspector
```

### Claude Code

```sh
claude mcp add --transport http context-stream http://localhost:3000/mcp \
  --header "Authorization: Bearer tok_local_dev" --header "X-Agent-Name: my-agent"
```

## The API surface

**Resources** (subscribe for live updates):

| URI | Content |
|---|---|
| `stream://{name}` | Last 50 events on a stream |
| `stream://{name}?from={id}` | Replay after cursor `{id}` |
| `tasks://queue` | Task board: counts + pending/active cards |
| `task://{id}` | One task record |
| `protocol://{name}` / `protocol://{name}/v{n}` | Latest / pinned playbook (markdown) |

| `agents://online` | Live presence roster: connected agents + their claimed tasks |

**Tools:** `publish_event`, `read_stream` (pull fallback: `blockMs` long-poll, `cursor`/`commit`
durable resume), `commit_cursor`, `list_cursors`, `list_streams` · `create_task`, `claim_task`,
`update_task_progress` (doubles as lease heartbeat), `complete_task`, `fail_task`, `release_task`,
`list_tasks` · `list_protocols`, `get_protocol`, `put_protocol` · `list_upstreams` · `whoami`

**Admin tools** (require an `:admin` token): `configure_stream` (retention + digest policy),
`add_webhook` / `remove_webhook` / `list_webhooks`, `add_upstream` / `remove_upstream`

**Prompts:** `follow_protocol`, `catch_up`

### Tool federation (senses in)

Connect the server to upstream MCP servers once; every agent gets their tools, namespaced
`{upstream}__{tool}`, with live `tools/list_changed` when the upstream set changes:

```
add_upstream {name: "github", url: "https://api.githubcopilot.com/mcp/", token: "..."}
→ every agent now has github__create_pull_request, github__search_issues, …
```

Upstream outages degrade gracefully (calls return errors, background reconnect with backoff);
self-federation is refused.

### Outbound webhooks (senses out)

The mirror of ingest — stream events POSTed to external URLs, HMAC-signed (`X-MCS-Signature`),
type-filterable, with retries and auto-disable after sustained failure (announced on
`stream://system`). Admin-managed; note the SSRF implication: only admins can point the server
at URLs.

### Agent-driven compaction (memory hygiene)

Set `configure_stream {stream, digestThreshold: N}` and when the stream grows past N, the server
creates a **digest task on its own queue**. Any connected agent claims it, follows the seeded
`stream-digest` protocol (summarize the old range into one `stream.digest` event), and the server
verifies + trims. The fleet maintains its own memory — no LLM key in the server.

**HTTP ingest** for non-MCP systems (CI, GitHub webhooks, monitoring):

```sh
curl -X POST localhost:3000/ingest/deployments \
  -H "Authorization: Bearer $TOKEN" -H "content-type: application/json" \
  -d '{"type": "ci.build.failed", "payload": {"repo": "api", "sha": "abc123"}}'
```

## How it works

- **Redis Streams** back every context stream (`XADD`/`XRANGE`, approximate `MAXLEN` trimming,
  AOF persistence). The entry ID is the replay cursor.
- **One blocking `XREAD` loop** per process fans events out to in-memory session subscriptions;
  a control stream interrupts the parked read so new subscriptions arm instantly.
- **Notifications are debounced** (200ms trailing edge, 1s max wait) — lossless, since
  `resources/updated` carries only a URI and clients re-read.
- **Tasks are a state machine** in Redis hashes with Lua-scripted atomic claims. Leases expire:
  a crashed agent's task returns to the queue within ~lease+10s. Every lifecycle change is also an
  event on `stream://tasks`, so who-is-doing-what is itself followable context.
- **Clients without subscription support** (it's an optional MCP capability) use `read_stream`
  with `blockMs` as a long-poll.

Identity: `X-Agent-Name` header → token-bound name → MCP `clientInfo` → anonymous. Stamped as
`source` on every event and `claimedBy` on claims — never client-supplied inside payloads.

## Development

Working on this repo with a coding agent? **`AGENTS.md`** has the full brief: commands,
architecture map, hard rules, and the gotchas that have bitten before. `.mcp.json` auto-connects
Claude Code sessions in this directory to a locally running stack.

```sh
npm install
npm run dev          # tsx watch (needs a local redis, e.g. docker compose up redis)
npm test             # unit tests (testcontainers — needs Docker)
npm run test:e2e     # in-process two-client smoke
npm run smoke        # smoke an already-running server: MCS_URL / MCS_TOKEN
npm run typecheck
```

## Configuration

| Env | Default | Meaning |
|---|---|---|
| `MCS_TOKENS` | *(empty = no auth, dev only — every session is admin)* | Comma-separated `token[:agentName[:admin]]` |
| `REDIS_URL` | `redis://localhost:6379` | Redis connection |
| `MCS_STREAM_MAXLEN` | `10000` | Per-stream retention (approximate) |
| `PORT` | `3000` | HTTP port (MCP + ingest + healthz + metrics) |

TLS is a deployment concern — put a reverse proxy in front for anything non-local.

## Operating it (Kubernetes / cloud)

A production Helm chart ships in `deploy/helm/model-context-stream`:

```sh
helm install mcs deploy/helm/model-context-stream \
  --set auth.tokens="tok_ops:ops:admin,tok_fleet:agents" \
  --set image.repository=ghcr.io/you/model-context-stream --set image.tag=0.2.0
```

- **Bundled Redis** (StatefulSet + PVC + AOF) by default; set `redis.enabled=false` +
  `externalRedisUrl` for managed Redis.
- **Prometheus metrics** at `GET /metrics`: `mcs_connected_sessions`, `mcs_events_published_total`,
  `mcs_tasks{status}`, `mcs_webhook_failed_deliveries_total`, `mcs_streams_compacted_total`,
  plus process defaults. Scrape annotations are one uncomment away in `values.yaml`.
- **Scaling posture:** MCP sessions live in server memory — ship `replicaCount: 1`, or enable the
  documented session-affinity blocks (Service `ClientIP` or nginx-ingress cookie affinity) before
  scaling out. `NOTES.txt` warns on risky configurations at install time.
- Hardened defaults: non-root, read-only rootfs, dropped capabilities, liveness/readiness on
  `/healthz` (which requires a Redis round-trip).
- Package/publish: `helm package deploy/helm/model-context-stream` → `helm push` to any OCI registry.
