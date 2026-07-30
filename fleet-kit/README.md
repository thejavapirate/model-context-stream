# fleet-kit — agents that notice

The delivery half of "auto-consumption" shipped with the server: `resources/updated`
pushes, `blockMs` long-polls, durable cursors. This kit ships the **attention** half
for Claude Code: a `UserPromptSubmit` hook that injects a compact digest of new fleet
events into the model's context at every turn boundary. Your agent doesn't check the
stream — it simply already knows.

```
[context-stream] 2 new events on stream://team:
  • ci.build.failed {"repo":"api","sha":"a3f9c12"} (ingest:ci 14:03:22Z)
  • task.created {"title":"Investigate failing auth test"} (ops 14:04:01Z)
```

## Install (Claude Code)

1. One-time per agent identity — skip retained history so the first prompt isn't flooded:

```sh
MCS_URL=http://localhost:3000 MCS_TOKEN=tok_agent MCS_AGENT_NAME=<your-name> \
  node fleet-kit/mcs-catchup.mjs --init
```

2. Add the hook to `.claude/settings.json` (project) or `.claude/settings.local.json` (personal):

```json
{
  "hooks": {
    "UserPromptSubmit": [
      {
        "hooks": [
          {
            "type": "command",
            "command": "MCS_URL=http://localhost:3000 MCS_TOKEN=tok_agent MCS_AGENT_NAME=<your-name> MCS_FOLLOW=team node fleet-kit/mcs-catchup.mjs"
          }
        ]
      }
    ]
  }
}
```

That's the whole install. From the next prompt on, new events on followed streams
appear as context.

## Configuration (env vars on the hook command)

| Var | Default | Meaning |
|---|---|---|
| `MCS_URL` | `http://localhost:3000` | The server |
| `MCS_TOKEN` | *(none)* | Bearer token |
| `MCS_AGENT_NAME` | *(none)* | Identity that owns the cursor — required unless the token is agent-bound |
| `MCS_FOLLOW` | `team` | Comma-separated streams to follow |
| `MCS_HOOK_CURSOR` | `hook` | Named durable cursor used by the hook |
| `MCS_HOOK_MAX_EVENTS` | `8` | Max events injected per stream per prompt (backlog is paced, not dumped) |

## Design contract

- **Zero noise**: empty stdout when nothing happened — quiet streams cost nothing.
- **Never breaks a prompt**: any failure (server down, bad token, timeout) exits 0 with
  no output. Debug by running the script by hand.
- **At-least-once, durably positioned**: the cursor lives server-side per agent
  identity (`GET /streams/:stream?cursor=hook&commit=true`), advancing only past
  delivered events. Reconnects, restarts, and multi-day gaps resume exactly.
- **Paced**: at most `MCS_HOOK_MAX_EVENTS` per stream per prompt, with a "more
  waiting" pointer so the agent can pull the rest via `read_stream` when it matters.

## Non-Claude harnesses: the sidecar pattern (documented, not built)

Any harness with a context-injection point can get the same behavior with a sidecar
that long-polls and writes where the harness looks:

```sh
while true; do
  curl -s "$MCS_URL/streams/team?cursor=sidecar&commit=true&blockMs=25000" \
    -H "Authorization: Bearer $MCS_TOKEN" -H "X-Agent-Name: $AGENT" \
    | jq -r '.events[] | "[\(.type)] \(.payload|tostring)"' >> fleet-events.log
done
```

Point the harness's file-watch/injection mechanism at `fleet-events.log`. Per-harness
implementations will land here as demand appears.

## wake-runner — waking agents that aren't running

The hook covers *running* agents. `wake-runner/runner.mjs` covers *sleeping* ones: an
agent registers a wake for itself (`register_wake {stream, url, secret, debounceSec}` —
agent-scoped, max 5, no admin needed), and when a matching event lands, the server POSTs
a signed wake envelope to the runner, which spawns a headless session that catches up
from its durable cursor and acts.

```sh
WAKE_SECRET=s3cret WAKE_CWD=~/oncall-workspace node fleet-kit/wake-runner/runner.mjs
# then, as the agent that wants waking:
#   register_wake {stream: "oncall", url: "http://runner-host:8377/", secret: "s3cret"}
```

> **The wake URL must be reachable FROM THE SERVER.** If the server runs in Docker and the
> runner on the host, `localhost` points at the container — use the host's address instead
> (`host.docker.internal` on Docker Desktop for Mac/Windows; on WSL2 or Linux, the host's
> LAN/WSL IP, e.g. `hostname -I`). URLs must be plain http(s) with no embedded credentials;
> delivery never follows redirects.

**Every wake is a paid session.** The runner's guards are deliberately conservative and
all tunable by env var:

| Guard | Env | Default |
|---|---|---|
| Kill switch | `WAKE_DISABLED=1` | off |
| HMAC verification | `WAKE_SECRET` | unset = dev-only warning |
| Rate cap (sliding hour) | `WAKE_MAX_PER_HOUR` | 6 |
| One session per owner | — | always on (busy wakes drop; the cursor catches up) |
| Session timeout | `WAKE_TIMEOUT_SEC` | 600 |

The spawned command defaults to `claude -p "<woken prompt>"`; override with
`WAKE_CMD` (a shell command receiving `$WAKE_PROMPT`, `$WAKE_STREAM`, `$WAKE_TYPE`,
`$WAKE_EVENT_ID`, `$WAKE_OWNER`), e.g. `WAKE_CMD='claude -p --model haiku "$WAKE_PROMPT"'`.
The server debounces at the source too (`debounceSec`, default 60): a burst of 50 events
is one wake. With `MCS_URL`+`MCS_TOKEN` set, the runner announces `agent.woke` /
`agent.slept` on `stream://agents`, so wake activity is itself followable fleet context.
`GET /` on the runner returns health + budget state. Helm/compose packaging: future work.

## mcs-standby — self-resuming idle sessions (tier 2.5)

The catch-up hook needs a prompt; `mcs-standby.mjs` removes even that for open-but-idle
terminals. Wired as a `Stop` hook with `asyncRewake`, it long-polls followed streams after
the agent goes idle and re-wakes the session with a digest when a teammate's event arrives
(exit 2 → rewake). Loop-safe: events from the agent's OWN identity never re-wake it, and
one bounded standby window runs per stop (`MCS_STANDBY_MAX_MS`, default 10 min) — after
that the session truly sleeps and tier 3 (wakes) takes over.

```json
"Stop": [{ "hooks": [{ "type": "command", "asyncRewake": true, "timeout": 630,
  "command": "MCS_URL=... MCS_TOKEN=... MCS_AGENT_NAME=me MCS_FOLLOW=team node fleet-kit/mcs-standby.mjs" }] }]
```
