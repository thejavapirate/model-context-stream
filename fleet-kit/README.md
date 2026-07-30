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

## Deliberately not shipped

- **A `Stop`-hook variant** ("new context arrived mid-turn, review before ending") —
  an agent whose review publishes events can loop itself. Revisit with a once-per-turn
  guard if real usage asks for it.
- **Waking idle agents** (tier 3): that's `register_wake` + a runner service — on the
  roadmap, not in this kit. The hook covers running agents; wakes cover sleeping ones.
