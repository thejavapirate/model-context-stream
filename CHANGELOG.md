# Changelog

All notable changes to this project are documented here. Format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); this project uses
[semantic versioning](https://semver.org/) (0.x: minor bumps may break).

Releases are cut by tagging (`git tag vX.Y.Z && git push origin vX.Y.Z`), which
publishes the multi-arch image and Helm chart to GHCR — see `AGENTS.md`.

## [Unreleased]

### Fixed

- **fleet-kit standby**: only *actionable* event types re-wake an idle session
  (`MCS_STANDBY_TYPES`, default `task.created,task.expired,review.requested`).
  Previously one informational broadcast re-woke every idle agent, costing N paid
  sessions to say "noted"; chatter is now picked up by the catch-up hook instead.
  `*` opts back into everything.
- **fleet-kit standby**: drain the cursor to each stream's tail before watching, so
  retained history no longer re-wakes sessions with stale events. Found in production
  by the agent fleet itself (night-shift, lead, builder-3, independently, within 33s).

## [0.6.0] — 2026-07-30

### Added

- **`fleet-kit/mcs-standby.mjs`** — the third attention tier: a `Stop`/`asyncRewake`
  hook that long-polls followed streams after an agent goes idle and self-resumes the
  session when a teammate publishes. Self-source filtered (no wake loops) with a
  bounded standby window that hands off to tier-3 wakes.

### Changed

- README, site, and `llms.txt` present the full three-tier attention model
  (engaged → idle-open → not-running).

## [0.5.0] — 2026-07-30

### Added

- **Wakes**: `register_wake` / `list_wakes` / `remove_wake` — agent-scoped, no admin
  token, quota 5 per agent, audited on `stream://system`. Wake deliveries carry a
  signed envelope (triggering event + `cursorAnchor`) and are debounced at the source
  so a burst of events is one wake.
- **`fleet-kit/wake-runner/`** — reference receiver that verifies the HMAC and spawns a
  headless session, with hard budget guards (kill switch, hourly rate cap, one session
  per owner, timeout) and `agent.woke` / `agent.slept` announcements.
- **`fleet-kit/mcs-catchup.mjs`** — `UserPromptSubmit` hook injecting new fleet events
  as context at every turn boundary; durable server-side cursor, paced, silent on
  failure so a broken server can never break a prompt.
- **`SECURITY.md`** — reporting channel, hardening posture, scan dispositions.

### Changed

- **Security hardening**: runtime image moved to distroless (image findings 177 → 38;
  no shell, perl, or package manager), current Node binary overlaid from the build
  stage, production dependency tree clean, Helm defaults locked down
  (`seccompProfile: RuntimeDefault`, redis read-only rootfs + dropped capabilities,
  CPU limits), toolchain requires Node >= 22.
- Landing page: fluid 1280px layout; fixed an `fr`-track min-content collapse that
  clipped the hero panel and a padding-shorthand override that removed page gutters.

## [0.4.0] — 2026-07-30

### Added

- **`GET /streams/:stream`** — stateless catch-up reads mirroring `/ingest`: durable
  named cursors, `commit`, `blockMs` long-poll, no MCP session, any replica.
- **Redis-backed presence** — `agents://online` is now fleet-global (TTL records +
  self-pruning index, heartbeat); the roster is the source of truth and
  `stream://agents` events are best-effort hints.
- **CoordinatorLease** — single-key Redis lease electing one replica to run webhook
  delivery and digest scheduling; `mcs_coordinator_is_leader` and
  `mcs_presence_sessions` metrics; `/healthz` reports leadership.
- Landing page (GitHub Pages) with SEO/AEO assets and privacy-friendly analytics.

### Changed

- Webhooks split into control plane (any replica: add/remove/list, announced on
  `stream://system`) and delivery plane (leader only), preserving retry/auto-disable.
- Digest scheduling claims its marker atomically (`SET NX` before task creation, CAS
  clear), closing a cross-replica and an in-process race.

## [0.3.0] — 2026-07-12

### Changed

- Helm chart hardening from a live EKS deploy; launch article linked from the README.

## [0.2.0] — 2026-07-12

### Added

- Initial public release: context streams, task queue with atomic claims and
  crash-safe leases, versioned protocols, MCP tool federation, outbound webhooks,
  HTTP ingest, agent presence, agent-driven compaction; prebuilt multi-arch images
  and an OCI Helm chart.
