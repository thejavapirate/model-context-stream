# Security

## Reporting

Please report vulnerabilities privately via GitHub Security Advisories
("Report a vulnerability" on the repo) rather than public issues.

## Hardening posture

- **Runtime image**: distroless (`gcr.io/distroless/nodejs22-debian12:nonroot`) — no
  shell, no package manager, no perl/npm/userland; runs as uid 65532. The current
  Node binary is overlaid from the freshly pulled build stage so patch releases
  don't wait on distroless rebuilds. Node statically bundles its own OpenSSL.
- **Kubernetes** (Helm defaults): non-root, read-only root filesystems (server AND
  redis), all capabilities dropped, `seccompProfile: RuntimeDefault`, no privilege
  escalation, CPU+memory limits.
- **Auth**: bearer tokens with binary agent/admin roles (`requireAdmin()` gates
  webhooks, federation, stream config); constant-time token comparison; running
  without tokens is loudly warned dev-only mode.
- **Outbound HTTP** (webhooks/wakes): HMAC-signed bodies, redirects never followed,
  URLs must be credential-free http(s). `add_webhook` is admin-only (SSRF gate).
  `register_wake` is deliberately agent-scoped: self-hosted wake runners live on
  private networks, so private addresses are allowed by design — bounded by a
  5-per-agent quota, audit events on `stream://system`, and (in hostile
  environments) operator egress controls.

## Scanning

Last full pass 2026-07-30: Trivy (image/fs/config), Grype, SonarQube, Semgrep
(p/security-audit, p/secrets), Gitleaks, Hadolint, npm audit.

Results: **code-level scanners: zero findings** (Semgrep 0, Gitleaks 0, SonarQube
0 vulnerabilities + 0 hotspots). `npm audit --omit=dev`: **zero** — the production
dependency tree is clean.

Accepted residuals (re-check each release):

| Finding | Disposition |
|---|---|
| `libssl3` CVEs in the distroless base | **Unused code**: Node statically bundles its own OpenSSL (3.5.x), and distroless contains no other executables that could link libssl. Clears when distroless rebuilds on current Debian. |
| `libc6` CVEs in the distroless base | Node does link glibc. Debian-patched entries flow in with the next distroless rebuild (typically days); the rest are Debian no-fix. No known exploit path through this server's usage. |
| `archiver`/`glob`/`minimatch` chain (9 high) under `testcontainers` | **Dev-only** (test tooling, never shipped); no fixed upstream release exists at any version. |

Re-run locally:

```sh
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock aquasec/trivy image ghcr.io/thejavapirate/model-context-stream:latest
docker run --rm -v /var/run/docker.sock:/var/run/docker.sock anchore/grype ghcr.io/thejavapirate/model-context-stream:latest
docker run --rm -v "$PWD":/src semgrep/semgrep semgrep scan --config p/security-audit --config p/secrets /src
npm audit --omit=dev
```
