# ── build ────────────────────────────────────────────────────────────────────
FROM node:22-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
# --ignore-scripts: the build stage only needs tsc, and dependency postinstall
# hooks are both a supply-chain risk and a cross-arch flake (esbuild's installer
# spawns its own binary → ETXTBSY under QEMU on linux/arm64).
RUN npm ci --ignore-scripts
COPY tsconfig.json ./
COPY src ./src
RUN npm run build

# ── production deps ──────────────────────────────────────────────────────────
FROM node:22-slim AS deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts

# ── runtime: distroless — no shell, no perl, no npm, no package manager ──────
# Removes the entire Debian userland CVE surface (perl, gzip, util-linux, …)
# and npm's bundled CLI deps. Runs as uid 65532 (nonroot).
FROM gcr.io/distroless/nodejs22-debian12:nonroot
ENV NODE_ENV=production
WORKDIR /app
# Distroless's bundled node can lag patch releases — overlay the current node
# binary from the (freshly pulled) build stage. Node statically bundles its own
# OpenSSL, so the base's system libssl3 is unused by anything that runs here.
COPY --from=build /usr/local/bin/node /nodejs/bin/node
COPY --from=deps /app/node_modules ./node_modules
COPY --from=build /app/dist ./dist
COPY package.json ./
EXPOSE 3000
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
  CMD ["/nodejs/bin/node", "-e", "fetch('http://localhost:3000/healthz').then(r=>process.exit(r.ok?0:1)).catch(()=>process.exit(1))"]
CMD ["dist/index.js"]
