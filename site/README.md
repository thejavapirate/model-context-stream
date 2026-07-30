# site/ — the landing page

Static landing page for model-context-stream, deployed to GitHub Pages by
`.github/workflows/pages.yml` on every push to `main` that touches `site/`.
Live at: https://thejavapirate.github.io/model-context-stream/

Single dependency-free `index.html` (inline CSS/JS) plus SEO/AEO assets:
`robots.txt` (allow all crawlers, incl. AI), `sitemap.xml`, `llms.txt`,
JSON-LD (`SoftwareApplication` + `SoftwareSourceCode`), and a self-hosted
GoatCounter `count.js` (ISC license, no cookies, no banner needed).

## Go-live checklist (one-time, manual)

1. **Enable Pages**: repo Settings → Pages → Source: **GitHub Actions**. Then push
   (or re-run the `Deploy site to GitHub Pages` workflow).
2. **Claim analytics**: register the code `mcstream` at https://www.goatcounter.com
   (free). If taken, pick another and update the `data-goatcounter` endpoint in
   `index.html`. Dashboard: https://mcstream.goatcounter.com.
3. **Google Search Console**: add a **URL-prefix** property for
   `https://thejavapirate.github.io/model-context-stream/`, choose meta-tag
   verification, paste the tag into the marked comment in `index.html`, push,
   verify, then submit `sitemap.xml`.
4. **Bing Webmaster Tools**: sign in → **Import from Google Search Console**
   (Bing's index feeds ChatGPT citations; its AI Performance report is the only
   first-party AI-citation data available).
5. **Repo hygiene** (higher AI-citation leverage than the site itself): set the
   repo description + topics (`mcp`, `mcp-server`, `multi-agent`, `ai-agents`,
   `agent-coordination`, `redis-streams`, `typescript`), upload a social-preview
   image in repo Settings, and keep the website link in the README top.
6. **Get listed**: PR the project into `awesome-mcp-servers` lists and MCP
   directories (PulseMCP, mcp.so, the official registry) — in this niche these
   are the sources AI assistants actually learned from.

## Metrics stack

- **Site**: GoatCounter — pageviews, referrers (filter `chatgpt.com`, `claude.ai`,
  `perplexity.ai` manually for AI traffic), plus outbound-click events
  `ext-github-hero`/`ext-github-nav`/`ext-github-footer`. Client-side numbers are
  a floor: ~half of a dev audience runs ad blockers.
- **Repo (ground truth, unblockable)**: Insights → Traffic (14-day window);
  `.github/workflows/traffic-snapshot.yml` snapshots it weekly into `metrics/`
  so history survives. Release download counts land in the same snapshots.
- **GHCR pulls**: no official API (2026). If pull metrics matter, front GHCR with
  Scarf Gateway (free for OSS) — costs changing the documented pull URL.
- **Campaigns**: links from articles to the site should carry
  `?utm_source=medium&utm_medium=article&utm_campaign=launch-2026` (GoatCounter
  Campaigns view); links from site → GitHub are tracked by the click events, and
  the repo's referrer report shows `thejavapirate.github.io` + `medium.com`.
