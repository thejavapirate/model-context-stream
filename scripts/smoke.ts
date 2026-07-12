/**
 * Smoke-test a running server:
 *   MCS_URL=http://localhost:3000 MCS_TOKEN=tok_local_dev npm run smoke
 */
import { runSmokeChecks } from "./smoke-checks.js";

const baseUrl = (process.env.MCS_URL ?? "http://localhost:3000").replace(/\/$/, "");
const token = process.env.MCS_TOKEN;

runSmokeChecks({ baseUrl, ...(token ? { token } : {}) })
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("smoke: FAILED —", err.message ?? err);
    process.exit(1);
  });
