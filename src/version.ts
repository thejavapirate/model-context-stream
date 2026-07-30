import { createRequire } from "node:module";

/**
 * Single-sourced package version. createRequire resolves "../package.json"
 * correctly from both src/ (tsx dev/tests) and dist/ (built image, where the
 * Dockerfile copies package.json alongside dist/).
 */
export const VERSION: string = (createRequire(import.meta.url)("../package.json") as { version: string }).version;
