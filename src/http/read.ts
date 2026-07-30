import type { Response } from "express";
import { z } from "zod";
import type { CursorService } from "../core/cursors.js";
import { streamNameSchema } from "../core/events.js";
import type { StreamService } from "../core/streams.js";
import { type AuthedRequest, resolveHttpAgentName } from "./auth.js";

const readQuerySchema = z
  .object({
    /** Named durable cursor (per-agent) — resumes where it last committed. */
    cursor: z.string().min(1).max(64).optional(),
    /** Exclusive entry-id cursor from a previous read. */
    from: z
      .string()
      .regex(/^\d+-\d+$/, "must be a stream entry id like 1720000000000-0")
      .optional(),
    sinceMs: z.coerce.number().int().positive().optional(),
    // REST stays cheap; the MCP read_stream tool (cap 1000) is the bulk-replay path.
    limit: z.coerce.number().int().min(1).max(200).default(50),
    /** Long-poll for new events (same cap as the read_stream tool). */
    blockMs: z.coerce.number().int().min(0).max(25_000).optional(),
    // NOT z.coerce.boolean: "false" would coerce truthy.
    commit: z
      .enum(["true", "false"])
      .default("false")
      .transform((v) => v === "true"),
  })
  .refine((q) => [q.cursor, q.from, q.sinceMs].filter((x) => x !== undefined).length <= 1, {
    message: "cursor, from, and sinceMs are mutually exclusive",
  });

/**
 * GET /streams/:stream — the read mirror of ingest: a session-free catch-up
 * surface for hooks, CI, and curl. Semantics match the read_stream MCP tool
 * exactly (fresh cursor replays retained history from 0-0; commit only
 * advances when events were returned). Works against any replica.
 */
export function readStreamHandler(streams: StreamService, cursors: CursorService) {
  return async (req: AuthedRequest, res: Response): Promise<void> => {
    const streamName = streamNameSchema.safeParse(req.params.stream);
    if (!streamName.success) {
      res.status(400).json({ error: "invalid stream name" });
      return;
    }
    const query = readQuerySchema.safeParse(req.query);
    if (!query.success) {
      res.status(400).json({ error: "invalid query", details: query.error.issues });
      return;
    }
    const { cursor, from, sinceMs, limit, blockMs, commit } = query.data;

    const agent = resolveHttpAgentName(req);
    if (cursor && agent === "anon") {
      // Anonymous callers would all share one cursor namespace and clobber each other.
      res.status(400).json({ error: "cursor requires an identity: send X-Agent-Name or use an agent-bound token" });
      return;
    }

    let fromId = from;
    if (cursor && !fromId) {
      // First resume on a fresh cursor replays from the start of retained history.
      fromId = (await cursors.get(agent, streamName.data, cursor)) ?? "0-0";
    }

    const result = await streams.read({
      stream: streamName.data,
      ...(fromId ? { fromId } : {}),
      ...(sinceMs ? { sinceMs } : {}),
      limit,
      ...(blockMs ? { blockMs } : {}),
    });

    let committed: string | undefined;
    if (cursor && commit && result.nextCursor && result.events.length > 0) {
      await cursors.commit(agent, streamName.data, result.nextCursor, cursor);
      committed = result.nextCursor;
    }
    res.json({ ...result, ...(cursor ? { cursor, committed: committed ?? null } : {}) });
  };
}
