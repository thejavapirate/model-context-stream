import type { Response } from "express";
import { z } from "zod";
import { eventTypeSchema, streamNameSchema } from "../core/events.js";
import type { StreamService } from "../core/streams.js";
import type { ListChangedNotifier } from "../mcp/notifier.js";
import type { AuthedRequest } from "./auth.js";

const ingestBodySchema = z.object({
  type: eventTypeSchema,
  payload: z.record(z.unknown()).default({}),
  correlationId: z.string().max(256).optional(),
});

/**
 * POST /ingest/:stream — the door for non-MCP systems (CI, webhooks,
 * monitoring). Same publish path as the publish_event tool, so ingested
 * events get identical envelopes, trimming, and notification fanout.
 */
export function ingestHandler(streams: StreamService, listChanged: ListChangedNotifier) {
  return async (req: AuthedRequest, res: Response): Promise<void> => {
    const streamName = streamNameSchema.safeParse(req.params.stream);
    if (!streamName.success) {
      res.status(400).json({ error: "invalid stream name" });
      return;
    }
    const body = ingestBodySchema.safeParse(req.body);
    if (!body.success) {
      res.status(400).json({ error: "invalid body", details: body.error.issues });
      return;
    }

    const result = await streams.publish({
      stream: streamName.data,
      type: body.data.type,
      payload: body.data.payload,
      source: `ingest:${req.tokenAgent ?? "external"}`,
      ...(body.data.correlationId ? { correlationId: body.data.correlationId } : {}),
    });
    if (result.createdStream) listChanged.enqueue();

    res.status(201).json({ id: result.event.id, stream: streamName.data, ts: result.event.ts });
  };
}
