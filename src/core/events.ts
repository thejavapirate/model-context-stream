import { z } from "zod";

/**
 * The canonical event envelope. `id` is the Redis stream entry ID — it doubles
 * as the replay cursor/offset and is never stored as a field.
 */
export interface StreamEvent {
  id: string;
  stream: string;
  type: string;
  source: string;
  ts: string;
  correlationId?: string;
  payload: Record<string, unknown>;
}

export const streamNameSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/, "stream names are alphanumeric plus . _ -");

export const eventTypeSchema = z
  .string()
  .min(1)
  .max(128)
  .regex(/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/, "event types are dot-namespaced identifiers");

export const publishInputSchema = z.object({
  stream: streamNameSchema,
  type: eventTypeSchema,
  payload: z.record(z.unknown()).default({}),
  correlationId: z.string().max(256).optional(),
});

/** Flatten an event into Redis XADD field/value pairs (payload JSON-encoded). */
export function toEntryFields(e: {
  type: string;
  source: string;
  ts: string;
  correlationId?: string;
  payload: Record<string, unknown>;
}): string[] {
  const fields = ["type", e.type, "source", e.source, "ts", e.ts, "payload", JSON.stringify(e.payload)];
  if (e.correlationId) fields.push("cid", e.correlationId);
  return fields;
}

/** Rebuild a StreamEvent from a raw XRANGE/XREAD entry. */
export function fromEntry(stream: string, id: string, raw: string[]): StreamEvent {
  const map: Record<string, string> = {};
  for (let i = 0; i + 1 < raw.length; i += 2) {
    map[raw[i]!] = raw[i + 1]!;
  }
  let payload: Record<string, unknown> = {};
  try {
    payload = map.payload ? JSON.parse(map.payload) : {};
  } catch {
    payload = { _raw: map.payload };
  }
  return {
    id,
    stream,
    type: map.type ?? "unknown",
    source: map.source ?? "unknown",
    ts: map.ts ?? "",
    ...(map.cid ? { correlationId: map.cid } : {}),
    payload,
  };
}
