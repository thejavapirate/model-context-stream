const PREFIX = "mcs";

/** Single source of truth for every Redis key this server touches. */
export const keys = {
  /** Append-only event log for a named stream. */
  stream: (name: string) => `${PREFIX}:stream:${name}`,
  /** Hash: stream name -> JSON metadata {description, createdAt, createdBy}. */
  streamRegistry: `${PREFIX}:streams`,
  /** Control stream used only to interrupt a parked XREAD when subscriptions change. */
  wakeStream: `${PREFIX}:stream:__wake__`,

  /** Hash: full task record. */
  task: (id: string) => `${PREFIX}:task:${id}`,
  /** ZSet: claimable tasks; score = priority * 1e13 + createdAtMs. */
  tasksPending: `${PREFIX}:tasks:pending`,
  /** ZSet: taskId -> lease expiry (epoch ms). */
  tasksLeases: `${PREFIX}:tasks:leases`,
  /** Set: all task ids. */
  tasksIndex: `${PREFIX}:tasks:index`,

  /** String: immutable protocol version body (markdown). */
  protocolVersion: (name: string, version: number) => `${PREFIX}:protocol:${name}:v${version}`,
  /** Hash: protocol head {latest, updatedAt, updatedBy, changelog}. */
  protocolHead: (name: string) => `${PREFIX}:protocol:${name}`,
  /** Set: protocol names. */
  protocolRegistry: `${PREFIX}:protocols`,
} as const;

/** Reserved stream names that carry system lifecycle events. */
export const SYSTEM_STREAMS = {
  tasks: "tasks",
  protocols: "protocols",
} as const;

/** Extract the stream name back out of a stream key. */
export function streamNameFromKey(key: string): string {
  return key.startsWith(`${PREFIX}:stream:`) ? key.slice(`${PREFIX}:stream:`.length) : key;
}
