/**
 * Resource URI scheme — the server's API contract:
 *   stream://{name}            recent events (subscribable)
 *   stream://{name}?from={id}  replay from cursor
 *   tasks://queue              task board summary (subscribable)
 *   task://{id}                one task (subscribable)
 *   protocol://{name}          latest protocol version (subscribable)
 *   protocol://{name}/v{n}     pinned immutable version
 *
 * Parsed by hand (not `new URL`) because URL lowercases the host and task
 * ids / stream names are case-sensitive.
 */

export type ParsedUri =
  | { kind: "stream"; name: string; from?: string }
  | { kind: "tasks-queue" }
  | { kind: "task"; id: string }
  | { kind: "protocol"; name: string; version?: number }
  | { kind: "agents-online" };

export const uris = {
  stream: (name: string) => `stream://${name}`,
  tasksQueue: "tasks://queue",
  task: (id: string) => `task://${id}`,
  protocol: (name: string, version?: number) =>
    version === undefined ? `protocol://${name}` : `protocol://${name}/v${version}`,
  agentsOnline: "agents://online",
};

const URI_RE = /^([a-z]+):\/\/([^/?#]+)(?:\/([^?#]*))?(?:\?(.*))?$/;

export function parseUri(uri: string): ParsedUri | undefined {
  const m = URI_RE.exec(uri);
  if (!m) return undefined;
  const [, scheme, host, path, query] = m;
  if (!host) return undefined;

  switch (scheme) {
    case "stream": {
      let from: string | undefined;
      if (query) {
        for (const part of query.split("&")) {
          const [k, v] = part.split("=");
          if (k === "from" && v) from = decodeURIComponent(v);
        }
      }
      return { kind: "stream", name: host, ...(from ? { from } : {}) };
    }
    case "tasks":
      return host === "queue" && !path ? { kind: "tasks-queue" } : undefined;
    case "agents":
      return host === "online" && !path ? { kind: "agents-online" } : undefined;
    case "task":
      return path ? undefined : { kind: "task", id: host };
    case "protocol": {
      if (!path) return { kind: "protocol", name: host };
      const vm = /^v(\d+)$/.exec(path);
      if (!vm) return undefined;
      return { kind: "protocol", name: host, version: Number(vm[1]) };
    }
    default:
      return undefined;
  }
}
