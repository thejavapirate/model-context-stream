import { Redis } from "ioredis";

export interface RedisConnections {
  /** General-purpose connection for commands. */
  main: Redis;
  /** Dedicated connection for blocking XREADs (fanout loop). */
  blocking: Redis;
  quit(): Promise<void>;
}

export function createRedis(url: string): RedisConnections {
  const opts = { maxRetriesPerRequest: 3, lazyConnect: false } as const;
  const main = new Redis(url, opts);
  // Blocking reads park the connection for seconds at a time; never share it.
  const blocking = new Redis(url, { ...opts, maxRetriesPerRequest: null });

  // ioredis reconnects on its own; without a listener every retry prints an
  // "Unhandled error event" stack. Log the first failure per connection, then
  // stay quiet until it recovers — /healthz is the real liveness signal.
  for (const [name, conn] of [
    ["main", main],
    ["blocking", blocking],
  ] as const) {
    let down = false;
    conn.on("error", (err: Error) => {
      if (down) return;
      down = true;
      console.warn(`[redis:${name}] ${err.message} — retrying in background`);
    });
    conn.on("ready", () => {
      if (down) console.log(`[redis:${name}] reconnected`);
      down = false;
    });
  }

  return {
    main,
    blocking,
    async quit() {
      await Promise.allSettled([main.quit(), blocking.quit()]);
    },
  };
}
