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

  return {
    main,
    blocking,
    async quit() {
      await Promise.allSettled([main.quit(), blocking.quit()]);
    },
  };
}
