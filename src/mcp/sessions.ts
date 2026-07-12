import { Fanout } from "../core/fanout.js";
import { SYSTEM_STREAMS } from "../redis/keys.js";
import { SessionNotifier } from "./notifier.js";
import { parseUri } from "./uris.js";

export interface Session {
  id: string;
  agentName: string;
  connectedAt: string;
  lastSeenAt: number;
  notifier: SessionNotifier;
  /** uri -> fanout unsubscribe */
  subscriptions: Map<string, () => void>;
  close: () => void;
}

const IDLE_SWEEP_MS = 60_000;
const IDLE_MAX_MS = 30 * 60_000;

export class SessionRegistry {
  private sessions = new Map<string, Session>();
  private sweeper?: NodeJS.Timeout;

  constructor(private readonly fanout: Fanout) {}

  add(input: {
    id: string;
    agentName: string;
    sendUpdated: (uri: string) => Promise<void>;
    close: () => void;
  }): Session {
    const session: Session = {
      id: input.id,
      agentName: input.agentName,
      connectedAt: new Date().toISOString(),
      lastSeenAt: Date.now(),
      notifier: new SessionNotifier(input.id, input.sendUpdated),
      subscriptions: new Map(),
      close: input.close,
    };
    this.sessions.set(input.id, session);
    return session;
  }

  get(id: string): Session | undefined {
    return this.sessions.get(id);
  }

  touch(id: string): void {
    const s = this.sessions.get(id);
    if (s) s.lastSeenAt = Date.now();
  }

  all(): Session[] {
    return [...this.sessions.values()];
  }

  /**
   * Map an MCP resource URI onto the underlying fanout stream(s) and register
   * a filtered listener that enqueues a notification for this session.
   * Returns false for unknown/unsubscribable URIs.
   */
  async subscribeUri(sessionId: string, uri: string): Promise<boolean> {
    const session = this.sessions.get(sessionId);
    if (!session) return false;
    if (session.subscriptions.has(uri)) return true;

    const parsed = parseUri(uri);
    if (!parsed) return false;

    let unsubscribe: (() => void) | undefined;
    switch (parsed.kind) {
      case "stream":
        unsubscribe = await this.fanout.subscribe(parsed.name, () => session.notifier.enqueue(uri));
        break;
      case "tasks-queue":
        unsubscribe = await this.fanout.subscribe(SYSTEM_STREAMS.tasks, () => session.notifier.enqueue(uri));
        break;
      case "task":
        unsubscribe = await this.fanout.subscribe(SYSTEM_STREAMS.tasks, (event) => {
          if (event.payload.taskId === parsed.id) session.notifier.enqueue(uri);
        });
        break;
      case "protocol":
        if (parsed.version !== undefined) return false; // pinned versions are immutable
        unsubscribe = await this.fanout.subscribe(SYSTEM_STREAMS.protocols, (event) => {
          if (event.payload.name === parsed.name) session.notifier.enqueue(uri);
        });
        break;
    }
    if (!unsubscribe) return false;
    session.subscriptions.set(uri, unsubscribe);
    return true;
  }

  unsubscribeUri(sessionId: string, uri: string): void {
    const session = this.sessions.get(sessionId);
    const unsub = session?.subscriptions.get(uri);
    if (unsub) {
      unsub();
      session!.subscriptions.delete(uri);
    }
  }

  remove(id: string): void {
    const session = this.sessions.get(id);
    if (!session) return;
    for (const unsub of session.subscriptions.values()) unsub();
    session.subscriptions.clear();
    session.notifier.close();
    this.sessions.delete(id);
  }

  startIdleSweeper(): void {
    this.sweeper = setInterval(() => {
      const cutoff = Date.now() - IDLE_MAX_MS;
      for (const session of this.sessions.values()) {
        if (session.lastSeenAt < cutoff) {
          console.log(`[sessions] evicting idle session ${session.id} (${session.agentName})`);
          try {
            session.close();
          } catch {
            /* transport already gone */
          }
          this.remove(session.id);
        }
      }
    }, IDLE_SWEEP_MS);
    this.sweeper.unref();
  }

  stop(): void {
    if (this.sweeper) clearInterval(this.sweeper);
    for (const id of [...this.sessions.keys()]) this.remove(id);
  }
}
