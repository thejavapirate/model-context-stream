/**
 * Debounced, coalesced dispatch of resources/updated notifications.
 * MCP resource-updated notifications carry only a URI (no payload), so
 * coalescing a burst into one notification is lossless — the client re-reads
 * the resource either way.
 */

const DEBOUNCE_MS = 200;
const MAX_WAIT_MS = 1000;
const LIST_CHANGED_MS = 500;

type SendUpdated = (uri: string) => Promise<void>;
type SendListChanged = () => Promise<void>;

interface PendingEntry {
  timer: NodeJS.Timeout;
  firstEnqueuedAt: number;
}

export class SessionNotifier {
  private pending = new Map<string, PendingEntry>(); // uri -> pending timer
  private closed = false;

  constructor(
    private readonly sessionId: string,
    private readonly sendUpdated: SendUpdated,
  ) {}

  /** Queue a resources/updated for this session; coalesces bursts per URI. */
  enqueue(uri: string): void {
    if (this.closed) return;
    const existing = this.pending.get(uri);
    const now = Date.now();

    if (existing) {
      // Trailing-edge debounce, but never delay past MAX_WAIT.
      if (now - existing.firstEnqueuedAt >= MAX_WAIT_MS - DEBOUNCE_MS) return; // fires soon enough
      clearTimeout(existing.timer);
      existing.timer = setTimeout(() => this.fire(uri), DEBOUNCE_MS);
      return;
    }

    this.pending.set(uri, {
      firstEnqueuedAt: now,
      timer: setTimeout(() => this.fire(uri), DEBOUNCE_MS),
    });
  }

  private fire(uri: string): void {
    this.pending.delete(uri);
    if (this.closed) return;
    this.sendUpdated(uri).catch((err) => {
      // A dying SSE stream must never break fanout for other sessions.
      console.error(`[notifier] send failed for session ${this.sessionId}, uri ${uri}:`, err?.message ?? err);
    });
  }

  close(): void {
    this.closed = true;
    for (const entry of this.pending.values()) clearTimeout(entry.timer);
    this.pending.clear();
  }
}

/** Globally coalesced resources/list_changed across all sessions. */
export class ListChangedNotifier {
  private timer?: NodeJS.Timeout;
  private senders = new Map<string, SendListChanged>(); // sessionId -> sender

  register(sessionId: string, sender: SendListChanged): void {
    this.senders.set(sessionId, sender);
  }

  unregister(sessionId: string): void {
    this.senders.delete(sessionId);
  }

  enqueue(): void {
    if (this.timer) return;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      for (const [sessionId, send] of this.senders) {
        send().catch((err) =>
          console.error(`[notifier] list_changed failed for ${sessionId}:`, err?.message ?? err),
        );
      }
    }, LIST_CHANGED_MS);
  }

  close(): void {
    if (this.timer) clearTimeout(this.timer);
    this.senders.clear();
  }
}
