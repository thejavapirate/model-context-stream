import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { ListChangedNotifier, SessionNotifier } from "../../src/mcp/notifier.js";

describe("SessionNotifier", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("sends one notification for a single enqueue after the debounce", async () => {
    const sent: string[] = [];
    const n = new SessionNotifier("s1", async (uri) => void sent.push(uri));

    n.enqueue("stream://demo");
    expect(sent).toHaveLength(0);
    await vi.advanceTimersByTimeAsync(250);
    expect(sent).toEqual(["stream://demo"]);
  });

  it("coalesces a burst into few notifications, bounded by max-wait", async () => {
    const sent: string[] = [];
    const n = new SessionNotifier("s1", async (uri) => void sent.push(uri));

    // Enqueue every 50ms for 2s: trailing-edge debounce alone would starve,
    // but max-wait forces roughly one send per second.
    for (let t = 0; t < 40; t++) {
      n.enqueue("stream://busy");
      await vi.advanceTimersByTimeAsync(50);
    }
    await vi.advanceTimersByTimeAsync(300);

    expect(sent.length).toBeGreaterThanOrEqual(1);
    expect(sent.length).toBeLessThanOrEqual(4); // ~2s of burst → a handful, not 40
  });

  it("tracks URIs independently", async () => {
    const sent: string[] = [];
    const n = new SessionNotifier("s1", async (uri) => void sent.push(uri));
    n.enqueue("stream://a");
    n.enqueue("stream://b");
    await vi.advanceTimersByTimeAsync(250);
    expect(sent.sort()).toEqual(["stream://a", "stream://b"]);
  });

  it("drops pending timers on close", async () => {
    const sent: string[] = [];
    const n = new SessionNotifier("s1", async (uri) => void sent.push(uri));
    n.enqueue("stream://a");
    n.close();
    await vi.advanceTimersByTimeAsync(500);
    expect(sent).toHaveLength(0);
  });

  it("survives send failures", async () => {
    const n = new SessionNotifier("s1", async () => {
      throw new Error("SSE died");
    });
    n.enqueue("stream://a");
    await vi.advanceTimersByTimeAsync(250); // must not reject/throw
  });
});

describe("ListChangedNotifier", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("coalesces globally and fans out to all registered sessions", async () => {
    const counts = { a: 0, b: 0 };
    const n = new ListChangedNotifier();
    n.register("a", async () => void counts.a++);
    n.register("b", async () => void counts.b++);

    n.enqueue();
    n.enqueue();
    n.enqueue();
    await vi.advanceTimersByTimeAsync(600);

    expect(counts).toEqual({ a: 1, b: 1 });
  });

  it("stops notifying unregistered sessions", async () => {
    const counts = { a: 0 };
    const n = new ListChangedNotifier();
    n.register("a", async () => void counts.a++);
    n.unregister("a");
    n.enqueue();
    await vi.advanceTimersByTimeAsync(600);
    expect(counts.a).toBe(0);
  });
});
