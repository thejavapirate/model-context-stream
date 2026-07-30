import { createHmac } from "node:crypto";
import { createServer, type Server } from "node:http";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Fanout } from "../../src/core/fanout.js";
import { StreamService } from "../../src/core/streams.js";
import { validateWakeUrl, WebhookService, type WakeEnvelope, type WebhookSettings } from "../../src/core/webhooks.js";

interface Received {
  body: string;
  headers: Record<string, string | string[] | undefined>;
}

let container: StartedRedisContainer;
let main: Redis;
let blocking: Redis;
let streams: StreamService;
let fanout: Fanout;
let webhooks: WebhookService;

let receiver: Server;
let receiverUrl: string;
const received: Received[] = [];

const FAST_SETTINGS: WebhookSettings = {
  timeoutMs: 2000,
  attemptDelaysMs: [50],
  disableAfterFailures: 10,
  resyncIntervalMs: 3_600_000,
};

async function waitFor(check: () => Promise<boolean> | boolean, what: string, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (await check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timed out waiting for ${what}`);
}

/** Deliveries whose event landed on `stream` (works for raw events AND wake envelopes). */
function forStream(stream: string): Received[] {
  return received.filter((d) => d.headers["x-mcs-stream"] === stream);
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
  streams = new StreamService(main, 1000, url);
  fanout = new Fanout(blocking, main);
  fanout.start();
  webhooks = new WebhookService(main, fanout, streams, FAST_SETTINGS);
  await webhooks.activate();

  receiver = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      received.push({ body, headers: req.headers });
      res.statusCode = 200;
      res.end();
    });
  });
  await new Promise<void>((r) => receiver.listen(0, () => r()));
  const addr = receiver.address();
  if (addr === null || typeof addr === "string") throw new Error("no port");
  receiverUrl = `http://127.0.0.1:${addr.port}/wake`;
});

afterAll(async () => {
  await webhooks?.stop();
  await fanout?.stop();
  receiver?.close();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

describe("wake registrations", () => {
  it("delivers a signed wake envelope with event + cursorAnchor", async () => {
    await webhooks.add({
      stream: "wk-env",
      url: receiverUrl,
      secret: "wake-secret",
      kind: "wake",
      owner: "alice",
      debounceSec: 1,
      createdBy: "alice",
    });

    const pub = await streams.publish({ stream: "wk-env", type: "oncall.page", source: "ci", payload: { sev: 1 } });
    await waitFor(() => forStream("wk-env").length >= 1, "wake delivery");

    const d = forStream("wk-env")[0]!;
    const envelope = JSON.parse(d.body) as WakeEnvelope;
    expect(envelope.wake.owner).toBe("alice");
    expect(envelope.wake.registrationId).toMatch(/^wh_/);
    expect(envelope.event.type).toBe("oncall.page");
    expect(envelope.cursorAnchor).toBe(pub.event.id);
    // Same HMAC scheme as data webhooks, signed over the envelope body.
    const expected = `sha256=${createHmac("sha256", "wake-secret").update(d.body).digest("hex")}`;
    expect(d.headers["x-mcs-signature"]).toBe(expected);
  });

  it("debounces at the source: a burst becomes one wake; the next window fires again", async () => {
    await webhooks.add({
      stream: "wk-deb",
      url: receiverUrl,
      kind: "wake",
      owner: "alice",
      debounceSec: 1,
      createdBy: "alice",
    });

    for (let i = 0; i < 3; i++) {
      await streams.publish({ stream: "wk-deb", type: "burst.item", source: "t", payload: { i } });
    }
    await waitFor(() => forStream("wk-deb").length >= 1, "first wake");
    await new Promise((r) => setTimeout(r, 400));
    expect(forStream("wk-deb")).toHaveLength(1); // burst collapsed to one wake

    await new Promise((r) => setTimeout(r, 1100)); // let the window pass
    await streams.publish({ stream: "wk-deb", type: "burst.item", source: "t", payload: { i: 99 } });
    await waitFor(() => forStream("wk-deb").length >= 2, "second wake after debounce window");
  });

  it("plain webhooks stay un-debounced with raw event bodies", async () => {
    await webhooks.add({ stream: "wk-plain", url: receiverUrl, createdBy: "admin" });

    for (let i = 0; i < 3; i++) {
      await streams.publish({ stream: "wk-plain", type: "data.item", source: "t", payload: { i } });
    }
    await waitFor(() => forStream("wk-plain").length >= 3, "all three deliveries");
    const bodies = forStream("wk-plain").map((d) => JSON.parse(d.body) as Record<string, unknown>);
    for (const b of bodies) {
      expect(b.wake).toBeUndefined(); // raw event, no envelope
      expect(b.type).toBe("data.item");
    }
  });

  it("enforces ownership on remove: owner or admin only", async () => {
    const hook = await webhooks.add({
      stream: "wk-own",
      url: receiverUrl,
      kind: "wake",
      owner: "alice",
      createdBy: "alice",
    });

    expect(await webhooks.removeOwned(hook.id, "bob", false)).toBe("forbidden");
    expect((await webhooks.listByOwner("alice")).some((w) => w.id === hook.id)).toBe(true); // survived

    expect(await webhooks.removeOwned(hook.id, "alice", false)).toBe("removed");
    expect(await webhooks.removeOwned(hook.id, "alice", false)).toBe("not_found");

    const another = await webhooks.add({
      stream: "wk-own",
      url: receiverUrl,
      kind: "wake",
      owner: "alice",
      createdBy: "alice",
    });
    expect(await webhooks.removeOwned(another.id, "someone-else", true)).toBe("removed"); // admin override
  });

  it("scopes listByOwner/countByOwner to one agent's wakes (quota input)", async () => {
    const before = await webhooks.countByOwner("carol");
    for (let i = 0; i < 5; i++) {
      await webhooks.add({
        stream: `wk-quota-${i}`,
        url: receiverUrl,
        kind: "wake",
        owner: "carol",
        secret: i === 0 ? "s" : undefined,
        createdBy: "carol",
      });
    }
    await webhooks.add({ stream: "wk-quota-x", url: receiverUrl, kind: "wake", owner: "dave", createdBy: "dave" });
    await webhooks.add({ stream: "wk-quota-x", url: receiverUrl, createdBy: "admin" }); // plain webhook

    expect(await webhooks.countByOwner("carol")).toBe(before + 5); // register_wake rejects at >= 5
    const mine = await webhooks.listByOwner("carol");
    expect(mine.every((w) => w.kind === "wake" && w.owner === "carol")).toBe(true);
    expect(mine.some((w) => w.hasSecret)).toBe(true);
    expect(mine.some((w) => "secret" in w && (w as Record<string, unknown>).secret)).toBe(false); // redacted
  });

  it("validateWakeUrl: http(s) without credentials only; private hosts deliberately allowed", () => {
    expect(validateWakeUrl("http://192.168.1.10:8377/")).toBeNull(); // self-hosted runners are the use case
    expect(validateWakeUrl("https://runner.example.com/wake")).toBeNull();
    expect(validateWakeUrl("ftp://example.com/")).toMatch(/http/);
    expect(validateWakeUrl("http://user:pass@example.com/")).toMatch(/credentials/);
    expect(validateWakeUrl("not a url")).toMatch(/invalid/);
  });
});
