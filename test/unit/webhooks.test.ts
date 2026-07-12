import { createHmac } from "node:crypto";
import { createServer, type Server } from "node:http";
import { RedisContainer, StartedRedisContainer } from "@testcontainers/redis";
import { Redis } from "ioredis";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Fanout } from "../../src/core/fanout.js";
import { StreamService } from "../../src/core/streams.js";
import { WebhookService } from "../../src/core/webhooks.js";
import { SYSTEM_STREAMS } from "../../src/redis/keys.js";

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
let respondWith = 200;

async function waitFor(check: () => boolean, what: string, timeoutMs = 8000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    if (check()) return;
    await new Promise((r) => setTimeout(r, 50));
  }
  throw new Error(`timed out waiting for ${what}`);
}

beforeAll(async () => {
  container = await new RedisContainer("redis:7-alpine").start();
  const url = container.getConnectionUrl();
  main = new Redis(url);
  blocking = new Redis(url, { maxRetriesPerRequest: null });
  streams = new StreamService(main, 1000, url);
  fanout = new Fanout(blocking, main);
  fanout.start();
  webhooks = new WebhookService(main, fanout, streams, {
    timeoutMs: 2000,
    attemptDelaysMs: [50],
    disableAfterFailures: 2,
  });
  await webhooks.start();

  receiver = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      received.push({ body, headers: req.headers });
      res.statusCode = respondWith;
      res.end();
    });
  });
  await new Promise<void>((r) => receiver.listen(0, () => r()));
  const addr = receiver.address();
  if (addr === null || typeof addr === "string") throw new Error("no port");
  receiverUrl = `http://127.0.0.1:${addr.port}/hook`;
});

afterAll(async () => {
  await webhooks?.stop();
  await fanout?.stop();
  receiver?.close();
  main?.disconnect();
  blocking?.disconnect();
  await container?.stop();
});

describe("WebhookService", () => {
  it("delivers events with envelope headers and HMAC signature", async () => {
    respondWith = 200;
    await webhooks.add({ stream: "wh-basic", url: receiverUrl, secret: "s3cret", createdBy: "admin" });

    await streams.publish({ stream: "wh-basic", type: "deploy.done", source: "ci", payload: { ok: true } });
    await waitFor(() => received.length >= 1, "first delivery");

    const d = received[0]!;
    const event = JSON.parse(d.body);
    expect(event.type).toBe("deploy.done");
    expect(event.payload).toEqual({ ok: true });
    expect(d.headers["x-mcs-stream"]).toBe("wh-basic");
    expect(d.headers["x-mcs-event-type"]).toBe("deploy.done");
    const expected = `sha256=${createHmac("sha256", "s3cret").update(d.body).digest("hex")}`;
    expect(d.headers["x-mcs-signature"]).toBe(expected);
  });

  it("filters by event types", async () => {
    respondWith = 200;
    const before = received.length;
    await webhooks.add({ stream: "wh-filter", url: receiverUrl, types: ["keep.me"], createdBy: "admin" });

    await streams.publish({ stream: "wh-filter", type: "drop.me", source: "t", payload: {} });
    await streams.publish({ stream: "wh-filter", type: "keep.me", source: "t", payload: {} });
    await waitFor(() => received.length > before, "filtered delivery");
    await new Promise((r) => setTimeout(r, 300)); // give the dropped one time to (not) arrive

    const types = received.slice(before).map((d) => JSON.parse(d.body).type);
    expect(types).toEqual(["keep.me"]);
  });

  it("disables a webhook after consecutive failures and announces it on the system stream", async () => {
    respondWith = 500;
    const hook = await webhooks.add({ stream: "wh-dying", url: receiverUrl, createdBy: "admin" });

    const sysBefore = await streams.read({ stream: SYSTEM_STREAMS.system, limit: 1 });
    await streams.publish({ stream: "wh-dying", type: "a.b", source: "t", payload: {} });
    await streams.publish({ stream: "wh-dying", type: "a.b", source: "t", payload: {} });

    await waitFor(() => webhooks.list().find((w) => w.id === hook.id)?.disabled === true, "webhook disabled", 15000);

    const sys = await streams.read({
      stream: SYSTEM_STREAMS.system,
      fromId: sysBefore.nextCursor ?? undefined,
      limit: 50,
    });
    const disabledEvent = sys.events.find(
      (e) => e.type === "webhook.disabled" && e.payload.webhookId === hook.id,
    );
    expect(disabledEvent).toBeDefined();

    // A disabled hook stops delivering.
    const count = received.length;
    respondWith = 200;
    await streams.publish({ stream: "wh-dying", type: "a.b", source: "t", payload: {} });
    await new Promise((r) => setTimeout(r, 400));
    expect(received.length).toBe(count);
  });

  it("survives restarts: registry reloads from Redis", async () => {
    respondWith = 200;
    await webhooks.add({ stream: "wh-reload", url: receiverUrl, createdBy: "admin" });

    const second = new WebhookService(main, fanout, streams, {
      timeoutMs: 2000,
      attemptDelaysMs: [50],
      disableAfterFailures: 2,
    });
    await second.start();
    expect(second.list().find((w) => w.stream === "wh-reload")).toBeDefined();
    await second.stop();
  });
});
