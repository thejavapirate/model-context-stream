import { describe, expect, it } from "vitest";
import { parseUri, uris } from "../../src/mcp/uris.js";

/** The URI scheme is the server's API contract, and the parser is hand-rolled
 *  (URL would lowercase hosts, and stream/task names are case-sensitive). */
describe("parseUri", () => {
  it("round-trips every builder in `uris`", () => {
    expect(parseUri(uris.stream("team"))).toEqual({ kind: "stream", name: "team" });
    expect(parseUri(uris.tasksQueue)).toEqual({ kind: "tasks-queue" });
    expect(parseUri(uris.task("t_abc123"))).toEqual({ kind: "task", id: "t_abc123" });
    expect(parseUri(uris.protocol("deploy"))).toEqual({ kind: "protocol", name: "deploy" });
    expect(parseUri(uris.protocol("deploy", 4))).toEqual({ kind: "protocol", name: "deploy", version: 4 });
    expect(parseUri(uris.agentsOnline)).toEqual({ kind: "agents-online" });
  });

  it("preserves case (the reason this parser exists)", () => {
    expect(parseUri("stream://TeamAlpha")).toEqual({ kind: "stream", name: "TeamAlpha" });
    expect(parseUri("task://T_MiXeD")).toEqual({ kind: "task", id: "T_MiXeD" });
  });

  it("parses the ?from= replay cursor, decoded", () => {
    expect(parseUri("stream://team?from=1785420000000-0")).toEqual({
      kind: "stream",
      name: "team",
      from: "1785420000000-0",
    });
    expect(parseUri("stream://team?limit=5&from=17-0")).toEqual({ kind: "stream", name: "team", from: "17-0" });
    expect(parseUri("stream://team?from=a%2Bb")).toEqual({ kind: "stream", name: "team", from: "a+b" });
  });

  it("omits `from` when absent or empty rather than emitting undefined-ish values", () => {
    expect(parseUri("stream://team?from=")).toEqual({ kind: "stream", name: "team" });
    expect(parseUri("stream://team?other=1")).toEqual({ kind: "stream", name: "team" });
  });

  it("rejects malformed and unknown URIs", () => {
    for (const bad of [
      "",
      "team",
      "stream://",
      "stream:/team",
      "nope://team",
      "STREAM://team", // scheme is lowercase-only by contract
      "tasks://other", // only tasks://queue exists
      "tasks://queue/extra",
      "agents://offline",
      "task://t_1/extra", // tasks take no path
      "protocol://deploy/v", // malformed version
      "protocol://deploy/vX",
      "protocol://deploy/4", // missing the v prefix
      "protocol://deploy/v4/extra",
    ]) {
      expect(parseUri(bad), bad).toBeUndefined();
    }
  });

  it("treats a pinned protocol version as an integer", () => {
    const parsed = parseUri("protocol://deploy/v12");
    expect(parsed).toEqual({ kind: "protocol", name: "deploy", version: 12 });
    expect(typeof (parsed as { version: number }).version).toBe("number");
  });
});
