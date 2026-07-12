import { Redis } from "ioredis";
import { nanoid } from "nanoid";
import { keys, SYSTEM_STREAMS } from "../redis/keys.js";
import { StreamService } from "./streams.js";

export type TaskStatus = "pending" | "claimed" | "in_progress" | "completed" | "failed";

export interface Task {
  id: string;
  title: string;
  description?: string;
  protocol?: string;
  priority: number;
  payload?: Record<string, unknown>;
  correlationId?: string;
  status: TaskStatus;
  createdBy: string;
  createdAt: string;
  updatedAt: string;
  claimedBy?: string;
  leaseExpiresAt?: string;
  attempts: number;
  progress?: string;
  progressPercent?: number;
  result?: Record<string, unknown>;
  error?: string;
}

export type ClaimOutcome =
  | { claimed: true; task: Task }
  | { claimed: false; reason: "queue_empty" }
  | { claimed: false; reason: "conflict"; status: string; claimedBy?: string }
  | { claimed: false; reason: "not_found" };

const TASK_PREFIX = "mcs:task:";
const DEFAULT_LEASE_SECONDS = 300;

/** Priority 1 (high) pops before 3 (low); FIFO within a priority band. */
function pendingScore(priority: number, createdAtMs: number): number {
  return priority * 1e13 + createdAtMs;
}

// Atomic claim: exactly one winner under any concurrency.
// KEYS: [pendingZset, leasesZset]  ARGV: [taskId|"", agent, nowMs, leaseMs, isoNow]
const CLAIM_LUA = `
local id = ARGV[1]
if id == "" then
  local popped = redis.call('ZPOPMIN', KEYS[1])
  if #popped == 0 then return {'empty'} end
  id = popped[1]
else
  local removed = redis.call('ZREM', KEYS[1], id)
  if removed == 0 then
    local key = '${TASK_PREFIX}'..id
    if redis.call('EXISTS', key) == 0 then return {'not_found'} end
    local status = redis.call('HGET', key, 'status')
    local claimedBy = redis.call('HGET', key, 'claimedBy')
    return {'conflict', status or 'unknown', claimedBy or ''}
  end
end
local key = '${TASK_PREFIX}'..id
local expiry = tonumber(ARGV[3]) + tonumber(ARGV[4])
redis.call('HSET', key, 'status', 'claimed', 'claimedBy', ARGV[2], 'leaseExpiresAt', expiry, 'leaseMs', ARGV[4], 'updatedAt', ARGV[5])
redis.call('ZADD', KEYS[2], expiry, id)
return {'ok', id}
`;

// Guarded mutation by the claimant. mode: progress|complete|fail|requeue|release
// KEYS: [pendingZset, leasesZset]
// ARGV: [taskId, agent, mode, nowMs, isoNow, extra]
//   extra = progress message | result JSON | error text | reason
const MUTATE_LUA = `
local id = ARGV[1]
local key = '${TASK_PREFIX}'..id
if redis.call('EXISTS', key) == 0 then return {'not_found'} end
local status = redis.call('HGET', key, 'status')
local claimedBy = redis.call('HGET', key, 'claimedBy')
if status ~= 'claimed' and status ~= 'in_progress' then
  return {'conflict', status or 'unknown', claimedBy or ''}
end
if claimedBy ~= ARGV[2] then
  return {'not_claimant', status, claimedBy or ''}
end
local mode = ARGV[3]
local now = tonumber(ARGV[4])
if mode == 'progress' then
  local leaseMs = tonumber(redis.call('HGET', key, 'leaseMs') or '${DEFAULT_LEASE_SECONDS * 1000}')
  local expiry = now + leaseMs
  redis.call('HSET', key, 'status', 'in_progress', 'progress', ARGV[6], 'leaseExpiresAt', expiry, 'updatedAt', ARGV[5])
  redis.call('ZADD', KEYS[2], expiry, id)
elseif mode == 'complete' then
  redis.call('HSET', key, 'status', 'completed', 'result', ARGV[6], 'updatedAt', ARGV[5])
  redis.call('ZREM', KEYS[2], id)
elseif mode == 'fail' then
  redis.call('HSET', key, 'status', 'failed', 'error', ARGV[6], 'updatedAt', ARGV[5])
  redis.call('ZREM', KEYS[2], id)
elseif mode == 'requeue' or mode == 'release' then
  if mode == 'requeue' then
    redis.call('HSET', key, 'error', ARGV[6])
  end
  redis.call('HSET', key, 'status', 'pending', 'claimedBy', '', 'leaseExpiresAt', '', 'progress', '', 'updatedAt', ARGV[5])
  redis.call('HINCRBY', key, 'attempts', 1)
  local priority = tonumber(redis.call('HGET', key, 'priority') or '2')
  local createdAtMs = tonumber(redis.call('HGET', key, 'createdAtMs') or ARGV[4])
  redis.call('ZADD', KEYS[1], priority * 1e13 + createdAtMs, id)
  redis.call('ZREM', KEYS[2], id)
end
return {'ok', id}
`;

// Release tasks whose lease expired. Guards against a concurrent heartbeat by
// re-checking the hash's own leaseExpiresAt before releasing.
// KEYS: [leasesZset, pendingZset]  ARGV: [nowMs, isoNow]
const REAP_LUA = `
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
local released = {}
for _, id in ipairs(expired) do
  local key = '${TASK_PREFIX}'..id
  local status = redis.call('HGET', key, 'status')
  if status == 'claimed' or status == 'in_progress' then
    local exp = tonumber(redis.call('HGET', key, 'leaseExpiresAt') or '0')
    if exp > 0 and exp <= tonumber(ARGV[1]) then
      local prevAgent = redis.call('HGET', key, 'claimedBy') or ''
      redis.call('HSET', key, 'status', 'pending', 'claimedBy', '', 'leaseExpiresAt', '', 'progress', '', 'updatedAt', ARGV[2])
      redis.call('HINCRBY', key, 'attempts', 1)
      local priority = tonumber(redis.call('HGET', key, 'priority') or '2')
      local createdAtMs = tonumber(redis.call('HGET', key, 'createdAtMs') or ARGV[1])
      redis.call('ZADD', KEYS[2], priority * 1e13 + createdAtMs, id)
      redis.call('ZREM', KEYS[1], id)
      table.insert(released, id)
      table.insert(released, prevAgent)
    else
      -- Heartbeat won the race: resync the leases zset to the hash's expiry.
      redis.call('ZREM', KEYS[1], id)
      if exp > 0 then redis.call('ZADD', KEYS[1], exp, id) end
    end
  else
    redis.call('ZREM', KEYS[1], id)
  end
end
return released
`;

declare module "ioredis" {
  interface RedisCommander {
    mcsClaim(pending: string, leases: string, ...args: (string | number)[]): Promise<string[]>;
    mcsMutate(pending: string, leases: string, ...args: (string | number)[]): Promise<string[]>;
    mcsReap(leases: string, pending: string, ...args: (string | number)[]): Promise<string[]>;
  }
}

export class TaskService {
  private reaperTimer?: NodeJS.Timeout;

  constructor(
    private readonly redis: Redis,
    private readonly streams: StreamService,
  ) {
    redis.defineCommand("mcsClaim", { numberOfKeys: 2, lua: CLAIM_LUA });
    redis.defineCommand("mcsMutate", { numberOfKeys: 2, lua: MUTATE_LUA });
    redis.defineCommand("mcsReap", { numberOfKeys: 2, lua: REAP_LUA });
  }

  async create(input: {
    title: string;
    description?: string;
    protocol?: string;
    priority?: number;
    payload?: Record<string, unknown>;
    correlationId?: string;
    createdBy: string;
  }): Promise<Task> {
    const id = `t_${nanoid(10)}`;
    const now = Date.now();
    const iso = new Date(now).toISOString();
    const priority = input.priority ?? 2;

    const record: Record<string, string | number> = {
      id,
      title: input.title,
      priority,
      status: "pending",
      createdBy: input.createdBy,
      createdAt: iso,
      createdAtMs: now,
      updatedAt: iso,
      attempts: 0,
    };
    if (input.description) record.description = input.description;
    if (input.protocol) record.protocol = input.protocol;
    if (input.payload) record.payload = JSON.stringify(input.payload);
    if (input.correlationId) record.correlationId = input.correlationId;

    await this.redis
      .multi()
      .hset(keys.task(id), record)
      .sadd(keys.tasksIndex, id)
      .zadd(keys.tasksPending, pendingScore(priority, now), id)
      .exec();

    const task = (await this.get(id))!;
    await this.emit("task.created", task, input.createdBy);
    return task;
  }

  async claim(input: { taskId?: string; agent: string; leaseSeconds?: number }): Promise<ClaimOutcome> {
    const leaseMs = Math.min(Math.max(input.leaseSeconds ?? DEFAULT_LEASE_SECONDS, 10), 3600) * 1000;
    const now = Date.now();
    const res = await this.redis.mcsClaim(
      keys.tasksPending,
      keys.tasksLeases,
      input.taskId ?? "",
      input.agent,
      now,
      leaseMs,
      new Date(now).toISOString(),
    );
    const [status] = res;
    if (status === "empty") return { claimed: false, reason: "queue_empty" };
    if (status === "not_found") return { claimed: false, reason: "not_found" };
    if (status === "conflict") {
      return { claimed: false, reason: "conflict", status: res[1] ?? "unknown", claimedBy: res[2] || undefined };
    }
    const task = (await this.get(res[1]!))!;
    await this.emit("task.claimed", task, input.agent);
    return { claimed: true, task };
  }

  async progress(input: { taskId: string; agent: string; message: string; percent?: number }): Promise<Task> {
    await this.guardedMutate(input.taskId, input.agent, "progress", input.message);
    if (input.percent !== undefined) {
      await this.redis.hset(keys.task(input.taskId), "progressPercent", input.percent);
    }
    const task = (await this.get(input.taskId))!;
    await this.emit("task.progress", task, input.agent, { message: input.message, percent: input.percent });
    return task;
  }

  async complete(input: { taskId: string; agent: string; result?: Record<string, unknown> }): Promise<Task> {
    await this.guardedMutate(input.taskId, input.agent, "complete", JSON.stringify(input.result ?? {}));
    const task = (await this.get(input.taskId))!;
    await this.emit("task.completed", task, input.agent);
    return task;
  }

  async fail(input: { taskId: string; agent: string; error: string; requeue?: boolean }): Promise<Task> {
    await this.guardedMutate(input.taskId, input.agent, input.requeue ? "requeue" : "fail", input.error);
    const task = (await this.get(input.taskId))!;
    await this.emit("task.failed", task, input.agent, { requeued: !!input.requeue, error: input.error });
    return task;
  }

  async release(input: { taskId: string; agent: string; reason?: string }): Promise<Task> {
    await this.guardedMutate(input.taskId, input.agent, "release", input.reason ?? "");
    const task = (await this.get(input.taskId))!;
    await this.emit("task.released", task, input.agent, { reason: input.reason });
    return task;
  }

  private async guardedMutate(taskId: string, agent: string, mode: string, extra: string): Promise<void> {
    const now = Date.now();
    const res = await this.redis.mcsMutate(
      keys.tasksPending,
      keys.tasksLeases,
      taskId,
      agent,
      mode,
      now,
      new Date(now).toISOString(),
      extra,
    );
    const [status] = res;
    if (status === "not_found") throw new TaskError(`task ${taskId} not found`);
    if (status === "conflict") throw new TaskError(`task ${taskId} is ${res[1]}, not claimable-mutable`);
    if (status === "not_claimant") {
      throw new TaskError(`task ${taskId} is claimed by ${res[2] || "another agent"}, not ${agent}`);
    }
  }

  /** Release tasks with expired leases. Returns the released task ids. */
  async reapExpired(): Promise<Array<{ taskId: string; previousAgent: string }>> {
    const now = Date.now();
    const flat = await this.redis.mcsReap(keys.tasksLeases, keys.tasksPending, now, new Date(now).toISOString());
    const released: Array<{ taskId: string; previousAgent: string }> = [];
    for (let i = 0; i + 1 < flat.length; i += 2) {
      released.push({ taskId: flat[i]!, previousAgent: flat[i + 1]! });
    }
    for (const { taskId, previousAgent } of released) {
      const task = await this.get(taskId);
      if (task) {
        await this.emit("task.expired", task, "system", { previousAgent });
      }
    }
    return released;
  }

  startReaper(intervalMs = 10_000): void {
    this.reaperTimer = setInterval(() => {
      this.reapExpired().catch((err) => console.error("[tasks] reaper error:", err));
    }, intervalMs);
    this.reaperTimer.unref();
  }

  stopReaper(): void {
    if (this.reaperTimer) clearInterval(this.reaperTimer);
  }

  async get(id: string): Promise<Task | undefined> {
    const raw = await this.redis.hgetall(keys.task(id));
    if (!raw.id) return undefined;
    return hydrate(raw);
  }

  async list(input: { status?: TaskStatus; limit?: number } = {}): Promise<Task[]> {
    const limit = Math.min(Math.max(input.limit ?? 50, 1), 500);
    const ids = await this.redis.smembers(keys.tasksIndex);
    if (ids.length === 0) return [];
    const pipeline = this.redis.pipeline();
    for (const id of ids) pipeline.hgetall(keys.task(id));
    const results = (await pipeline.exec()) ?? [];
    const tasks: Task[] = [];
    for (const [err, raw] of results) {
      if (err || !raw || !(raw as Record<string, string>).id) continue;
      const task = hydrate(raw as Record<string, string>);
      if (!input.status || task.status === input.status) tasks.push(task);
    }
    tasks.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
    return tasks.slice(0, limit);
  }

  async queueSummary(): Promise<{
    counts: Record<TaskStatus, number>;
    pending: Task[];
    active: Task[];
  }> {
    const all = await this.list({ limit: 500 });
    const counts: Record<TaskStatus, number> = {
      pending: 0,
      claimed: 0,
      in_progress: 0,
      completed: 0,
      failed: 0,
    };
    for (const t of all) counts[t.status] += 1;
    return {
      counts,
      pending: all.filter((t) => t.status === "pending"),
      active: all.filter((t) => t.status === "claimed" || t.status === "in_progress"),
    };
  }

  /** Task lifecycle changes are ordinary events on the system tasks stream. */
  private async emit(
    type: string,
    task: Task,
    source: string,
    extra: Record<string, unknown> = {},
  ): Promise<void> {
    await this.streams.publish({
      stream: SYSTEM_STREAMS.tasks,
      type,
      source,
      ...(task.correlationId ? { correlationId: task.correlationId } : {}),
      payload: { taskId: task.id, title: task.title, status: task.status, claimedBy: task.claimedBy, ...extra },
    });
  }
}

export class TaskError extends Error {}

function hydrate(raw: Record<string, string>): Task {
  const task: Task = {
    id: raw.id!,
    title: raw.title ?? "",
    priority: Number(raw.priority ?? 2),
    status: (raw.status as TaskStatus) ?? "pending",
    createdBy: raw.createdBy ?? "unknown",
    createdAt: raw.createdAt ?? "",
    updatedAt: raw.updatedAt ?? "",
    attempts: Number(raw.attempts ?? 0),
  };
  if (raw.description) task.description = raw.description;
  if (raw.protocol) task.protocol = raw.protocol;
  if (raw.correlationId) task.correlationId = raw.correlationId;
  if (raw.claimedBy) task.claimedBy = raw.claimedBy;
  if (raw.leaseExpiresAt) task.leaseExpiresAt = new Date(Number(raw.leaseExpiresAt)).toISOString();
  if (raw.progress) task.progress = raw.progress;
  if (raw.progressPercent) task.progressPercent = Number(raw.progressPercent);
  if (raw.payload) {
    try {
      task.payload = JSON.parse(raw.payload);
    } catch {
      /* ignore malformed payload */
    }
  }
  if (raw.result) {
    try {
      task.result = JSON.parse(raw.result);
    } catch {
      /* ignore */
    }
  }
  if (raw.error) task.error = raw.error;
  return task;
}
