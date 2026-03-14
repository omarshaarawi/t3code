/**
 * ClaudeAdapterLive - Effect-based Claude Code provider adapter layer.
 *
 * Follows the same architecture as CodexAdapterLive: wraps a manager class
 * (ClaudeCodeManager) that emits ProviderEvent objects, maps them to canonical
 * ProviderRuntimeEvent objects, and enqueues them on a shared queue exposed
 * as `streamEvents`.
 *
 * Event mapping mirrors the Codex adapter's `mapToRuntimeEvents` with full
 * feature parity for tool lifecycle, thinking, usage, diffs, tasks, and hooks.
 *
 * @module ClaudeAdapterLive
 */
import type {
  CanonicalItemType,
  ProviderEvent,
  ProviderKind,
  ProviderRuntimeEvent,
  RuntimeEventRawSource,
  ThreadId,
} from "@t3tools/contracts";
import { EventId, RuntimeItemId, RuntimeRequestId, RuntimeTaskId } from "@t3tools/contracts";
import { Effect, Layer, Queue, Stream } from "effect";

import { ClaudeCodeManager } from "../../claudeCodeManager.ts";
import {
  ProviderAdapterProcessError,
  ProviderAdapterRequestError,
  ProviderAdapterValidationError,
} from "../Errors.ts";
import type { ClaudeAdapterShape } from "../Services/ClaudeAdapter.ts";
import { ClaudeAdapter } from "../Services/ClaudeAdapter.ts";

const PROVIDER: ProviderKind = "claude";

// ── Helpers ─────────────────────────────────────────────────────────

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

function toMessage(cause: unknown, fallback: string): string {
  if (cause instanceof Error) return cause.message;
  if (typeof cause === "string") return cause;
  return fallback;
}

function toRequestError(
  threadId: ThreadId,
  method: string,
  cause: unknown,
): ProviderAdapterRequestError {
  return new ProviderAdapterRequestError({
    provider: PROVIDER,
    method,
    detail: toMessage(cause, `Claude adapter request failed: ${method}`),
    cause,
  });
}

function toTurnStatus(value: unknown): "completed" | "failed" | "cancelled" | "interrupted" {
  switch (value) {
    case "completed":
    case "failed":
    case "cancelled":
    case "interrupted":
      return value;
    default:
      return "completed";
  }
}

// ── Event mapping helpers ───────────────────────────────────────────

function eventRawSource(event: ProviderEvent): RuntimeEventRawSource {
  return event.kind === "request" ? "claude.sdk.stream-event" : "claude.sdk.message";
}

function providerRefsFromEvent(
  event: ProviderEvent,
): ProviderRuntimeEvent["providerRefs"] | undefined {
  const refs: Record<string, string> = {};
  if (event.turnId) refs.providerTurnId = event.turnId;
  if (event.itemId) refs.providerItemId = event.itemId;
  if (event.requestId) refs.providerRequestId = event.requestId;
  return Object.keys(refs).length > 0 ? (refs as ProviderRuntimeEvent["providerRefs"]) : undefined;
}

function asRuntimeItemId(id: string): ProviderRuntimeEvent["itemId"] {
  return RuntimeItemId.makeUnsafe(id);
}

function asRuntimeRequestId(id: string): ProviderRuntimeEvent["requestId"] {
  return RuntimeRequestId.makeUnsafe(id);
}

function runtimeEventBase(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  const refs = providerRefsFromEvent(event);
  return {
    eventId: EventId.makeUnsafe(event.id),
    provider: event.provider,
    threadId: canonicalThreadId,
    createdAt: event.createdAt,
    ...(event.turnId ? { turnId: event.turnId } : {}),
    ...(event.itemId ? { itemId: asRuntimeItemId(event.itemId) } : {}),
    ...(event.requestId ? { requestId: asRuntimeRequestId(event.requestId) } : {}),
    ...(refs ? { providerRefs: refs } : {}),
    raw: {
      source: eventRawSource(event),
      method: event.method,
      payload: event.payload ?? {},
    },
  };
}

// ── Content delta stream kind resolution ────────────────────────────
//
// Mirrors the Codex adapter's contentStreamKindFromMethod.

function contentStreamKindFromMethod(
  method: string,
):
  | "assistant_text"
  | "reasoning_text"
  | "reasoning_summary_text"
  | "command_output"
  | "file_change_output" {
  switch (method) {
    case "item/agentMessage/delta":
      return "assistant_text";
    case "item/reasoning/textDelta":
      return "reasoning_text";
    case "item/reasoning/summaryTextDelta":
      return "reasoning_summary_text";
    case "item/commandExecution/outputDelta":
      return "command_output";
    case "item/fileChange/outputDelta":
      return "file_change_output";
    default:
      return "assistant_text";
  }
}

// ── Item lifecycle mapping ──────────────────────────────────────────
//
// Mirrors the Codex adapter's mapItemLifecycle.

function mapItemLifecycle(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
  lifecycle: "item.started" | "item.updated" | "item.completed",
): ProviderRuntimeEvent | undefined {
  const payload = asObject(event.payload);
  const item = asObject(payload?.item);
  const source = item ?? payload;
  if (!source) return undefined;

  const itemType = (asString(source.itemType) ??
    asString(source.type) ??
    "unknown") as CanonicalItemType;
  const title = asString(payload?.title) ?? asString(source.title);
  const detail = asString(payload?.detail) ?? asString(source.detail);

  const status =
    lifecycle === "item.started"
      ? "inProgress"
      : lifecycle === "item.completed"
        ? "completed"
        : undefined;

  return {
    ...runtimeEventBase(event, canonicalThreadId),
    type: lifecycle,
    payload: {
      itemType,
      ...(status ? { status } : {}),
      ...(title ? { title } : {}),
      ...(detail ? { detail } : {}),
      ...(event.payload !== undefined ? { data: event.payload } : {}),
    },
  };
}

// ── Main event mapper ───────────────────────────────────────────────
//
// Maps ProviderEvent → ProviderRuntimeEvent[].
// Mirrors the Codex adapter's mapToRuntimeEvents with full parity.

function mapClaudeToRuntimeEvents(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): ReadonlyArray<ProviderRuntimeEvent> {
  const payload = asObject(event.payload);
  const turn = asObject(payload?.turn);

  // ── Error events ────────────────────────────────────────────────

  if (event.kind === "error") {
    if (!event.message) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "runtime.error",
        payload: {
          message: event.message,
          class: "provider_error",
          ...(event.payload !== undefined ? { detail: event.payload } : {}),
        },
      },
    ];
  }

  // ── Approval request events ─────────────────────────────────────

  if (event.kind === "request") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "request.opened",
        payload: {
          requestType: event.method.includes("fileChange")
            ? "file_change_approval"
            : event.method.includes("commandExecution")
              ? "command_execution_approval"
              : "unknown",
          ...(asString(payload?.toolName) ? { detail: asString(payload?.toolName) } : {}),
          ...(event.payload !== undefined ? { args: event.payload } : {}),
        },
      },
    ];
  }

  // ── Approval decision events ────────────────────────────────────

  if (event.method === "item/requestApproval/decision" && event.requestId) {
    const decision = asString(payload?.decision);
    const requestKind = asString(payload?.requestKind);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "request.resolved",
        payload: {
          requestType:
            requestKind === "file-change" ? "file_change_approval" : "command_execution_approval",
          ...(decision ? { decision } : {}),
          ...(event.payload !== undefined ? { resolution: event.payload } : {}),
        },
      },
    ];
  }

  // ── Session lifecycle ───────────────────────────────────────────

  if (event.method === "session/connecting") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "session.state.changed",
        payload: {
          state: "starting",
          ...(event.message ? { reason: event.message } : {}),
        },
      },
    ];
  }

  if (event.method === "session/ready") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "session.state.changed",
        payload: {
          state: "ready",
          ...(event.message ? { reason: event.message } : {}),
        },
      },
    ];
  }

  if (event.method === "session/started") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "session.started",
        payload: {
          ...(event.message ? { message: event.message } : {}),
          ...(event.payload !== undefined ? { resume: event.payload } : {}),
        },
      },
    ];
  }

  if (event.method === "session/exited" || event.method === "session/closed") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "session.exited",
        payload: {
          ...(event.message ? { reason: event.message } : {}),
          ...(event.method === "session/closed" ? { exitKind: "graceful" } : {}),
        },
      },
    ];
  }

  if (event.method === "session/startFailed") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "runtime.error",
        payload: {
          message: event.message ?? "Failed to start Claude session.",
          class: "provider_error",
        },
      },
    ];
  }

  // ── Thread lifecycle ────────────────────────────────────────────

  if (event.method === "thread/started") {
    const thread = asObject(payload?.thread);
    const providerThreadId = asString(thread?.id) ?? asString(payload?.threadId);
    if (!providerThreadId) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "thread.started",
        payload: { providerThreadId },
      },
    ];
  }

  if (event.method === "thread/compacted") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "thread.state.changed",
        payload: {
          state: "compacted",
          ...(event.payload !== undefined ? { data: event.payload } : {}),
        },
      },
    ];
  }

  // ── Thread metadata ─────────────────────────────────────────────

  if (event.method === "thread/name/updated") {
    return [
      {
        type: "thread.metadata.updated",
        ...runtimeEventBase(event, canonicalThreadId),
        payload: {
          ...(asString(payload?.threadName) ? { name: asString(payload?.threadName) } : {}),
          ...(event.payload !== undefined ? { metadata: asObject(event.payload) } : {}),
        },
      },
    ];
  }

  // ── Token usage ─────────────────────────────────────────────────

  if (event.method === "thread/tokenUsage/updated") {
    return [
      {
        type: "thread.token-usage.updated",
        ...runtimeEventBase(event, canonicalThreadId),
        payload: {
          usage: event.payload ?? {},
        },
      },
    ];
  }

  // ── Turn lifecycle ──────────────────────────────────────────────

  if (event.method === "turn/started") {
    const turnId = event.turnId;
    if (!turnId) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        turnId,
        type: "turn.started",
        payload: {
          ...(asString(turn?.model) ? { model: asString(turn?.model) } : {}),
          ...(asString(turn?.effort) ? { effort: asString(turn?.effort) } : {}),
        },
      },
    ];
  }

  if (event.method === "turn/completed") {
    const errorMessage = asString(asObject(turn?.error)?.message);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "turn.completed",
        payload: {
          state: toTurnStatus(turn?.status),
          ...(asString(turn?.stopReason) ? { stopReason: asString(turn?.stopReason) } : {}),
          ...(turn?.usage !== undefined ? { usage: turn.usage } : {}),
          ...(asObject(turn?.modelUsage) ? { modelUsage: asObject(turn?.modelUsage) } : {}),
          ...(asNumber(turn?.totalCostUsd) !== undefined
            ? { totalCostUsd: asNumber(turn?.totalCostUsd) }
            : {}),
          ...(errorMessage ? { errorMessage } : {}),
        },
      },
    ];
  }

  if (event.method === "turn/aborted") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "turn.aborted",
        payload: {
          reason: event.message ?? "Turn aborted",
        },
      },
    ];
  }

  // ── Turn diff ───────────────────────────────────────────────────

  if (event.method === "turn/diff/updated") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "turn.diff.updated",
        payload: {
          unifiedDiff:
            asString(payload?.unifiedDiff) ??
            asString(payload?.diff) ??
            asString(payload?.patch) ??
            "",
        },
      },
    ];
  }

  // ── Item lifecycle ──────────────────────────────────────────────

  if (event.method === "item/started") {
    const started = mapItemLifecycle(event, canonicalThreadId, "item.started");
    return started ? [started] : [];
  }

  if (event.method === "item/completed") {
    const completed = mapItemLifecycle(event, canonicalThreadId, "item.completed");
    return completed ? [completed] : [];
  }

  // ── Content deltas ──────────────────────────────────────────────

  if (
    event.method === "item/agentMessage/delta" ||
    event.method === "item/commandExecution/outputDelta" ||
    event.method === "item/fileChange/outputDelta" ||
    event.method === "item/reasoning/summaryTextDelta" ||
    event.method === "item/reasoning/textDelta"
  ) {
    const delta =
      event.textDelta ??
      asString(payload?.delta) ??
      asString(payload?.text) ??
      asString(asObject(payload?.content)?.text);
    if (!delta || delta.length === 0) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "content.delta",
        payload: {
          streamKind: contentStreamKindFromMethod(event.method),
          delta,
        },
      },
    ];
  }

  // ── Tool progress ───────────────────────────────────────────────

  if (event.method === "tool_use/progress") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "tool.progress",
        payload: {
          toolUseId: asString(payload?.toolUseId),
          toolName: asString(payload?.toolName),
          elapsedSeconds: asNumber(payload?.elapsedSeconds),
        },
      },
    ];
  }

  // ── Tool summary ────────────────────────────────────────────────

  if (event.method === "tool_use/summary") {
    const summary = asString(payload?.summary);
    if (!summary) return [];
    const rawIds = payload?.precedingToolUseIds;
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "tool.summary",
        payload: {
          summary,
          ...(Array.isArray(rawIds)
            ? {
                precedingToolUseIds: rawIds.filter((id): id is string => typeof id === "string"),
              }
            : {}),
        },
      },
    ];
  }

  // ── Task lifecycle (subagent tasks) ─────────────────────────────

  if (event.method === "task/started") {
    const taskId = asString(payload?.taskId);
    if (!taskId) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "task.started",
        payload: {
          taskId: RuntimeTaskId.makeUnsafe(taskId),
          ...(asString(payload?.description)
            ? { description: asString(payload?.description) }
            : {}),
          ...(asString(payload?.taskType) ? { taskType: asString(payload?.taskType) } : {}),
        },
      },
    ];
  }

  if (event.method === "task/progress") {
    const taskId = asString(payload?.taskId);
    if (!taskId) return [];
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "task.progress",
        payload: {
          taskId: RuntimeTaskId.makeUnsafe(taskId),
          description: asString(payload?.description) ?? "Task in progress",
          ...(asString(payload?.summary) ? { summary: asString(payload?.summary) } : {}),
          ...(payload?.usage !== undefined ? { usage: payload.usage } : {}),
          ...(asString(payload?.lastToolName)
            ? { lastToolName: asString(payload?.lastToolName) }
            : {}),
        },
      },
    ];
  }

  if (event.method === "task/completed") {
    const taskId = asString(payload?.taskId);
    if (!taskId) return [];
    const rawStatus = asString(payload?.status);
    const status: "completed" | "failed" | "stopped" =
      rawStatus === "failed" ? "failed" : rawStatus === "stopped" ? "stopped" : "completed";
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "task.completed",
        payload: {
          taskId: RuntimeTaskId.makeUnsafe(taskId),
          status,
          ...(asString(payload?.summary) ? { summary: asString(payload?.summary) } : {}),
          ...(payload?.usage !== undefined ? { usage: payload.usage } : {}),
        },
      },
    ];
  }

  // ── Hook lifecycle ──────────────────────────────────────────────

  if (event.method === "hook/started") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "hook.started",
        payload: {
          hookId: asString(payload?.hookId) ?? "",
          hookName: asString(payload?.hookName) ?? "",
          hookEvent: asString(payload?.hookEvent) ?? "",
        },
      },
    ];
  }

  if (event.method === "hook/progress") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "hook.progress",
        payload: {
          hookId: asString(payload?.hookId) ?? "",
          ...(asString(payload?.output) ? { output: asString(payload?.output) } : {}),
          ...(asString(payload?.stdout) ? { stdout: asString(payload?.stdout) } : {}),
          ...(asString(payload?.stderr) ? { stderr: asString(payload?.stderr) } : {}),
        },
      },
    ];
  }

  if (event.method === "hook/completed") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "hook.completed",
        payload: {
          hookId: asString(payload?.hookId) ?? "",
          outcome: (asString(payload?.outcome) as "success" | "error" | "cancelled") ?? "success",
          ...(asString(payload?.output) ? { output: asString(payload?.output) } : {}),
          ...(asString(payload?.stdout) ? { stdout: asString(payload?.stdout) } : {}),
          ...(asString(payload?.stderr) ? { stderr: asString(payload?.stderr) } : {}),
          ...(typeof payload?.exitCode === "number"
            ? { exitCode: payload.exitCode as number }
            : {}),
        },
      },
    ];
  }

  // ── Auth status ─────────────────────────────────────────────────

  if (event.method === "auth/status") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "auth.status",
        payload: {
          ...(typeof payload?.isAuthenticating === "boolean"
            ? { isAuthenticating: payload.isAuthenticating }
            : {}),
          ...(Array.isArray(payload?.output) ? { output: payload.output } : {}),
          ...(asString(payload?.error) ? { error: asString(payload?.error) } : {}),
        },
      },
    ];
  }

  // ── Account / rate limits ───────────────────────────────────────

  if (event.method === "account/rateLimits/updated") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "account.rate-limits.updated",
        payload: { rateLimits: event.payload ?? {} },
      },
    ];
  }

  // ── Files persisted ─────────────────────────────────────────────

  if (event.method === "files/persisted") {
    const filesPayload = asObject(event.payload);
    const files = Array.isArray(filesPayload?.files)
      ? (filesPayload.files as Array<{ filename: string; file_id: string }>).map((f) => ({
          filename: f.filename,
          fileId: f.file_id,
        }))
      : [];
    const failed = Array.isArray(filesPayload?.failed)
      ? (filesPayload.failed as Array<{ filename: string; error: string }>)
      : undefined;
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "files.persisted",
        payload: { files, ...(failed ? { failed } : {}) },
      },
    ];
  }

  // Unhandled event method -- drop it
  return [];
}

// ── Adapter factory ─────────────────────────────────────────────────

export interface ClaudeAdapterLiveOptions {
  readonly manager?: ClaudeCodeManager;
}

const makeClaudeAdapter = (options?: ClaudeAdapterLiveOptions) =>
  Effect.gen(function* () {
    const manager = yield* Effect.acquireRelease(
      Effect.sync(() => options?.manager ?? new ClaudeCodeManager()),
      (manager) =>
        Effect.sync(() => {
          try {
            manager.stopAll();
          } catch {
            /* finalizers must not throw */
          }
        }),
    );

    // ── Adapter methods ─────────────────────────────────────────────

    const startSession: ClaudeAdapterShape["startSession"] = (input) => {
      if (input.provider !== undefined && input.provider !== PROVIDER) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "startSession",
            issue: `Expected provider '${PROVIDER}' but received '${input.provider}'.`,
          }),
        );
      }

      return Effect.tryPromise({
        try: () => manager.startSession(input),
        catch: (cause) =>
          new ProviderAdapterProcessError({
            provider: PROVIDER,
            threadId: input.threadId,
            detail: toMessage(cause, "Failed to start Claude adapter session."),
            cause,
          }),
      });
    };

    const sendTurn: ClaudeAdapterShape["sendTurn"] = (input) =>
      Effect.tryPromise({
        try: () => manager.sendTurn(input),
        catch: (cause) => toRequestError(input.threadId, "turn/start", cause),
      });

    const interruptTurn: ClaudeAdapterShape["interruptTurn"] = (threadId, turnId) =>
      Effect.tryPromise({
        try: () => manager.interruptTurn(threadId, turnId),
        catch: (cause) => toRequestError(threadId, "turn/interrupt", cause),
      });

    const readThread: ClaudeAdapterShape["readThread"] = (threadId) =>
      Effect.try({
        try: () => manager.readThread(threadId),
        catch: (cause) => toRequestError(threadId, "thread/read", cause),
      });

    const rollbackThread: ClaudeAdapterShape["rollbackThread"] = (threadId, numTurns) => {
      if (!Number.isInteger(numTurns) || numTurns < 1) {
        return Effect.fail(
          new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "rollbackThread",
            issue: "numTurns must be an integer >= 1.",
          }),
        );
      }
      return Effect.try({
        try: () => manager.rollbackThread(threadId, numTurns),
        catch: (cause) => toRequestError(threadId, "thread/rollback", cause),
      });
    };

    const respondToRequest: ClaudeAdapterShape["respondToRequest"] = (
      threadId,
      requestId,
      decision,
    ) =>
      Effect.try({
        try: () => manager.respondToRequest(threadId, requestId, decision),
        catch: (cause) => toRequestError(threadId, "item/requestApproval/decision", cause),
      });

    const respondToUserInput: ClaudeAdapterShape["respondToUserInput"] = (
      threadId,
      requestId,
      answers,
    ) =>
      Effect.try({
        try: () => manager.respondToUserInput(threadId, requestId, answers),
        catch: (cause) => toRequestError(threadId, "item/tool/requestUserInput", cause),
      });

    const stopSession: ClaudeAdapterShape["stopSession"] = (threadId) =>
      Effect.sync(() => manager.stopSession(threadId));

    const listSessions: ClaudeAdapterShape["listSessions"] = () =>
      Effect.sync(() => manager.listSessions());

    const hasSession: ClaudeAdapterShape["hasSession"] = (threadId) =>
      Effect.sync(() => manager.hasSession(threadId));

    const stopAll: ClaudeAdapterShape["stopAll"] = () => Effect.sync(() => manager.stopAll());

    // ── Event stream wiring ─────────────────────────────────────────

    const runtimeEventQueue = yield* Queue.unbounded<ProviderRuntimeEvent>();

    yield* Effect.acquireRelease(
      Effect.gen(function* () {
        const services = yield* Effect.services<never>();
        const listener = (event: ProviderEvent) =>
          Effect.gen(function* () {
            const runtimeEvents = mapClaudeToRuntimeEvents(event, event.threadId);
            if (runtimeEvents.length === 0) {
              yield* Effect.logDebug("ignoring unhandled Claude provider event", {
                method: event.method,
                threadId: event.threadId,
                turnId: event.turnId,
                itemId: event.itemId,
              });
              return;
            }
            yield* Queue.offerAll(runtimeEventQueue, runtimeEvents);
          }).pipe(Effect.runPromiseWith(services));
        manager.on("event", listener);
        return listener;
      }),
      (listener) =>
        Effect.gen(function* () {
          yield* Effect.sync(() => manager.off("event", listener));
          yield* Queue.shutdown(runtimeEventQueue);
        }),
    );

    return {
      provider: PROVIDER,
      capabilities: { sessionModelSwitch: "in-session" },
      startSession,
      sendTurn,
      interruptTurn,
      readThread,
      rollbackThread,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      stopAll,
      streamEvents: Stream.fromQueue(runtimeEventQueue),
    } satisfies ClaudeAdapterShape;
  });

export const ClaudeAdapterLive = Layer.effect(ClaudeAdapter, makeClaudeAdapter());

export function makeClaudeAdapterLive(options?: ClaudeAdapterLiveOptions) {
  return Layer.effect(ClaudeAdapter, makeClaudeAdapter(options));
}
