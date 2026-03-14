/**
 * ClaudeAdapterLive - Effect-based Claude Code provider adapter layer.
 *
 * Follows the same architecture as CodexAdapterLive: wraps a manager class
 * (ClaudeCodeManager) that emits ProviderEvent objects, maps them to canonical
 * ProviderRuntimeEvent objects, and enqueues them on a shared queue exposed
 * as `streamEvents`.
 *
 * @module ClaudeAdapterLive
 */
import type {
  ProviderEvent,
  ProviderKind,
  ProviderRuntimeEvent,
  RuntimeEventRawSource,
  ThreadId,
} from "@t3tools/contracts";
import { EventId, RuntimeItemId, RuntimeRequestId } from "@t3tools/contracts";
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

// ── Helpers (mirroring CodexAdapter helpers) ────────────────────────

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
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

// ── Event mapping (ProviderEvent → ProviderRuntimeEvent) ────────────
//
// Follows the same structure as the Codex adapter's mapToRuntimeEvents.
// The Claude manager emits ProviderEvent objects with methods like
// "session/connecting", "turn/started", "item/started", etc. that
// mirror the Codex event methods.

function eventRawSource(event: ProviderEvent): RuntimeEventRawSource {
  return event.kind === "request" ? "claude.sdk.stream-event" : "claude.sdk.message";
}

function runtimeEventBase(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): Omit<ProviderRuntimeEvent, "type" | "payload"> {
  const refs: Record<string, string> = {};
  if (event.turnId) refs.providerTurnId = event.turnId;
  if (event.itemId) refs.providerItemId = event.itemId;
  if (event.requestId) refs.providerRequestId = event.requestId;

  const providerRefs =
    Object.keys(refs).length > 0 ? (refs as ProviderRuntimeEvent["providerRefs"]) : undefined;

  return {
    eventId: EventId.makeUnsafe(event.id),
    provider: event.provider,
    threadId: canonicalThreadId,
    createdAt: event.createdAt,
    ...(event.turnId ? { turnId: event.turnId } : {}),
    ...(event.itemId ? { itemId: RuntimeItemId.makeUnsafe(event.itemId) } : {}),
    ...(event.requestId ? { requestId: RuntimeRequestId.makeUnsafe(event.requestId) } : {}),
    ...(providerRefs ? { providerRefs } : {}),
    raw: {
      source: eventRawSource(event),
      method: event.method,
      payload: event.payload ?? {},
    },
  };
}

function mapClaudeToRuntimeEvents(
  event: ProviderEvent,
  canonicalThreadId: ThreadId,
): ReadonlyArray<ProviderRuntimeEvent> {
  const payload = asObject(event.payload);

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
    const detail = asString(payload?.toolName) ?? asString(payload?.reason);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "request.opened",
        payload: {
          requestType: event.method.includes("fileChange")
            ? "file_change_approval"
            : "command_execution_approval",
          ...(detail ? { detail } : {}),
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
        payload: { state: "starting", ...(event.message ? { reason: event.message } : {}) },
      },
    ];
  }

  if (event.method === "session/ready") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "session.state.changed",
        payload: { state: "ready", ...(event.message ? { reason: event.message } : {}) },
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

  // ── Turn lifecycle ──────────────────────────────────────────────

  if (event.method === "turn/started") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "turn.started",
        payload: {},
      },
    ];
  }

  if (event.method === "turn/completed") {
    const turn = asObject(payload?.turn);
    const status = asString(turn?.status);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "turn.completed",
        payload: {
          state: status === "failed" ? "failed" : "completed",
          ...(asString(asObject(turn?.error)?.message)
            ? { errorMessage: asString(asObject(turn?.error)?.message) }
            : {}),
        },
      },
    ];
  }

  // ── Item lifecycle ──────────────────────────────────────────────

  if (event.method === "item/started") {
    const item = asObject(payload?.item);
    const itemType = asString(item?.type);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "item.started",
        payload: {
          itemType:
            itemType === "agentMessage"
              ? "assistant_message"
              : itemType === "toolUse"
                ? "command_execution"
                : "unknown",
          status: "inProgress",
          ...(asString(item?.tool) ? { title: asString(item?.tool) } : {}),
          ...(item ? { data: item } : {}),
        },
      },
    ];
  }

  if (event.method === "item/completed") {
    const item = asObject(payload?.item);
    const itemType = asString(item?.type);
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "item.completed",
        payload: {
          itemType:
            itemType === "agentMessage"
              ? "assistant_message"
              : itemType === "toolUse"
                ? "command_execution"
                : "unknown",
          status: "completed",
          ...(item ? { data: item } : {}),
        },
      },
    ];
  }

  // ── Content deltas ──────────────────────────────────────────────

  if (event.method === "item/agentMessage/delta") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "content.delta",
        payload: {
          streamKind: "assistant_text",
          delta: event.textDelta ?? asString(payload?.delta) ?? "",
        },
      },
    ];
  }

  if (event.method === "assistant/thinking") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "content.delta",
        payload: {
          streamKind: "reasoning_text",
          delta: event.textDelta ?? asString(payload?.thinking) ?? asString(payload?.text) ?? "",
        },
      },
    ];
  }

  // ── Tool progress / summary ─────────────────────────────────────

  if (event.method === "tool_use/progress") {
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "tool.progress",
        payload: {
          toolName: asString(payload?.toolName),
        },
      },
    ];
  }

  if (event.method === "tool_use/summary") {
    const summary = asString(payload?.summary);
    if (!summary) return [];
    const rawIds = payload?.toolUseIds;
    return [
      {
        ...runtimeEventBase(event, canonicalThreadId),
        type: "tool.summary",
        payload: {
          summary,
          ...(Array.isArray(rawIds)
            ? { precedingToolUseIds: rawIds.filter((id): id is string => typeof id === "string") }
            : {}),
        },
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
