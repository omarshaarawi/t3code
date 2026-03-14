/**
 * ClaudeCodeManager - Claude Code session lifecycle manager.
 *
 * Wraps the `@anthropic-ai/claude-agent-sdk` and emits `ProviderEvent` objects
 * through an EventEmitter, following the same pattern as `CodexAppServerManager`.
 *
 * Ported from the theo/claude branch and adapted to emit events compatible with
 * the existing canonical event pipeline.
 *
 * @module ClaudeCodeManager
 */
import { randomUUID } from "node:crypto";
import { EventEmitter } from "node:events";

import {
  query as claudeQuery,
  type CanUseTool,
  type PermissionMode,
  type Query,
  type SDKMessage,
  type SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";
import {
  type ApprovalRequestId,
  type ProviderApprovalDecision,
  type ProviderApprovalPolicy,
  type ProviderKind,
  type ProviderRequestKind,
  type ProviderSendTurnInput,
  type ProviderSession,
  type ProviderSessionStartInput,
  type ProviderTurnStartResult,
  type ProviderUserInputAnswers,
  type ThreadId,
  type TurnId,
  ProviderItemId,
} from "@t3tools/contracts";
import { resolveModelSlugForProvider } from "@t3tools/shared/model";

import type {
  ProviderThreadSnapshot,
  ProviderThreadTurnSnapshot,
} from "./provider/Services/ProviderAdapter";

// Re-export the ProviderEvent type from contracts so the adapter layer can
// subscribe to events with the correct type.
type ProviderEvent = import("@t3tools/contracts").ProviderEvent;

const PROVIDER: ProviderKind = "claude";

// ── Internal types ──────────────────────────────────────────────────

interface PendingApprovalRequest {
  requestId: string;
  requestKind: ProviderRequestKind;
  toolName: string;
  resolve: (decision: ProviderApprovalDecision) => void;
}

interface QueuedTurnInput {
  message: SDKUserMessage;
}

interface ClaudeTurnState {
  turnId: string;
  assistantItemId: string;
  userContent: unknown[];
  assistantText: string;
  assistantStarted: boolean;
}

interface ClaudeSessionContext {
  session: ProviderSession;
  query: Query;
  queue: MessageQueue;
  abortController: AbortController;
  pendingApprovals: Map<string, PendingApprovalRequest>;
  turns: ProviderThreadTurnSnapshot[];
  approvalPolicy: ProviderApprovalPolicy;
  permissionMode: PermissionMode;
  allowAllForSession: boolean;
  stopping: boolean;
  currentTurn: ClaudeTurnState | null;
}

// ── Async message queue for multi-turn conversations ────────────────

interface MessageQueue {
  push: (input: QueuedTurnInput) => void;
  terminate: () => void;
  generator: AsyncIterable<SDKUserMessage>;
}

function createMessageQueue(): MessageQueue {
  const queue: QueuedTurnInput[] = [];
  let waiter: ((value: QueuedTurnInput | null) => void) | null = null;
  let terminated = false;

  return {
    push(input) {
      if (terminated) throw new Error("Claude input stream is closed.");
      if (waiter) {
        const resolve = waiter;
        waiter = null;
        resolve(input);
      } else {
        queue.push(input);
      }
    },
    terminate() {
      terminated = true;
      if (waiter) {
        const resolve = waiter;
        waiter = null;
        resolve(null);
      }
    },
    generator: (async function* () {
      while (!terminated) {
        const next = queue.shift();
        if (next) {
          yield next.message;
          continue;
        }
        const awaited = await new Promise<QueuedTurnInput | null>((resolve) => {
          waiter = resolve;
        });
        if (!awaited) break;
        yield awaited.message;
      }
    })(),
  };
}

// ── Helpers ─────────────────────────────────────────────────────────

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === "object" ? (value as Record<string, unknown>) : undefined;
}

function asString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function inferRequestKindForTool(toolName: string): ProviderRequestKind {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("file") ||
    normalized.includes("patch")
  ) {
    return "file-change";
  }
  return "command";
}

// ── Manager ─────────────────────────────────────────────────────────

export interface ClaudeCodeManagerEvents {
  event: [event: ProviderEvent];
}

export class ClaudeCodeManager extends EventEmitter<ClaudeCodeManagerEvents> {
  private readonly sessions = new Map<string, ClaudeSessionContext>();

  private emitProviderEvent(
    ctx: ClaudeSessionContext,
    kind: ProviderEvent["kind"],
    method: string,
    extra: Partial<Omit<ProviderEvent, "id" | "kind" | "provider" | "createdAt" | "method">> = {},
  ): void {
    this.emit("event", {
      id: randomUUID(),
      kind,
      provider: PROVIDER,
      createdAt: new Date().toISOString(),
      method,
      threadId: ctx.session.threadId,
      ...(ctx.currentTurn ? { turnId: ctx.currentTurn.turnId as TurnId } : {}),
      ...extra,
    } as ProviderEvent);
  }

  private updateSession(ctx: ClaudeSessionContext, updates: Partial<ProviderSession>): void {
    ctx.session = { ...ctx.session, ...updates, updatedAt: new Date().toISOString() };
  }

  private resolvePermissionMode(input: ProviderSessionStartInput): PermissionMode {
    if (input.runtimeMode === "full-access") return "bypassPermissions";
    return "default";
  }

  // ── Session lifecycle ─────────────────────────────────────────────

  async startSession(input: ProviderSessionStartInput): Promise<ProviderSession> {
    const threadId = input.threadId;
    const cwd = input.cwd ?? process.cwd();
    const model = resolveModelSlugForProvider("claude", input.model);
    const permissionMode = this.resolvePermissionMode(input);
    const claudeOptions = input.providerOptions?.claude;

    const session: ProviderSession = {
      provider: PROVIDER,
      status: "connecting",
      runtimeMode: input.runtimeMode,
      cwd,
      model,
      threadId,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
    };

    const abortController = new AbortController();
    const queue = createMessageQueue();

    const query = claudeQuery({
      prompt: queue.generator,
      options: {
        cwd,
        model,
        ...(claudeOptions?.binaryPath
          ? { pathToClaudeCodeExecutable: claudeOptions.binaryPath }
          : {}),
        permissionMode,
        allowDangerouslySkipPermissions: permissionMode === "bypassPermissions",
        ...(claudeOptions?.maxThinkingTokens != null
          ? { maxThinkingTokens: claudeOptions.maxThinkingTokens }
          : {}),
        ...(input.resumeCursor ? { resume: input.resumeCursor as string } : {}),
        includePartialMessages: true,
        abortController,
        canUseTool: (toolName, toolInput, options) =>
          this.handleCanUseTool(threadId as string, toolName, toolInput, options),
      },
    });

    const ctx: ClaudeSessionContext = {
      session,
      query,
      queue,
      abortController,
      pendingApprovals: new Map(),
      turns: [],
      approvalPolicy: input.approvalPolicy ?? "never",
      permissionMode,
      allowAllForSession: input.runtimeMode === "full-access",
      stopping: false,
      currentTurn: null,
    };
    this.sessions.set(threadId as string, ctx);

    this.emitProviderEvent(ctx, "session", "session/connecting", {
      message: "Starting Claude Code session",
    });
    this.consumeMessages(ctx);

    try {
      const initialization = await ctx.query.initializationResult();

      // The SDK may assign a different session ID. Store it as the
      // resume cursor so we can reconnect later, but keep the
      // canonical threadId unchanged -- the orchestration layer uses
      // that to route events back to the correct thread.
      const sessionId = asString(asObject(initialization)?.session_id);
      if (sessionId) {
        this.updateSession(ctx, { resumeCursor: sessionId });
      }

      this.updateSession(ctx, { status: "ready" });
      this.emitProviderEvent(ctx, "session", "session/ready", {
        message: `Connected to session ${ctx.session.threadId}`,
      });
      return { ...ctx.session };
    } catch (error) {
      const message = error instanceof Error ? error.message : "Failed to start Claude session.";
      this.updateSession(ctx, { status: "error", lastError: message });
      this.emitProviderEvent(ctx, "error", "session/startFailed", { message });
      this.stopSession(threadId);
      throw new Error(message, { cause: error });
    }
  }

  async sendTurn(input: ProviderSendTurnInput): Promise<ProviderTurnStartResult> {
    const ctx = this.requireSession(input.threadId as string);
    const turnId = randomUUID();
    const assistantItemId = randomUUID();
    const userContent: unknown[] = [];

    if (input.input) {
      userContent.push({ type: "text", text: input.input });
    }
    if (userContent.length === 0) {
      throw new Error("Turn input must include text.");
    }

    if (input.model) {
      const model = resolveModelSlugForProvider("claude", input.model);
      await ctx.query.setModel(model);
      this.updateSession(ctx, { model });
    }

    ctx.currentTurn = {
      turnId,
      assistantItemId,
      userContent,
      assistantText: "",
      assistantStarted: false,
    };

    this.updateSession(ctx, { status: "running", activeTurnId: turnId as TurnId });
    this.emitProviderEvent(ctx, "notification", "turn/started", {
      turnId: turnId as TurnId,
      payload: { turn: { id: turnId } },
    });

    const messageContent: Array<Record<string, unknown>> = [];
    if (input.input) {
      messageContent.push({ type: "text", text: input.input });
    }

    ctx.queue.push({
      message: {
        type: "user",
        session_id: (ctx.session.threadId ?? "") as string,
        parent_tool_use_id: null,
        message: { role: "user", content: messageContent },
      },
    });

    return {
      threadId: ctx.session.threadId,
      turnId: turnId as TurnId,
      resumeCursor: ctx.session.threadId,
    };
  }

  async interruptTurn(threadId: ThreadId, _turnId?: TurnId): Promise<void> {
    const ctx = this.requireSession(threadId as string);
    await ctx.query.interrupt();
  }

  readThread(threadId: ThreadId): ProviderThreadSnapshot {
    const ctx = this.requireSession(threadId as string);
    return { threadId: ctx.session.threadId, turns: [...ctx.turns] };
  }

  rollbackThread(threadId: ThreadId, numTurns: number): ProviderThreadSnapshot {
    const ctx = this.requireSession(threadId as string);
    if (!Number.isInteger(numTurns) || numTurns < 1) {
      throw new Error("numTurns must be an integer >= 1.");
    }
    ctx.turns = ctx.turns.slice(0, Math.max(0, ctx.turns.length - numTurns));
    this.updateSession(ctx, { status: "ready", activeTurnId: undefined });
    return { threadId: ctx.session.threadId, turns: [...ctx.turns] };
  }

  respondToRequest(
    threadId: ThreadId,
    requestId: ApprovalRequestId,
    decision: ProviderApprovalDecision,
  ): void {
    const ctx = this.requireSession(threadId as string);
    const pending = ctx.pendingApprovals.get(requestId as string);
    if (!pending) {
      throw new Error(`Unknown pending approval request: ${requestId}`);
    }
    ctx.pendingApprovals.delete(requestId as string);
    pending.resolve(decision);

    this.emitProviderEvent(ctx, "notification", "item/requestApproval/decision", {
      requestId,
      requestKind: pending.requestKind,
      payload: {
        requestId,
        requestKind: pending.requestKind,
        decision,
        toolName: pending.toolName,
      },
    });
  }

  respondToUserInput(
    _threadId: ThreadId,
    _requestId: ApprovalRequestId,
    _answers: ProviderUserInputAnswers,
  ): void {
    throw new Error("Claude Code does not support structured user input requests.");
  }

  stopSession(threadId: ThreadId): void {
    const ctx = this.sessions.get(threadId as string);
    if (!ctx) return;
    ctx.stopping = true;
    for (const pending of ctx.pendingApprovals.values()) pending.resolve("cancel");
    ctx.pendingApprovals.clear();
    ctx.queue.terminate();
    ctx.abortController.abort();
    ctx.query.close();
    this.updateSession(ctx, { status: "closed", activeTurnId: undefined });
    this.emitProviderEvent(ctx, "session", "session/closed", { message: "Session stopped" });
    this.sessions.delete(threadId as string);
  }

  listSessions(): ProviderSession[] {
    return Array.from(this.sessions.values(), ({ session }) => ({ ...session }));
  }

  hasSession(threadId: ThreadId): boolean {
    return this.sessions.has(threadId as string);
  }

  stopAll(): void {
    const threadIds = Array.from(this.sessions.keys());
    for (const threadId of threadIds) {
      this.stopSession(threadId as ThreadId);
    }
  }

  // ── SDK message consumer ──────────────────────────────────────────

  private async consumeMessages(ctx: ClaudeSessionContext): Promise<void> {
    try {
      for await (const message of ctx.query) {
        this.handleMessage(ctx, message);
      }
    } catch (error) {
      if (ctx.stopping) return;
      const message = error instanceof Error ? error.message : "Claude session crashed.";
      this.updateSession(ctx, { status: "error", lastError: message });
      this.emitProviderEvent(ctx, "error", "session/exited", { message });
      this.sessions.delete(ctx.session.threadId as string);
      return;
    }

    if (!ctx.stopping && this.sessions.has(ctx.session.threadId as string)) {
      this.updateSession(ctx, { status: "closed", activeTurnId: undefined });
      this.emitProviderEvent(ctx, "session", "session/exited", {
        message: "Claude session ended.",
      });
      this.sessions.delete(ctx.session.threadId as string);
    }
  }

  private handleMessage(ctx: ClaudeSessionContext, message: SDKMessage): void {
    // The SDK may report a different session_id in each message. Store
    // it as the resume cursor but never change the canonical threadId
    // -- the orchestration layer uses that to match events.
    const sessionThreadId = asString(asObject(message)?.session_id);
    if (sessionThreadId && sessionThreadId !== ctx.session.resumeCursor) {
      this.updateSession(ctx, { resumeCursor: sessionThreadId });
    }

    if (message.type === "system" && message.subtype === "init") {
      this.emitProviderEvent(ctx, "notification", "thread/started", {
        payload: { thread: { id: ctx.session.threadId }, raw: message },
      });
      return;
    }

    if (message.type === "stream_event") {
      this.handleStreamEvent(ctx, message);
      return;
    }

    if (message.type === "assistant") {
      this.handleAssistantMessage(ctx, message);
      return;
    }

    if (message.type === "tool_progress") {
      this.emitProviderEvent(ctx, "notification", "tool_use/progress", {
        itemId: ProviderItemId.makeUnsafe(message.tool_use_id),
        payload: { toolName: message.tool_name, raw: message },
      });
      return;
    }

    if (message.type === "tool_use_summary") {
      this.emitProviderEvent(ctx, "notification", "tool_use/summary", {
        payload: { summary: message.summary, toolUseIds: message.preceding_tool_use_ids },
      });
      return;
    }

    if (message.type === "result") {
      this.completeTurn(ctx, message);
    }
  }

  private handleStreamEvent(
    ctx: ClaudeSessionContext,
    message: Extract<SDKMessage, { type: "stream_event" }>,
  ): void {
    const event = asObject(message.event);
    const eventType = asString(event?.type);
    if (eventType !== "content_block_delta") return;

    const delta = asObject(event?.delta);
    const deltaType = asString(delta?.type);

    if (deltaType === "text_delta") {
      const text = asString(delta?.text);
      if (text) this.appendAssistantDelta(ctx, text);
    } else if (deltaType === "thinking_delta") {
      const thinking = asString(delta?.thinking);
      if (thinking) {
        this.emitProviderEvent(ctx, "notification", "assistant/thinking", {
          textDelta: thinking,
          payload: { raw: message },
        });
      }
    }
  }

  private handleAssistantMessage(
    ctx: ClaudeSessionContext,
    message: Extract<SDKMessage, { type: "assistant" }>,
  ): void {
    const rawContent = asObject(message.message)?.content;
    const content = Array.isArray(rawContent) ? rawContent : [];
    let collectedText = "";

    for (const blockValue of content) {
      const block = asObject(blockValue);
      const type = asString(block?.type);
      if (!type) continue;

      if (type === "text") {
        const text = asString(block?.text);
        if (text) collectedText += text;
      } else if (type === "thinking") {
        const thinking = asString(block?.thinking);
        if (thinking) {
          this.emitProviderEvent(ctx, "notification", "assistant/thinking", {
            payload: { text: thinking },
          });
        }
      } else if (type === "tool_use") {
        const toolUseId = asString(block?.id) ?? randomUUID();
        const toolName = asString(block?.name) ?? "tool";
        this.emitProviderEvent(ctx, "notification", "item/started", {
          itemId: ProviderItemId.makeUnsafe(toolUseId),
          payload: {
            item: { id: toolUseId, type: "toolUse", tool: toolName, input: block?.input },
          },
        });
      }
    }

    if (collectedText.length > 0) {
      const existing = ctx.currentTurn?.assistantText ?? "";
      const delta = collectedText.startsWith(existing)
        ? collectedText.slice(existing.length)
        : collectedText;
      if (delta.length > 0) this.appendAssistantDelta(ctx, delta);
    }
  }

  private appendAssistantDelta(ctx: ClaudeSessionContext, delta: string): void {
    if (!delta) return;

    if (!ctx.currentTurn) {
      ctx.currentTurn = {
        turnId: (ctx.session.activeTurnId ?? randomUUID()) as string,
        assistantItemId: randomUUID(),
        userContent: [],
        assistantText: "",
        assistantStarted: false,
      };
    }

    if (!ctx.currentTurn.assistantStarted) {
      ctx.currentTurn.assistantStarted = true;
      this.emitProviderEvent(ctx, "notification", "item/started", {
        itemId: ProviderItemId.makeUnsafe(ctx.currentTurn.assistantItemId),
        payload: {
          item: { id: ctx.currentTurn.assistantItemId, type: "agentMessage", text: "" },
        },
      });
    }

    ctx.currentTurn.assistantText += delta;
    this.emitProviderEvent(ctx, "notification", "item/agentMessage/delta", {
      itemId: ProviderItemId.makeUnsafe(ctx.currentTurn.assistantItemId),
      textDelta: delta,
      payload: { itemId: ctx.currentTurn.assistantItemId, delta },
    });
  }

  private completeTurn(
    ctx: ClaudeSessionContext,
    result: Extract<SDKMessage, { type: "result" }>,
  ): void {
    const turn = ctx.currentTurn;
    const turnId = (turn?.turnId ?? ctx.session.activeTurnId ?? randomUUID()) as string;

    if (turn?.assistantStarted) {
      this.emitProviderEvent(ctx, "notification", "item/completed", {
        turnId: turnId as TurnId,
        itemId: ProviderItemId.makeUnsafe(turn.assistantItemId),
        payload: {
          item: { id: turn.assistantItemId, type: "agentMessage", text: turn.assistantText },
        },
      });
    }

    if (turn) {
      const items: unknown[] = [{ type: "userMessage", content: turn.userContent }];
      if (turn.assistantText.trim()) {
        items.push({ type: "agentMessage", text: turn.assistantText });
      }
      ctx.turns.push({ id: turnId as TurnId, items });
    }

    const isError = result.subtype !== "success";
    const errorMessage =
      isError && Array.isArray(result.errors) && result.errors.length > 0
        ? result.errors[0]
        : undefined;

    this.updateSession(ctx, {
      status: isError ? "error" : "ready",
      activeTurnId: undefined,
      ...(errorMessage ? { lastError: errorMessage } : {}),
    });

    this.emitProviderEvent(ctx, "notification", "turn/completed", {
      turnId: turnId as TurnId,
      payload: {
        turn: {
          id: turnId,
          status: isError ? "failed" : "completed",
          ...(errorMessage ? { error: { message: errorMessage } } : {}),
        },
        raw: result,
      },
    });

    ctx.currentTurn = null;
  }

  // ── Tool approval ─────────────────────────────────────────────────

  private async handleCanUseTool(
    threadId: string,
    toolName: Parameters<CanUseTool>[0],
    toolInput: Parameters<CanUseTool>[1],
    options: Parameters<CanUseTool>[2],
  ): ReturnType<CanUseTool> {
    const ctx = this.sessions.get(threadId);
    if (!ctx) return { behavior: "deny", message: "Session not found." };

    if (
      ctx.allowAllForSession ||
      ctx.approvalPolicy === "never" ||
      ctx.permissionMode === "bypassPermissions"
    ) {
      return { behavior: "allow", updatedInput: toolInput };
    }

    const requestKind = inferRequestKindForTool(toolName);
    const requestId = randomUUID();

    const decision = await new Promise<ProviderApprovalDecision>((resolve, reject) => {
      let settled = false;
      const pending: PendingApprovalRequest = {
        requestId,
        requestKind,
        toolName,
        resolve(value) {
          if (settled) return;
          settled = true;
          options.signal.removeEventListener("abort", onAbort);
          resolve(value);
        },
      };
      ctx.pendingApprovals.set(requestId, pending);

      this.emitProviderEvent(
        ctx,
        "request",
        requestKind === "file-change"
          ? "item/fileChange/requestApproval"
          : "item/commandExecution/requestApproval",
        {
          requestId: requestId as ApprovalRequestId,
          requestKind,
          payload: {
            toolName,
            input: toolInput,
            toolUseId: options.toolUseID,
            blockedPath: options.blockedPath,
            reason: options.decisionReason,
          },
        },
      );

      const onAbort = () => {
        if (settled) return;
        settled = true;
        ctx.pendingApprovals.delete(requestId);
        reject(new Error("Permission request was cancelled."));
      };
      options.signal.addEventListener("abort", onAbort, { once: true });
    });

    if (decision === "acceptForSession") {
      ctx.allowAllForSession = true;
      return {
        behavior: "allow",
        updatedInput: toolInput,
        ...(Array.isArray(options.suggestions) && options.suggestions.length > 0
          ? { updatedPermissions: options.suggestions }
          : {}),
      };
    }
    if (decision === "accept") {
      return { behavior: "allow", updatedInput: toolInput };
    }
    return { behavior: "deny", message: "User declined." };
  }

  private requireSession(threadId: string): ClaudeSessionContext {
    const ctx = this.sessions.get(threadId);
    if (!ctx) throw new Error(`Unknown session: ${threadId}`);
    if (ctx.session.status === "closed") throw new Error(`Session is closed: ${threadId}`);
    return ctx;
  }
}
