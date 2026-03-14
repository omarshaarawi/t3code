/**
 * ClaudeCodeManager - Claude Code session lifecycle manager.
 *
 * Wraps the `@anthropic-ai/claude-agent-sdk` and emits `ProviderEvent` objects
 * through an EventEmitter, following the same pattern as `CodexAppServerManager`.
 *
 * Handles all SDK message types and emits events that the adapter layer maps
 * to canonical ProviderRuntimeEvent objects with full Codex feature parity.
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
  model: string | undefined;
  interrupted: boolean;
  /** Track active tool_use items so we can emit item/completed for each. */
  activeToolUseIds: Set<string>;
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

function asNumber(value: unknown): number | undefined {
  return typeof value === "number" ? value : undefined;
}

function inferRequestKindForTool(toolName: string): ProviderRequestKind {
  const normalized = toolName.toLowerCase();
  if (
    normalized.includes("edit") ||
    normalized.includes("write") ||
    normalized.includes("patch") ||
    normalized === "multiedit"
  ) {
    return "file-change";
  }
  return "command";
}

/**
 * Map Claude tool names to canonical item types used by the Codex adapter.
 * Claude Code tools: Bash, Read, Write, Edit, MultiEdit, Glob, Grep, WebFetch,
 * Task, TodoRead, TodoWrite, Agent, NotebookEdit, etc.
 */
function inferCanonicalItemType(toolName: string): string {
  const normalized = toolName.toLowerCase();
  if (normalized === "bash" || normalized === "command" || normalized === "terminal") {
    return "command_execution";
  }
  if (
    normalized === "edit" ||
    normalized === "write" ||
    normalized === "multiedit" ||
    normalized === "notebookedit" ||
    normalized === "patch"
  ) {
    return "file_change";
  }
  if (normalized === "read" || normalized === "glob" || normalized === "grep") {
    return "file_change";
  }
  if (normalized === "webfetch" || normalized === "web_search") {
    return "web_search";
  }
  if (normalized === "task" || normalized === "agent") {
    return "collab_agent_tool_call";
  }
  if (normalized.startsWith("mcp_") || normalized.includes("mcp")) {
    return "mcp_tool_call";
  }
  return "command_execution";
}

/**
 * Extract a human-readable detail string from a tool's input object.
 * Mirrors the Codex adapter's `itemDetail` function.
 */
function extractToolDetail(toolName: string, input: unknown): string | undefined {
  const obj = asObject(input);
  if (!obj) return undefined;

  // Bash: show the command
  if (toolName.toLowerCase() === "bash") {
    return asString(obj.command) ?? asString(obj.cmd);
  }
  // File tools: show the path
  const path =
    asString(obj.file_path) ??
    asString(obj.filePath) ??
    asString(obj.path) ??
    asString(obj.file) ??
    asString(obj.pattern);
  if (path) return path;

  // Task/Agent: show description
  return asString(obj.description) ?? asString(obj.prompt) ?? asString(obj.query);
}

/**
 * Map a Claude tool name to a human-readable title matching
 * the Codex adapter's `itemTitle` function.
 */
function toolTitle(toolName: string, itemType: string): string {
  switch (itemType) {
    case "command_execution":
      return "Ran command";
    case "file_change":
      return "File change";
    case "web_search":
      return "Web search";
    case "mcp_tool_call":
      return `MCP: ${toolName}`;
    case "collab_agent_tool_call":
      return "Agent task";
    default:
      return toolName;
  }
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
        enableFileCheckpointing: true,
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

      // Store the SDK session ID as a resume cursor only -- never
      // overwrite the canonical threadId that the orchestration layer
      // uses for event routing.
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

    // Handle image attachments
    if (input.attachments && input.attachments.length > 0) {
      for (const attachment of input.attachments) {
        if (attachment.type === "image" && attachment.id) {
          // Attachments are persisted on disk by the WS server.
          // The Claude SDK expects base64 image data in the message content.
          // For now we include a reference; the adapter layer should resolve
          // the actual data before passing to sendTurn if needed.
          userContent.push({
            type: "text",
            text: `[Image attachment: ${attachment.name ?? attachment.id}]`,
          });
        }
      }
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
      model: ctx.session.model,
      interrupted: false,
      activeToolUseIds: new Set(),
    };

    this.updateSession(ctx, { status: "running", activeTurnId: turnId as TurnId });
    this.emitProviderEvent(ctx, "notification", "turn/started", {
      turnId: turnId as TurnId,
      payload: {
        turn: { id: turnId, model: ctx.session.model },
      },
    });

    const messageContent: Array<Record<string, unknown>> = [];
    if (input.input) {
      messageContent.push({ type: "text", text: input.input });
    }

    ctx.queue.push({
      message: {
        type: "user",
        session_id: (ctx.session.resumeCursor ?? ctx.session.threadId ?? "") as string,
        parent_tool_use_id: null,
        message: { role: "user", content: messageContent },
      },
    });

    return {
      threadId: ctx.session.threadId,
      turnId: turnId as TurnId,
      resumeCursor: ctx.session.resumeCursor ?? ctx.session.threadId,
    };
  }

  async interruptTurn(threadId: ThreadId, _turnId?: TurnId): Promise<void> {
    const ctx = this.requireSession(threadId as string);
    if (ctx.currentTurn) {
      ctx.currentTurn.interrupted = true;
    }
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
    // Store the SDK session ID as resume cursor without changing canonical threadId.
    const sessionThreadId = asString(asObject(message)?.session_id);
    if (sessionThreadId && sessionThreadId !== ctx.session.resumeCursor) {
      this.updateSession(ctx, { resumeCursor: sessionThreadId });
    }

    // ── System messages ───────────────────────────────────────────
    if (message.type === "system") {
      this.handleSystemMessage(ctx, message);
      return;
    }

    // ── Streaming partial messages ────────────────────────────────
    if (message.type === "stream_event") {
      this.handleStreamEvent(ctx, message);
      return;
    }

    // ── Complete assistant messages ────────────────────────────────
    if (message.type === "assistant") {
      this.handleAssistantMessage(ctx, message);
      return;
    }

    // ── Tool progress ─────────────────────────────────────────────
    if (message.type === "tool_progress") {
      this.emitProviderEvent(ctx, "notification", "tool_use/progress", {
        itemId: ProviderItemId.makeUnsafe(message.tool_use_id),
        payload: {
          toolUseId: message.tool_use_id,
          toolName: message.tool_name,
          elapsedSeconds: message.elapsed_time_seconds,
        },
      });
      return;
    }

    // ── Tool use summary ──────────────────────────────────────────
    if (message.type === "tool_use_summary") {
      this.emitProviderEvent(ctx, "notification", "tool_use/summary", {
        payload: {
          summary: message.summary,
          precedingToolUseIds: message.preceding_tool_use_ids,
        },
      });
      return;
    }

    // ── Result (turn completed) ───────────────────────────────────
    if (message.type === "result") {
      this.completeTurn(ctx, message);
      return;
    }

    // ── Rate limit events ─────────────────────────────────────────
    if (message.type === "rate_limit_event") {
      const info = asObject(message.rate_limit_info);
      if (info) {
        this.emitProviderEvent(ctx, "notification", "account/rateLimits/updated", {
          payload: info,
        });
      }
      return;
    }

    // ── Auth status ───────────────────────────────────────────────
    if (message.type === "auth_status") {
      this.emitProviderEvent(ctx, "notification", "auth/status", {
        payload: {
          isAuthenticating: message.isAuthenticating,
          output: message.output,
          ...(message.error ? { error: message.error } : {}),
        },
      });
      return;
    }
  }

  // ── System message handler ────────────────────────────────────────

  private handleSystemMessage(
    ctx: ClaudeSessionContext,
    message: Extract<SDKMessage, { type: "system" }>,
  ): void {
    const subtype = asString(asObject(message)?.subtype);

    if (subtype === "init") {
      this.emitProviderEvent(ctx, "notification", "thread/started", {
        payload: { thread: { id: ctx.session.threadId }, raw: message },
      });
      return;
    }

    // Task lifecycle (subagent tasks)
    if (subtype === "task_started") {
      const msg = message as unknown as {
        task_id: string;
        tool_use_id?: string;
        description: string;
        task_type?: string;
      };
      this.emitProviderEvent(ctx, "notification", "task/started", {
        payload: {
          taskId: msg.task_id,
          toolUseId: msg.tool_use_id,
          description: msg.description,
          taskType: msg.task_type,
        },
      });
      return;
    }

    if (subtype === "task_progress") {
      const msg = message as unknown as {
        task_id: string;
        tool_use_id?: string;
        description: string;
        usage: { total_tokens: number; tool_uses: number; duration_ms: number };
        last_tool_name?: string;
        summary?: string;
      };
      this.emitProviderEvent(ctx, "notification", "task/progress", {
        payload: {
          taskId: msg.task_id,
          toolUseId: msg.tool_use_id,
          description: msg.description,
          usage: msg.usage,
          lastToolName: msg.last_tool_name,
          summary: msg.summary,
        },
      });
      return;
    }

    if (subtype === "task_notification") {
      const msg = message as unknown as {
        task_id: string;
        tool_use_id?: string;
        status: string;
        summary: string;
        usage?: { total_tokens: number; tool_uses: number; duration_ms: number };
      };
      this.emitProviderEvent(ctx, "notification", "task/completed", {
        payload: {
          taskId: msg.task_id,
          toolUseId: msg.tool_use_id,
          status: msg.status,
          summary: msg.summary,
          usage: msg.usage,
        },
      });
      return;
    }

    // Hook lifecycle
    if (subtype === "hook_started") {
      const msg = message as unknown as {
        hook_id: string;
        hook_name: string;
        hook_event: string;
      };
      this.emitProviderEvent(ctx, "notification", "hook/started", {
        payload: { hookId: msg.hook_id, hookName: msg.hook_name, hookEvent: msg.hook_event },
      });
      return;
    }

    if (subtype === "hook_progress") {
      const msg = message as unknown as {
        hook_id: string;
        hook_name: string;
        hook_event: string;
        stdout: string;
        stderr: string;
        output: string;
      };
      this.emitProviderEvent(ctx, "notification", "hook/progress", {
        payload: {
          hookId: msg.hook_id,
          output: msg.output,
          stdout: msg.stdout,
          stderr: msg.stderr,
        },
      });
      return;
    }

    if (subtype === "hook_response") {
      const msg = message as unknown as {
        hook_id: string;
        hook_name: string;
        hook_event: string;
        output: string;
        stdout: string;
        stderr: string;
        exit_code?: number;
        outcome: string;
      };
      this.emitProviderEvent(ctx, "notification", "hook/completed", {
        payload: {
          hookId: msg.hook_id,
          outcome: msg.outcome,
          output: msg.output,
          stdout: msg.stdout,
          stderr: msg.stderr,
          exitCode: msg.exit_code,
        },
      });
      return;
    }

    // Files persisted
    if (subtype === "files_persisted") {
      this.emitProviderEvent(ctx, "notification", "files/persisted", {
        payload: message,
      });
      return;
    }

    // Compact boundary
    if (subtype === "compact_boundary") {
      this.emitProviderEvent(ctx, "notification", "thread/compacted", {
        payload: message,
      });
      return;
    }
  }

  // ── Stream event handler (partial messages) ───────────────────────

  private handleStreamEvent(
    ctx: ClaudeSessionContext,
    message: Extract<SDKMessage, { type: "stream_event" }>,
  ): void {
    const event = asObject(message.event);
    const eventType = asString(event?.type);

    if (eventType === "content_block_delta") {
      const delta = asObject(event?.delta);
      const deltaType = asString(delta?.type);

      if (deltaType === "text_delta") {
        const text = asString(delta?.text);
        if (text) this.appendAssistantDelta(ctx, text);
      } else if (deltaType === "thinking_delta") {
        const thinking = asString(delta?.thinking);
        if (thinking) {
          this.emitProviderEvent(ctx, "notification", "item/reasoning/textDelta", {
            textDelta: thinking,
            payload: { raw: message },
          });
        }
      }
      return;
    }

    // content_block_start with tool_use type -- emit item/started for tool
    if (eventType === "content_block_start") {
      const contentBlock = asObject(event?.content_block);
      const blockType = asString(contentBlock?.type);
      if (blockType === "tool_use") {
        const toolUseId = asString(contentBlock?.id) ?? randomUUID();
        const toolName = asString(contentBlock?.name) ?? "tool";
        const itemType = inferCanonicalItemType(toolName);
        const title = toolTitle(toolName, itemType);

        if (ctx.currentTurn) {
          ctx.currentTurn.activeToolUseIds.add(toolUseId);
        }

        this.emitProviderEvent(ctx, "notification", "item/started", {
          itemId: ProviderItemId.makeUnsafe(toolUseId),
          payload: {
            item: {
              id: toolUseId,
              type: itemType,
              tool: toolName,
            },
            itemType,
            title,
          },
        });
      }
      return;
    }

    // content_block_stop -- emit item/completed for tool_use blocks
    if (eventType === "content_block_stop") {
      // We don't get the tool_use_id in content_block_stop, but we can
      // check if this is a tool block by index. For now, we'll handle
      // completion in handleAssistantMessage when we get the full message.
      return;
    }
  }

  // ── Assistant message handler (complete messages) ─────────────────

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
          this.emitProviderEvent(ctx, "notification", "item/reasoning/textDelta", {
            payload: { text: thinking },
          });
        }
      } else if (type === "tool_use") {
        const toolUseId = asString(block?.id) ?? randomUUID();
        const toolName = asString(block?.name) ?? "tool";
        const itemType = inferCanonicalItemType(toolName);
        const title = toolTitle(toolName, itemType);
        const detail = extractToolDetail(toolName, block?.input);

        // If we haven't already emitted item/started for this tool
        // (from stream_event), emit it now.
        if (ctx.currentTurn && !ctx.currentTurn.activeToolUseIds.has(toolUseId)) {
          ctx.currentTurn.activeToolUseIds.add(toolUseId);
          this.emitProviderEvent(ctx, "notification", "item/started", {
            itemId: ProviderItemId.makeUnsafe(toolUseId),
            payload: {
              item: {
                id: toolUseId,
                type: itemType,
                tool: toolName,
                input: block?.input,
              },
              itemType,
              title,
              ...(detail ? { detail } : {}),
            },
          });
        }

        // Emit item/completed for this tool_use
        this.emitProviderEvent(ctx, "notification", "item/completed", {
          itemId: ProviderItemId.makeUnsafe(toolUseId),
          payload: {
            item: {
              id: toolUseId,
              type: itemType,
              tool: toolName,
              input: block?.input,
            },
            itemType,
            title,
            ...(detail ? { detail } : {}),
          },
        });
      } else if (type === "tool_result") {
        // Emit command output for tool results
        const toolResultContent = asString(block?.content) ?? asString(block?.text);
        if (toolResultContent) {
          const isError = block?.is_error === true;
          this.emitProviderEvent(ctx, "notification", "item/commandExecution/outputDelta", {
            textDelta: toolResultContent,
            payload: {
              delta: toolResultContent,
              isError,
            },
          });
        }
      }
    }

    // Emit collected assistant text
    if (collectedText.length > 0) {
      const existing = ctx.currentTurn?.assistantText ?? "";
      const delta = collectedText.startsWith(existing)
        ? collectedText.slice(existing.length)
        : collectedText;
      if (delta.length > 0) this.appendAssistantDelta(ctx, delta);
    }

    // Extract usage from the message if available
    const usage = asObject(asObject(message.message)?.usage);
    if (usage) {
      this.emitProviderEvent(ctx, "notification", "thread/tokenUsage/updated", {
        payload: { usage },
      });
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
        model: ctx.session.model,
        interrupted: false,
        activeToolUseIds: new Set(),
      };
    }

    if (!ctx.currentTurn.assistantStarted) {
      ctx.currentTurn.assistantStarted = true;
      this.emitProviderEvent(ctx, "notification", "item/started", {
        itemId: ProviderItemId.makeUnsafe(ctx.currentTurn.assistantItemId),
        payload: {
          item: {
            id: ctx.currentTurn.assistantItemId,
            type: "agentMessage",
            text: "",
          },
          itemType: "assistant_message",
          title: "Assistant message",
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

  // ── Turn completion ───────────────────────────────────────────────

  private completeTurn(
    ctx: ClaudeSessionContext,
    result: Extract<SDKMessage, { type: "result" }>,
  ): void {
    const turn = ctx.currentTurn;
    const turnId = (turn?.turnId ?? ctx.session.activeTurnId ?? randomUUID()) as string;

    // Complete the assistant message item if started
    if (turn?.assistantStarted) {
      this.emitProviderEvent(ctx, "notification", "item/completed", {
        turnId: turnId as TurnId,
        itemId: ProviderItemId.makeUnsafe(turn.assistantItemId),
        payload: {
          item: {
            id: turn.assistantItemId,
            type: "agentMessage",
            text: turn.assistantText,
          },
          itemType: "assistant_message",
          title: "Assistant message",
        },
      });
    }

    // Store turn snapshot
    if (turn) {
      const items: unknown[] = [{ type: "userMessage", content: turn.userContent }];
      if (turn.assistantText.trim()) {
        items.push({ type: "agentMessage", text: turn.assistantText });
      }
      ctx.turns.push({ id: turnId as TurnId, items });
    }

    // Extract rich metadata from the result message
    const isError = result.subtype !== "success";
    const resultObj = result as Record<string, unknown>;
    const stopReason = asString(resultObj.stop_reason);
    const usage = resultObj.usage;
    const modelUsage = asObject(resultObj.modelUsage);
    const totalCostUsd = asNumber(resultObj.total_cost_usd);
    const durationMs = asNumber(resultObj.duration_ms);
    const numTurns = asNumber(resultObj.num_turns);

    const errors = Array.isArray(resultObj.errors) ? resultObj.errors : [];
    const errorMessage =
      isError && errors.length > 0 ? (asString(errors[0]) ?? undefined) : undefined;

    // Determine turn state
    let turnState: string;
    if (turn?.interrupted) {
      turnState = "interrupted";
    } else if (isError) {
      turnState = "failed";
    } else {
      turnState = "completed";
    }

    this.updateSession(ctx, {
      status: isError ? "error" : "ready",
      activeTurnId: undefined,
      ...(errorMessage ? { lastError: errorMessage } : {}),
    });

    // Emit turn.aborted if interrupted
    if (turn?.interrupted) {
      this.emitProviderEvent(ctx, "notification", "turn/aborted", {
        turnId: turnId as TurnId,
        message: "Turn interrupted by user",
        payload: {
          turn: { id: turnId, status: "interrupted" },
          raw: result,
        },
      });
    }

    // Emit turn/completed with full metadata
    this.emitProviderEvent(ctx, "notification", "turn/completed", {
      turnId: turnId as TurnId,
      payload: {
        turn: {
          id: turnId,
          status: turnState,
          ...(stopReason ? { stopReason } : {}),
          ...(usage !== undefined ? { usage } : {}),
          ...(modelUsage ? { modelUsage } : {}),
          ...(totalCostUsd !== undefined ? { totalCostUsd } : {}),
          ...(durationMs !== undefined ? { durationMs } : {}),
          ...(numTurns !== undefined ? { numTurns } : {}),
          ...(errorMessage ? { error: { message: errorMessage } } : {}),
        },
        raw: result,
      },
    });

    // Emit token usage update from result
    if (usage !== undefined || modelUsage) {
      this.emitProviderEvent(ctx, "notification", "thread/tokenUsage/updated", {
        payload: { usage: usage ?? modelUsage },
      });
    }

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
