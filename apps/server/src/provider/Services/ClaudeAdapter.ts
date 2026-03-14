/**
 * ClaudeAdapter - Claude Code implementation of the generic provider adapter contract.
 *
 * Uses the `@anthropic-ai/claude-agent-sdk` to manage Claude Code sessions and
 * emits canonical ProviderRuntimeEvent objects through `streamEvents`.
 *
 * @module ClaudeAdapter
 */
import { ServiceMap } from "effect";

import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * ClaudeAdapterShape - Service API for the Claude Code provider adapter.
 */
export interface ClaudeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {
  readonly provider: "claude";
}

/**
 * ClaudeAdapter - Service tag for Claude Code provider adapter operations.
 */
export class ClaudeAdapter extends ServiceMap.Service<ClaudeAdapter, ClaudeAdapterShape>()(
  "t3/provider/Services/ClaudeAdapter",
) {}
