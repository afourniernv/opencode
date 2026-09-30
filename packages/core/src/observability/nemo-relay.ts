import { Cause, Context, Effect, Exit, Layer, Stream } from "effect"
import { RequestExecutor, type AttemptEvent, type AttemptOrdinal } from "@opencode-ai/llm/route"
import type { LlmHandle, PropagationContext, ScopeHandle, ScopeStack, ToolHandle } from "nemo-relay-node"
import { makeGlobalNode } from "../effect/app-node"

const ENABLE_ENV = "OPENCODE_NEMO_RELAY"
const CONFIG_ENV = "OPENCODE_NEMO_RELAY_PLUGINS_TOML"
const RUNTIME_MODULE_ENV = "OPENCODE_NEMO_RELAY_RUNTIME_MODULE"
const RUNTIME_MODULE = "nemo-relay-node"
const SCHEMA_VERSION = "3"
const DEFAULT_STARTUP_TIMEOUT_MS = 5_000
const DEFAULT_TEARDOWN_TIMEOUT_MS = 5_000

// Bun replaces this identifier with a literal platform package while
// compiling standalone OpenCode binaries. In source/Node execution it remains
// undefined and the public metapackage performs its normal native selection.
declare const OPENCODE_NEMO_RELAY_BUNDLED_MODULE: string | undefined

type MetricAttributes = Record<string, string | number | boolean>

type MetricMeasurement = {
  readonly name: string
  readonly kind: unknown
  readonly valueType: unknown
  readonly value: number
  readonly unit?: string
  readonly description?: string
  readonly attributes?: MetricAttributes
  readonly boundaries?: ReadonlyArray<number>
}

type RelayMetrics = {
  readonly MetricKind: { readonly Counter: unknown; readonly Histogram: unknown }
  readonly MetricValueType: { readonly U64: unknown; readonly F64: unknown }
  readonly metric: (
    name: string,
    measurements: ReadonlyArray<MetricMeasurement>,
    handle?: null,
    metadata?: MetricAttributes | null,
    timestamp?: number | null,
  ) => void
  readonly flushSubscribers: () => Promise<void>
}

type RelayHandle = ScopeHandle | LlmHandle | ToolHandle

type RelayScopeContext<Handle extends RelayHandle = RelayHandle> = {
  readonly stack: ScopeStack
  readonly handle: Handle
}

type RelayTracing = {
  readonly ScopeType: {
    readonly Agent: unknown
    readonly Function?: unknown
    readonly Llm: unknown
    readonly Tool: unknown
    readonly Guardrail?: unknown
  }
  readonly createScopeStack: () => ScopeStack
  readonly capturePropagationContext: () => PropagationContext
  readonly createScopeStackFromPropagation: (context: PropagationContext) => ScopeStack
  readonly withScopeStack: <A>(stack: ScopeStack, callback: () => A) => A
  readonly pushScope: (
    name: string,
    scopeType: unknown,
    handle?: ScopeHandle | null,
    attributes?: number | null,
    data?: unknown,
    metadata?: unknown,
    input?: unknown,
    timestamp?: number | null,
  ) => ScopeHandle
  readonly popScope: (handle: ScopeHandle, output?: unknown, timestamp?: number | null, metadata?: unknown) => void
  readonly llmCall: (
    name: string,
    request: unknown,
    handle?: ScopeHandle | null,
    attributes?: number | null,
    data?: unknown,
    metadata?: unknown,
    modelName?: string | null,
    timestamp?: number | null,
  ) => LlmHandle
  readonly llmCallEnd: (
    handle: LlmHandle,
    response: unknown,
    data?: unknown,
    metadata?: unknown,
    timestamp?: number | null,
  ) => void
  readonly toolCall: (
    name: string,
    args: unknown,
    handle?: ScopeHandle | null,
    attributes?: number | null,
    data?: unknown,
    metadata?: unknown,
    toolCallId?: string | null,
    timestamp?: number | null,
  ) => ToolHandle
  readonly toolCallEnd: (
    handle: ToolHandle,
    result: { readonly result: unknown },
    data?: unknown,
    metadata?: unknown,
    timestamp?: number | null,
  ) => void
  readonly event: (
    name: string,
    handle?: ScopeHandle | null,
    data?: unknown,
    metadata?: unknown,
    timestamp?: number | null,
  ) => void
}

type RelayRuntime = RelayMetrics & Partial<RelayTracing>

type RawPluginHostActivation = {
  readonly isActive: boolean
  readonly report: unknown
  readonly close: () => Promise<void>
}

type PluginHostActivation = {
  readonly report: () => unknown
  readonly isActive: () => boolean | undefined
  readonly close: () => Promise<void>
}

type RelayModule = RelayMetrics &
  RelayTracing & {
    readonly initialize: (
      config: { readonly version: 1; readonly components: readonly [] },
      additionalPluginsToml?: string,
    ) => Promise<RawPluginHostActivation>
  }

type Loader = (specifier: string) => Promise<unknown>
type EnvironmentReader = () => Readonly<Record<string, string | undefined>>

export type ReportSummary = {
  readonly configPathCount: number
  readonly componentCount: number
  readonly dynamicSelectedCount: number
  readonly warningCount: number
  readonly errorCount: number
  readonly runtimeDiagnosticCount: number
  readonly dynamicFailureCount: number
}

export type UnavailableReason =
  | "configuration_without_enable"
  | "invalid_enable_value"
  | "unsupported_platform"
  | "runtime_unavailable"
  | "incompatible_runtime"
  | "activation_conflict"
  | "activation_failed"
  | "startup_timeout"
  | "configuration_failed"
  | "invalid_activation"
  | "teardown_failed"
  | "stopped"

export type Status =
  | { readonly state: "disabled"; readonly reason: "not_requested" | "explicitly_disabled" | "pure_mode" }
  | {
      readonly state: "active"
      readonly activation: "healthy" | "degraded"
      readonly configuration: "empty" | "present"
      readonly report: ReportSummary
    }
  | { readonly state: "unavailable"; readonly reason: UnavailableReason }

export type ProcessHealth = {
  readonly phase: "idle" | "starting" | "active" | "draining" | "teardown_failed" | "stopped"
  readonly owners: number
  readonly operations: number
  readonly status: Status
}

type Host = {
  status: Status
  relay?: RelayRuntime
  activation?: PluginHostActivation
  accepting: boolean
  operations: number
  drainWaiters: Set<() => void>
  flushTask?: Promise<{ readonly ok: boolean }>
  flushComplete?: boolean
  flushSucceeded?: boolean
  closeTask?: Promise<{ readonly ok: boolean }>
  closeComplete?: boolean
  activationTask?: Promise<void>
}

export type Runtime = "native" | "ai_sdk" | "workflow" | "unknown"
export type TurnRuntime = "v1" | "v2"
export type CallRole = "primary" | "compaction" | "title" | "summary" | "agent_generation" | "other"
export type LlmMode = "stream" | "unary"
export type LlmOutputKind = "text" | "reasoning" | "tool"
export type LlmOperation =
  | "openai.chat_completions"
  | "openai.responses"
  | "anthropic.messages"
  | "google.generate_content"
  | "aws.converse"
  | "openrouter.chat_completions"
  | "unknown"
export type LlmOutcome = "success" | "provider_error" | "failed" | "cancelled" | "incomplete" | "unknown"
export type LlmCostSource = "provider_reported" | "price_table_estimate"
export type HostRetryErrorKind = "rate_limit" | "quota_exceeded" | "provider_internal" | "transport" | "unknown"
export type HostRetryDelaySource = "retry_after" | "backoff" | "unknown"
export type TurnOutcome = "success" | "failed" | "blocked" | "cancelled"
export type ToolOutcome = "success" | "failed" | "blocked" | "cancelled" | "unknown"
export type TerminalResultFamily = "zero_exit" | "nonzero_exit" | "timeout" | "aborted" | "signal" | "unknown"
export type PermissionEffect = "allow" | "deny" | "ask"
export type PermissionResolution = "once" | "always" | "reject" | "corrected" | "cancelled" | "unknown"
export type QuestionResolution = "answered" | "rejected" | "cancelled" | "unknown"
export type CompactionTrigger = "proactive" | "overflow_recovery" | "manual"
export type CompactionOutcome = "success" | "failed" | "not_possible" | "cancelled"
export type PermissionFamily =
  | "filesystem"
  | "terminal"
  | "network"
  | "delegation"
  | "interaction"
  | "safety"
  | "mcp"
  | "other"
  | "unknown"
export type ToolCategory =
  | "code_search"
  | "file_read"
  | "file_write"
  | "terminal"
  | "code_execution"
  | "delegation"
  | "planning"
  | "human_input"
  | "web"
  | "skill"
  | "mcp"
  | "provider"
  | "extension"
  | "other"
  | "unknown"

export type TokenUsage = {
  readonly inputTotal?: number
  readonly inputNonCached?: number
  readonly inputCacheRead?: number
  readonly inputCacheWrite?: number
  readonly outputTotal?: number
  readonly outputReasoning?: number
}

export type LlmStreamStarted = {
  readonly role: CallRole
  readonly agentRuntime: TurnRuntime
  readonly runtime: Runtime
  readonly provider: string
  readonly model: string
  readonly protocol?: string
  /** Selected catalog context limit. Used only with provider-reported input usage. */
  readonly contextLimit?: number
}

export type LlmStreamCompleted = LlmStreamStarted & {
  readonly outcome: LlmOutcome
  readonly finish?: string
  readonly durationMs: number
  readonly tokens?: TokenUsage
  /** One ratio per provider step; never a host-stream aggregate divided by one context window. */
  readonly inputContextUtilizations?: ReadonlyArray<number>
  readonly firstOutput?: {
    readonly kind: LlmOutputKind
    readonly latencyMs: number
  }
  readonly providerError?: {
    readonly classification: string
    readonly retryable?: boolean
  }
}

export type LlmUnaryCompleted = LlmStreamStarted & {
  readonly outcome: Exclude<LlmOutcome, "provider_error" | "incomplete" | "unknown">
  readonly finish?: string
  readonly durationMs: number
  readonly tokens?: TokenUsage
}

export type LlmCostRecorded = {
  readonly role: CallRole
  readonly agentRuntime: TurnRuntime
  readonly provider: string
  readonly model: string
  readonly costUsd: number
  readonly source: LlmCostSource
}

export type RunStarted = {
  readonly runtime: TurnRuntime
}

export type RunOperationCounts = {
  readonly turns: number
  readonly llmOperations: number
  readonly toolCalls: number
  readonly hostRetries: number
  readonly providerAttempts: number
  readonly providerRetries: number
  readonly permissionWaits: number
  readonly questionWaits: number
}

export type RunCompleted = RunStarted & {
  readonly outcome: TurnOutcome
  readonly durationMs: number
  readonly operations: RunOperationCounts
}

export type TurnStarted = {
  readonly role: CallRole
  readonly runtime: TurnRuntime
}

export type ToolStarted = {
  readonly name?: string
  readonly category?: ToolCategory
  readonly execution: "local" | "provider"
}

export type ToolCompleted = ToolStarted & {
  readonly outcome: ToolOutcome
  readonly durationMs?: number
  readonly terminalResult?: TerminalResultFamily
}

export type ToolCompletionDetails = {
  readonly terminalResult?: TerminalResultFamily
}

export type PermissionEvaluation = {
  readonly runtime: TurnRuntime
  readonly family: PermissionFamily
  readonly effect: PermissionEffect
}

export type PermissionWaitStarted = {
  readonly runtime: TurnRuntime
  readonly family: PermissionFamily
}

export type PermissionWaitCompleted = PermissionWaitStarted & {
  readonly resolution: PermissionResolution
  readonly durationMs: number
}

export type QuestionWaitStarted = {
  readonly runtime: TurnRuntime
}

export type QuestionWaitCompleted = QuestionWaitStarted & {
  readonly resolution: QuestionResolution
  readonly durationMs: number
}

export type CompactionCompleted = {
  readonly runtime: "v2"
  readonly sourceEstimatedTokens: number
  readonly summaryEstimatedTokens: number
  readonly retainedRecentEstimatedTokens: number
}

export type CompactionAttemptCompleted = {
  readonly runtime: TurnRuntime
  readonly trigger: CompactionTrigger
  readonly outcome: CompactionOutcome
  readonly durationMs: number
}

export interface ToolObservation {
  readonly run: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  readonly complete: (outcome: ToolOutcome, details?: ToolCompletionDetails) => Effect.Effect<void>
}

type UsageLike = {
  readonly inputTokens?: number
  readonly nonCachedInputTokens?: number
  readonly cacheReadInputTokens?: number
  readonly cacheWriteInputTokens?: number
  readonly outputTokens?: number
  readonly reasoningTokens?: number
}

type LlmEventLike = {
  readonly type: string
  readonly text?: string
  readonly reason?: string
  readonly usage?: UsageLike
  readonly classification?: string
  readonly retryable?: boolean
}

export interface Interface {
  readonly status: Status
  readonly detached: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>
  readonly observeRun: <A, E, R>(
    input: RunStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => TurnOutcome | undefined,
  ) => Effect.Effect<A, E, R>
  readonly observeLlmStream: <A extends LlmEventLike, E, R>(
    input: LlmStreamStarted,
    source: Stream.Stream<A, E, R>,
  ) => Stream.Stream<A, E, R>
  readonly observeLlmUnary: <A, E, R>(
    input: LlmStreamStarted,
    effect: Effect.Effect<A, E, R>,
    project?: (value: A) => { readonly finish?: string; readonly tokens?: TokenUsage },
  ) => Effect.Effect<A, E, R>
  readonly observeTurn: <A, E, R>(
    input: TurnStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => TurnOutcome | undefined,
  ) => Effect.Effect<A, E, R>
  readonly observePermissionWait: <A, E, R>(
    input: PermissionWaitStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => PermissionResolution | undefined,
  ) => Effect.Effect<A, E, R>
  readonly observeQuestionWait: <A, E, R>(
    input: QuestionWaitStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => QuestionResolution | undefined,
  ) => Effect.Effect<A, E, R>
  readonly beginTool: (input: ToolStarted) => Effect.Effect<ToolObservation>
  readonly runStarted: (input: RunStarted) => Effect.Effect<void>
  readonly runCompleted: (input: RunCompleted) => Effect.Effect<void>
  readonly llmStreamCompleted: (input: LlmStreamCompleted) => Effect.Effect<void>
  readonly llmUnaryCompleted: (input: LlmUnaryCompleted) => Effect.Effect<void>
  readonly llmCostRecorded: (input: LlmCostRecorded) => Effect.Effect<void>
  readonly turnCompleted: (
    input: TurnStarted & { readonly outcome: TurnOutcome; readonly durationMs: number },
  ) => Effect.Effect<void>
  readonly toolCompleted: (input: ToolCompleted) => Effect.Effect<void>
  readonly permissionEvaluated: (input: PermissionEvaluation) => Effect.Effect<void>
  readonly permissionWaitCompleted: (input: PermissionWaitCompleted) => Effect.Effect<void>
  readonly questionWaitCompleted: (input: QuestionWaitCompleted) => Effect.Effect<void>
  readonly compactionAttemptCompleted: (input: CompactionAttemptCompleted) => Effect.Effect<void>
  readonly compactionCompleted: (input: CompactionCompleted) => Effect.Effect<void>
  readonly retryScheduled: (input: {
    readonly runtime: TurnRuntime
    readonly attempt: number
    readonly delayMs?: number
    readonly delaySource?: HostRetryDelaySource
    readonly errorKind?: HostRetryErrorKind
  }) => Effect.Effect<void>
}

export type TestingHooks = {
  readonly runStarted?: (input: RunStarted) => Effect.Effect<void>
  readonly runCompleted?: (input: RunCompleted) => Effect.Effect<void>
  readonly llmStreamCompleted?: (input: LlmStreamCompleted) => Effect.Effect<void>
  readonly llmUnaryCompleted?: (input: LlmUnaryCompleted) => Effect.Effect<void>
  readonly llmCostRecorded?: (input: LlmCostRecorded) => Effect.Effect<void>
  readonly turnCompleted?: (
    input: TurnStarted & { readonly outcome: TurnOutcome; readonly durationMs: number },
  ) => Effect.Effect<void>
  readonly toolCompleted?: (input: ToolCompleted) => Effect.Effect<void>
  readonly permissionWaitCompleted?: (input: PermissionWaitCompleted) => Effect.Effect<void>
  readonly questionWaitCompleted?: (input: QuestionWaitCompleted) => Effect.Effect<void>
  readonly compactionAttemptCompleted?: (input: CompactionAttemptCompleted) => Effect.Effect<void>
  readonly compactionCompleted?: (input: CompactionCompleted) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/NemoRelay") {}

const CurrentRelayScope = Context.Reference<RelayScopeContext | undefined>("@opencode/NemoRelay/CurrentScope", {
  defaultValue: () => undefined,
})

type MutableRunOperationCounts = { -readonly [K in keyof RunOperationCounts]: RunOperationCounts[K] } & {
  lastTurnOutcome?: TurnOutcome
}

const CurrentRelayRun = Context.Reference<MutableRunOperationCounts | undefined>("@opencode/NemoRelay/CurrentRun", {
  defaultValue: () => undefined,
})

const disabledStatus: Status = { state: "disabled", reason: "not_requested" }
const noopTool: ToolObservation = { run: (effect) => effect, complete: () => Effect.void }
const noop: Interface = {
  status: disabledStatus,
  detached: (effect) => effect,
  observeRun: (_input, effect) => effect,
  observeLlmStream: (_input, source) => source,
  observeLlmUnary: (_input, effect) => effect,
  observeTurn: (_input, effect) => effect,
  observePermissionWait: (_input, effect) => effect,
  observeQuestionWait: (_input, effect) => effect,
  beginTool: () => Effect.succeed(noopTool),
  runStarted: () => Effect.void,
  runCompleted: () => Effect.void,
  llmStreamCompleted: () => Effect.void,
  llmUnaryCompleted: () => Effect.void,
  llmCostRecorded: () => Effect.void,
  turnCompleted: () => Effect.void,
  toolCompleted: () => Effect.void,
  permissionEvaluated: () => Effect.void,
  permissionWaitCompleted: () => Effect.void,
  questionWaitCompleted: () => Effect.void,
  compactionAttemptCompleted: () => Effect.void,
  compactionCompleted: () => Effect.void,
  retryScheduled: () => Effect.void,
}

const defaultLoader: Loader = async (specifier) => {
  if (specifier === RUNTIME_MODULE && typeof OPENCODE_NEMO_RELAY_BUNDLED_MODULE === "string") {
    const loaded = await import(OPENCODE_NEMO_RELAY_BUNDLED_MODULE)
    return isRecord(loaded) && "default" in loaded ? loaded.default : loaded
  }
  return import(specifier)
}
const defaultEnvironment: EnvironmentReader = () => process.env

function relayModule(value: unknown): RelayModule {
  if (typeof value !== "object" || value === null) throw new Error("NeMo Relay runtime module is not an object")
  const candidate = value as Partial<RelayModule>
  if (
    !candidate.MetricKind ||
    !("Counter" in candidate.MetricKind) ||
    !("Histogram" in candidate.MetricKind) ||
    !candidate.MetricValueType ||
    !("U64" in candidate.MetricValueType) ||
    !("F64" in candidate.MetricValueType) ||
    typeof candidate.metric !== "function"
  )
    throw new Error("NeMo Relay runtime module does not expose the metric API")
  if (
    !candidate.ScopeType ||
    !("Agent" in candidate.ScopeType) ||
    !("Llm" in candidate.ScopeType) ||
    !("Tool" in candidate.ScopeType) ||
    typeof candidate.createScopeStack !== "function" ||
    typeof candidate.capturePropagationContext !== "function" ||
    typeof candidate.createScopeStackFromPropagation !== "function" ||
    typeof candidate.withScopeStack !== "function" ||
    typeof candidate.pushScope !== "function" ||
    typeof candidate.popScope !== "function" ||
    typeof candidate.llmCall !== "function" ||
    typeof candidate.llmCallEnd !== "function" ||
    typeof candidate.toolCall !== "function" ||
    typeof candidate.toolCallEnd !== "function" ||
    typeof candidate.event !== "function"
  )
    throw new Error("NeMo Relay runtime module does not expose the trace API")
  if (typeof candidate.flushSubscribers !== "function")
    throw new Error("NeMo Relay runtime module does not expose flushSubscribers")
  if (typeof candidate.initialize !== "function")
    throw new Error("NeMo Relay runtime module does not expose initialize")
  return candidate as RelayModule
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}

function isScopeEventParent(value: RelayHandle): value is ScopeHandle {
  try {
    return isRecord(value) && value.scopeType !== undefined
  } catch {
    return false
  }
}

function strings(value: unknown) {
  return Array.isArray(value) ? value.filter((item): item is Record<string, unknown> => isRecord(item)) : []
}

function reportSummary(report: unknown): ReportSummary {
  const root = isRecord(report) ? report : {}
  const config = isRecord(root.config) ? root.config : {}
  const diagnostics = strings(config.diagnostics)
  const runtimeDiagnostics = strings(config.runtime_diagnostics)
  const dynamic = strings(root.dynamic_plugins)
  const resolved = isRecord(root.resolved_config) ? root.resolved_config : {}
  return {
    configPathCount: Array.isArray(root.config_paths) ? root.config_paths.length : 0,
    componentCount: Array.isArray(resolved.components) ? resolved.components.length : 0,
    dynamicSelectedCount: dynamic.filter((item) => item.selected === true).length,
    warningCount: diagnostics.filter((item) => item.level === "warning").length,
    errorCount: diagnostics.filter((item) => item.level === "error").length,
    runtimeDiagnosticCount: runtimeDiagnostics.length,
    dynamicFailureCount: dynamic.filter((item) => item.failure !== undefined && item.failure !== null).length,
  }
}

function reportHasCode(report: unknown, code: string) {
  if (!isRecord(report) || !isRecord(report.config)) return false
  return strings(report.config.diagnostics).some((item) => item.code === code)
}

type Request =
  | { readonly state: "disabled"; readonly status: Status }
  | { readonly state: "invalid"; readonly status: Status }
  | {
      readonly state: "enabled"
      readonly config?: string
      readonly runtimeSpecifier: string
      readonly customRuntime: boolean
    }

function request(environment: Readonly<Record<string, string | undefined>>): Request {
  const raw = environment[ENABLE_ENV]?.trim().toLowerCase()
  const config = environment[CONFIG_ENV]?.trim() || undefined
  const pure = environment.OPENCODE_PURE?.trim().toLowerCase()
  if (pure === "1" || pure === "true") return { state: "disabled", status: { state: "disabled", reason: "pure_mode" } }
  if (raw === undefined || raw === "") {
    if (config)
      return {
        state: "invalid",
        status: { state: "unavailable", reason: "configuration_without_enable" },
      }
    return { state: "disabled", status: disabledStatus }
  }
  if (["0", "false", "no", "off"].includes(raw))
    return { state: "disabled", status: { state: "disabled", reason: "explicitly_disabled" } }
  if (!["1", "true", "yes", "on"].includes(raw))
    return { state: "invalid", status: { state: "unavailable", reason: "invalid_enable_value" } }
  const runtimeSpecifier = environment[RUNTIME_MODULE_ENV]?.trim() || RUNTIME_MODULE
  return {
    state: "enabled",
    config,
    runtimeSpecifier,
    customRuntime: runtimeSpecifier !== RUNTIME_MODULE,
  }
}

function activationConflict(error: unknown) {
  const message = error instanceof Error ? error.message.toLowerCase() : ""
  return (
    message.includes("active dynamic plugin host") ||
    message.includes("static plugin configuration is already active") ||
    message.includes("plugin configuration is owned by an active") ||
    message.includes("binding identity is already initialized")
  )
}

function activationMetric(relay: RelayModule) {
  relay.metric(
    "opencode.runtime.activation",
    [
      {
        name: "opencode.runtime.activation.count",
        kind: relay.MetricKind.Counter,
        valueType: relay.MetricValueType.U64,
        value: 1,
        unit: "{activation}",
        attributes: { "opencode.metric.schema_version": SCHEMA_VERSION },
      },
    ],
    null,
    { "opencode.metric.schema_version": SCHEMA_VERSION },
    null,
  )
}

function inactive(status: Status): Host {
  return { status, accepting: false, operations: 0, drainWaiters: new Set() }
}

function activeHost(status: Status, relay: RelayRuntime, activation?: PluginHostActivation): Host {
  return { status, relay, activation, accepting: status.state === "active", operations: 0, drainWaiters: new Set() }
}

function activationIsActive(activation: PluginHostActivation) {
  try {
    return activation.isActive()
  } catch {
    return undefined
  }
}

function activationHandle(value: unknown): PluginHostActivation | undefined {
  if (!isRecord(value)) return undefined
  let close: unknown
  try {
    close = value.close
  } catch {
    close = undefined
  }
  if (typeof close !== "function") return undefined
  return {
    // Relay's report is a live getter. Keep access lazy so process health can
    // reflect runtime diagnostics added after activation, while never exposing
    // the report itself outside this adapter.
    report: () => Reflect.get(value, "report", value),
    isActive: () => {
      try {
        return typeof value.isActive === "boolean" ? value.isActive : undefined
      } catch {
        return undefined
      }
    },
    close: () => Promise.resolve().then(() => Reflect.apply(close, value, [])),
  }
}

type OpenOptions = {
  readonly loader: Loader
  readonly environment: EnvironmentReader
  readonly platform: string
  readonly arch: string
  readonly startupTimeoutMs: number
}

async function open(options: OpenOptions): Promise<Host> {
  let selected: Request
  try {
    selected = request(options.environment())
  } catch {
    return inactive({ state: "unavailable", reason: "configuration_failed" })
  }
  if (selected.state !== "enabled") return inactive(selected.status)
  if (!selected.customRuntime && options.platform === "darwin" && options.arch === "x64")
    return inactive({ state: "unavailable", reason: "unsupported_platform" })
  const startupDeadline = performance.now() + Math.max(0, options.startupTimeoutMs)

  let loaded: unknown
  try {
    const result = await bounded(
      Promise.resolve().then(() => options.loader(selected.runtimeSpecifier)),
      remaining(startupDeadline),
    )
    if (!result.settled) return inactive({ state: "unavailable", reason: "startup_timeout" })
    loaded = result.value
  } catch {
    return inactive({ state: "unavailable", reason: "runtime_unavailable" })
  }

  let relay: RelayModule
  try {
    relay = relayModule(loaded)
  } catch {
    return inactive({ state: "unavailable", reason: "incompatible_runtime" })
  }

  let rawActivation: unknown
  try {
    const initialization = Promise.resolve().then(() =>
      relay.initialize({ version: 1, components: [] }, selected.config),
    )
    const result = await bounded(initialization, remaining(startupDeadline))
    if (!result.settled) {
      const late = activeHost({ state: "unavailable", reason: "startup_timeout" }, relay)
      late.accepting = false
      const activationTask = initialization.then(
        (value) => {
          late.activation = activationHandle(value)
        },
        () => {},
      )
      late.activationTask = activationTask
      // Native initialization cannot be cancelled. If it finishes after the
      // host has failed open, immediately clean up that process-global state;
      // the lifecycle also retains this host until cleanup is proven complete.
      void activationTask.then(() => closeHost(late, DEFAULT_TEARDOWN_TIMEOUT_MS))
      return late
    }
    rawActivation = result.value
  } catch (error) {
    return inactive({
      state: "unavailable",
      reason: activationConflict(error) ? "activation_conflict" : "activation_failed",
    })
  }

  const activation = activationHandle(rawActivation)
  let activationReport: unknown
  try {
    activationReport = activation?.report()
  } catch {
    activationReport = undefined
  }
  if (!activation || activationIsActive(activation) !== true || !isRecord(activationReport)) {
    if (activation) {
      const invalid = activeHost({ state: "unavailable", reason: "invalid_activation" }, relay, activation)
      invalid.accepting = false
      const closed = await closeHost(invalid, 2_000)
      if (!closed.closed) return invalid
    }
    return inactive({ state: "unavailable", reason: "invalid_activation" })
  }

  let summary: ReportSummary
  let invalidConfiguration: boolean
  try {
    summary = reportSummary(activationReport)
    invalidConfiguration =
      summary.errorCount > 0 ||
      (selected.config !== undefined && reportHasCode(activationReport, "plugin.configuration_file_missing"))
  } catch {
    const invalid = activeHost({ state: "unavailable", reason: "invalid_activation" }, relay, activation)
    invalid.accepting = false
    const closed = await closeHost(invalid, 2_000)
    return closed.closed ? inactive(invalid.status) : invalid
  }
  if (invalidConfiguration) {
    const invalid = activeHost({ state: "unavailable", reason: "configuration_failed" }, relay, activation)
    invalid.accepting = false
    const closed = await closeHost(invalid, 2_000)
    return closed.closed ? inactive(invalid.status) : invalid
  }

  const configured =
    selected.config !== undefined ||
    summary.configPathCount > 0 ||
    summary.componentCount > 0 ||
    summary.dynamicSelectedCount > 0
  const degraded = summary.warningCount > 0 || summary.runtimeDiagnosticCount > 0 || summary.dynamicFailureCount > 0
  const status: Status = {
    state: "active",
    activation: degraded ? "degraded" : "healthy",
    configuration: configured ? "present" : "empty",
    report: summary,
  }
  try {
    activationMetric(relay)
  } catch {
    // Activation remains valid. The health surface reports plugin-host state,
    // not exporter delivery, which must be checked at the destination.
  }
  return activeHost(status, relay, activation)
}

function refreshStatus(host: Host): Status {
  if (host.status.state !== "active" || !host.activation) return host.status
  let value: unknown
  try {
    value = host.activation.report()
  } catch {
    host.status = { ...host.status, activation: "degraded" }
    return host.status
  }
  if (!isRecord(value)) {
    host.status = { ...host.status, activation: "degraded" }
    return host.status
  }
  try {
    const summary = reportSummary(value)
    const configuration =
      host.status.configuration === "present" ||
      summary.configPathCount > 0 ||
      summary.componentCount > 0 ||
      summary.dynamicSelectedCount > 0
        ? "present"
        : "empty"
    const degraded =
      summary.warningCount > 0 ||
      summary.errorCount > 0 ||
      summary.runtimeDiagnosticCount > 0 ||
      summary.dynamicFailureCount > 0
    host.status = {
      state: "active",
      activation: degraded ? "degraded" : "healthy",
      configuration,
      report: summary,
    }
  } catch {
    host.status = { ...host.status, activation: "degraded" }
  }
  return host.status
}

function remaining(deadline: number) {
  return Math.max(0, deadline - performance.now())
}

async function bounded<A>(
  promise: Promise<A>,
  timeoutMs: number,
): Promise<{ readonly settled: true; readonly value: A } | { readonly settled: false }> {
  if (timeoutMs <= 0) return { settled: false }
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise.then((value) => ({ settled: true as const, value })),
      new Promise<{ readonly settled: false }>((resolve) => {
        timer = setTimeout(() => resolve({ settled: false }), timeoutMs)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

function waitForDrain(host: Host) {
  if (host.operations === 0) return Promise.resolve()
  return new Promise<void>((resolve) => host.drainWaiters.add(resolve))
}

type CloseResult = {
  readonly drained: boolean
  readonly flushed: boolean
  readonly closed: boolean
}

async function closeHost(host: Host, timeoutMs: number): Promise<CloseResult> {
  host.accepting = false
  const deadline = performance.now() + Math.max(0, timeoutMs)
  const drained = await bounded(waitForDrain(host), remaining(deadline))
  if (!drained.settled) return { drained: false, flushed: false, closed: false }

  if (host.activationTask) {
    const activated = await bounded(host.activationTask, remaining(deadline))
    if (!activated.settled) return { drained: true, flushed: false, closed: false }
    host.activationTask = undefined
  }
  if (!host.relay || !host.activation) return { drained: true, flushed: true, closed: true }
  if (host.closeComplete) return { drained: true, flushed: host.flushSucceeded === true, closed: true }

  if (!host.flushComplete) {
    host.flushTask ??= Promise.resolve()
      .then(() => host.relay!.flushSubscribers())
      .then(
        () => ({ ok: true }),
        () => ({ ok: false }),
      )
    const flushed = await bounded(host.flushTask, remaining(deadline))
    if (!flushed.settled) return { drained: true, flushed: false, closed: false }
    host.flushComplete = true
    host.flushSucceeded = flushed.value.ok
    host.flushTask = undefined
  }

  host.closeTask ??= Promise.resolve()
    .then(() => host.activation!.close())
    .then(
      () => ({ ok: true }),
      () => ({ ok: false }),
    )
  const closed = await bounded(host.closeTask, remaining(deadline))
  if (!closed.settled) return { drained: true, flushed: host.flushSucceeded === true, closed: false }
  host.closeTask = undefined
  host.closeComplete = closed.value.ok && activationIsActive(host.activation) === false
  return {
    drained: true,
    flushed: host.flushSucceeded === true,
    closed: host.closeComplete,
  }
}

class Lifecycle {
  private owners = 0
  private current: Promise<Host> | undefined
  private teardown: Promise<void> | undefined
  private retained: Host | undefined
  private stopping = false
  private stopped = false
  private shutdownTask: Promise<CloseResult> | undefined
  private lastTeardown: CloseResult | undefined
  private host: Host | undefined
  private phase: ProcessHealth["phase"] = "idle"
  private lastStatus: Status = disabledStatus

  constructor(private readonly options: OpenOptions) {}

  async acquire() {
    if (this.stopping || this.stopped) return inactive({ state: "unavailable", reason: "stopped" })
    this.owners++
    if (!this.current) {
      const previous = this.teardown
      this.phase = "starting"
      this.current = (async () => {
        if (previous) await previous
        if (this.stopping || this.stopped) return inactive({ state: "unavailable", reason: "stopped" })
        if (this.retained) {
          const retry = await closeHost(this.retained, DEFAULT_TEARDOWN_TIMEOUT_MS)
          if (!retry.closed) {
            this.phase = "teardown_failed"
            this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
            return inactive(this.lastStatus)
          }
          this.retained = undefined
        }
        if (this.stopping || this.stopped) return inactive({ state: "unavailable", reason: "stopped" })
        const host = await open(this.options).catch(() =>
          inactive({ state: "unavailable", reason: "activation_failed" }),
        )
        this.host = host
        if (this.stopping || this.stopped) {
          // Shutdown may race a native module load that cannot be cancelled.
          // Preserve the late activation for this or a later shutdown attempt,
          // but never admit host work through the service returned to the racer.
          host.accepting = false
          if (host.activation || host.activationTask) this.retained = host
          host.status = { state: "unavailable", reason: "stopped" }
          this.lastStatus = host.status
          return host
        }
        if (host.status.state === "active") this.lastTeardown = undefined
        this.lastStatus = host.status
        this.phase = host.status.state === "active" ? "active" : "idle"
        return host
      })()
    }
    return this.current
  }

  async release() {
    this.owners = Math.max(0, this.owners - 1)
    if (this.stopping || this.stopped) return
    if (this.owners !== 0 || !this.current) return
    const current = this.current
    this.current = undefined
    if (this.host) this.host.accepting = false
    void current.then(
      (host) => {
        host.accepting = false
      },
      () => {},
    )
    this.phase = "draining"
    const task = (async () => {
      const host = await current
      const result = await closeHost(host, DEFAULT_TEARDOWN_TIMEOUT_MS)
      this.lastTeardown = result
      if (!result.closed && (host.activation || host.activationTask)) {
        this.retained = host
        this.phase = "teardown_failed"
        this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
        return
      }
      if (this.host === host) this.host = undefined
      if (this.retained === host) this.retained = undefined
      if (!this.stopping) {
        this.phase = result.flushed ? "idle" : "teardown_failed"
        if (!result.flushed) this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
      }
    })()
    this.teardown = task
    try {
      await task
    } finally {
      if (this.teardown === task) this.teardown = undefined
    }
  }

  admit(host: Host) {
    if (this.stopping || this.stopped || !host.accepting || host.status.state !== "active") return false
    if (host.activation && activationIsActive(host.activation) !== true) return false
    host.operations++
    return true
  }

  finish(host: Host) {
    host.operations = Math.max(0, host.operations - 1)
    if (host.operations !== 0) return
    for (const resolve of host.drainWaiters) resolve()
    host.drainWaiters.clear()
  }

  shutdown(timeoutMs = DEFAULT_TEARDOWN_TIMEOUT_MS) {
    if (this.shutdownTask) return this.shutdownTask
    this.stopping = true
    this.phase = "draining"
    this.owners = 0
    const current = this.current
    const task = (async () => {
      const deadline = performance.now() + Math.max(0, timeoutMs)
      if (this.teardown) {
        const prior = await bounded(this.teardown, remaining(deadline))
        if (!prior.settled) {
          this.stopped = false
          this.phase = "teardown_failed"
          this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
          return { drained: false, flushed: false, closed: false }
        }
      }
      let host = this.retained
      if (current) {
        const opened = await bounded(current, remaining(deadline))
        if (!opened.settled) {
          this.stopped = false
          this.phase = "teardown_failed"
          this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
          return { drained: false, flushed: false, closed: false }
        }
        host = opened.value.activation || opened.value.activationTask ? opened.value : (this.retained ?? opened.value)
      }
      if (!host) {
        const result = this.lastTeardown ?? { drained: true, flushed: true, closed: true }
        this.stopped = result.closed
        this.phase = result.drained && result.flushed && result.closed ? "stopped" : "teardown_failed"
        this.lastStatus = {
          state: "unavailable",
          reason: result.drained && result.flushed && result.closed ? "stopped" : "teardown_failed",
        }
        return result
      }
      const result = await closeHost(host, remaining(deadline))
      this.lastTeardown = result
      if (result.closed) {
        this.retained = undefined
        if (this.host === host) this.host = undefined
        if (this.current === current) this.current = undefined
      } else if (host.activation || host.activationTask) {
        // isActive is false while Relay is in Closing, so retain on every
        // incomplete close and retry the same close promise/handle later.
        this.retained = host
      }
      this.stopped = result.closed
      const complete = result.drained && result.flushed && result.closed
      this.phase = complete ? "stopped" : "teardown_failed"
      this.lastStatus = { state: "unavailable", reason: complete ? "stopped" : "teardown_failed" }
      return result
    })()
    this.shutdownTask = task
    void task.then(
      (result) => {
        if (!result.closed && this.shutdownTask === task) this.shutdownTask = undefined
      },
      () => {
        if (this.shutdownTask === task) this.shutdownTask = undefined
        this.stopped = false
        this.phase = "teardown_failed"
        this.lastStatus = { state: "unavailable", reason: "teardown_failed" }
      },
    )
    return task
  }

  health(): ProcessHealth {
    const host = this.host ?? this.retained
    if (host && this.phase === "active") this.lastStatus = refreshStatus(host)
    return {
      phase: this.phase,
      owners: this.owners,
      operations: host?.operations ?? 0,
      status: this.lastStatus,
    }
  }
}

function integer(value: number | undefined) {
  if (value === undefined || !Number.isFinite(value) || value < 0) return undefined
  return Math.min(Number.MAX_SAFE_INTEGER, Math.floor(value))
}

function inputContextUtilization(inputTokens: number | undefined, contextLimit: number | undefined) {
  const tokens = integer(inputTokens)
  if (
    tokens === undefined ||
    tokens <= 0 ||
    contextLimit === undefined ||
    !Number.isFinite(contextLimit) ||
    contextLimit <= 0
  )
    return undefined
  const ratio = tokens / contextLimit
  return Number.isFinite(ratio) && ratio >= 0 ? ratio : undefined
}

export function durationBucket(value: number) {
  if (!Number.isFinite(value) || value < 0) return "unknown"
  if (value < 1_000) return "lt_1s"
  if (value < 5_000) return "1s_to_5s"
  if (value < 30_000) return "5s_to_30s"
  if (value < 120_000) return "30s_to_2m"
  if (value < 600_000) return "2m_to_10m"
  return "gte_10m"
}

export function toolDurationBucket(value: number) {
  if (!Number.isFinite(value) || value < 0) return "unknown"
  if (value < 100) return "lt_100ms"
  if (value < 250) return "100ms_to_250ms"
  if (value < 500) return "250ms_to_500ms"
  if (value < 1_000) return "500ms_to_1s"
  if (value < 2_000) return "1s_to_2s"
  if (value < 5_000) return "2s_to_5s"
  if (value < 10_000) return "5s_to_10s"
  if (value < 30_000) return "10s_to_30s"
  return "gte_30s"
}

export function providerFamily(value: string) {
  const provider = value.trim().toLowerCase()
  if (!provider) return "unknown"
  const patterns: ReadonlyArray<readonly [string, string]> = [
    ["github-copilot", "github"],
    ["openrouter", "openrouter"],
    ["opencode", "opencode"],
    ["amazon-bedrock", "amazon"],
    ["bedrock", "amazon"],
    ["google-vertex", "google"],
    ["vertex", "google"],
    ["nvidia", "nvidia"],
    ["perplexity", "perplexity"],
    ["cerebras", "cerebras"],
    ["together", "together"],
    ["cohere", "cohere"],
    ["groq", "groq"],
    ["moonshot", "moonshot"],
    ["alibaba", "alibaba"],
    ["dashscope", "alibaba"],
    ["gitlab", "gitlab"],
    ["venice", "venice"],
    ["anthropic", "anthropic"],
    ["azure", "azure"],
    ["openai", "openai"],
    ["google", "google"],
    ["xai", "xai"],
    ["meta", "meta"],
  ]
  const known = patterns.find(([pattern]) => provider.includes(pattern))?.[1]
  if (known) return known
  if (["ollama", "lmstudio", "vllm", "local"].some((pattern) => provider.includes(pattern))) return "local"
  return "custom"
}

export function modelFamily(value: string) {
  const model = value.trim().toLowerCase()
  if (!model) return "unknown"
  const patterns: ReadonlyArray<readonly [RegExp, string]> = [
    [/claude/, "claude"],
    [/gemini/, "gemini"],
    [/gemma/, "gemma"],
    [/nemotron/, "nemotron"],
    [/(^|[/_-])o1($|[/_.-])/, "o1"],
    [/(^|[/_-])o3($|[/_.-])/, "o3"],
    [/(^|[/_-])o4($|[/_.-])/, "o4"],
    [/(^|[/_-])gpt|codex/, "gpt"],
    [/llama/, "llama"],
    [/qwen/, "qwen"],
    [/deepseek/, "deepseek"],
    [/mistral|codestral|ministral/, "mistral"],
    [/grok/, "grok"],
    [/(^|[/_.-])glm/, "glm"],
    [/kimi|moonshot/, "kimi"],
    [/minimax/, "minimax"],
    [/(^|[/_.-])mimo/, "mimo"],
    [/(^|[/_.-])nova/, "nova"],
    [/(^|[/_.-])step/, "step"],
    [/trinity/, "trinity"],
    [/(^|[/_.-])muse/, "muse"],
  ]
  return patterns.find(([pattern]) => pattern.test(model))?.[1] ?? "custom"
}

export function llmOperation(value: string | undefined): LlmOperation {
  const protocol = value?.trim().toLowerCase().replaceAll("_", "-")
  if (!protocol) return "unknown"
  if (protocol === "openai-chat" || protocol === "openai-compatible-chat") return "openai.chat_completions"
  if (protocol === "openai-responses" || protocol === "openai-responses-websocket") return "openai.responses"
  if (protocol === "anthropic-messages") return "anthropic.messages"
  if (protocol === "gemini") return "google.generate_content"
  if (protocol === "bedrock-converse") return "aws.converse"
  if (protocol === "openrouter" || protocol === "openrouter-chat") return "openrouter.chat_completions"
  return "unknown"
}

export function toolCategory(value: string | undefined): ToolCategory {
  const name = value?.toLowerCase()
  if (!name) return "unknown"
  if (["shell", "bash"].includes(name)) return "terminal"
  if (["task", "delegate", "delegate_task"].includes(name)) return "delegation"
  if (["webfetch", "web_fetch", "websearch", "web_search"].includes(name)) return "web"
  if (["todo", "todowrite", "todo_write", "plan", "plan_exit"].includes(name)) return "planning"
  if (["question", "ask_question", "request_user_input"].includes(name)) return "human_input"
  if (["skill", "read_skill"].includes(name)) return "skill"
  if (["lsp", "code_search", "grep", "glob"].includes(name)) return "code_search"
  if (["read", "list", "ls"].includes(name)) return "file_read"
  if (["write", "edit", "apply_patch", "patch"].includes(name)) return "file_write"
  if (name === "execute") return "code_execution"
  if (["list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"].includes(name)) return "mcp"
  if (name === "invalid") return "other"
  return "extension"
}

export function terminalResultFamily(input: unknown): TerminalResultFamily {
  if (!isRecord(input)) return "unknown"
  if (input.aborted === true) return "aborted"
  if (input.timeout === true) return "timeout"
  if (input.signal === true || (typeof input.signal === "string" && input.signal.length > 0)) return "signal"
  if (typeof input.exit !== "number" || !Number.isFinite(input.exit)) return "unknown"
  return input.exit === 0 ? "zero_exit" : "nonzero_exit"
}

export function permissionFamily(value: string | undefined): PermissionFamily {
  const action = value?.trim().toLowerCase()
  if (!action) return "unknown"
  if (["read", "write", "edit", "apply_patch", "glob", "grep", "list", "external_directory"].includes(action))
    return "filesystem"
  if (["bash", "shell", "terminal", "execute"].includes(action)) return "terminal"
  if (["webfetch", "web_fetch", "websearch", "web_search", "network"].includes(action)) return "network"
  if (["task", "delegate", "delegate_task"].includes(action)) return "delegation"
  if (["question", "ask_question", "request_user_input"].includes(action)) return "interaction"
  if (["doom_loop", "policy", "guardrail"].includes(action)) return "safety"
  if (["mcp", "list_mcp_resources", "list_mcp_resource_templates", "read_mcp_resource"].includes(action)) return "mcp"
  return "other"
}

export function finishReason(value: string | undefined) {
  const finish = value?.toLowerCase().replaceAll("_", "-")
  if (!finish) return "unknown"
  if (finish === "tool-calls") return "tool_calls"
  if (["stop", "length", "content-filter", "error"].includes(finish)) return finish.replace("-", "_")
  return "unknown"
}

function firstOutputKind(event: LlmEventLike): LlmOutputKind | undefined {
  if (event.type === "text-delta") return event.text ? "text" : undefined
  if (event.type === "reasoning-delta") return event.text ? "reasoning" : undefined
  if (event.type === "tool-input-delta") return event.text ? "tool" : undefined
  if (event.type === "tool-call") return "tool"
  return undefined
}

function providerErrorClassification(value: string | undefined) {
  if (!value) return "unknown"
  if (value.toLowerCase().replaceAll("_", "-") === "context-overflow") return "context_overflow"
  return "other"
}

function retryable(value: boolean | undefined) {
  return value === undefined ? "unknown" : value ? "true" : "false"
}

function usage(value: UsageLike | undefined): TokenUsage | undefined {
  if (!value) return undefined
  const result = {
    inputTotal: integer(value.inputTokens),
    inputNonCached: integer(value.nonCachedInputTokens),
    inputCacheRead: integer(value.cacheReadInputTokens),
    inputCacheWrite: integer(value.cacheWriteInputTokens),
    outputTotal: integer(value.outputTokens),
    outputReasoning: integer(value.reasoningTokens),
  }
  return Object.values(result).some((token) => token !== undefined) ? result : undefined
}

function addUsage(left: TokenUsage | undefined, right: TokenUsage | undefined): TokenUsage | undefined {
  if (!left) return right
  if (!right) return left
  const add = (a: number | undefined, b: number | undefined) =>
    a === undefined && b === undefined ? undefined : integer((a ?? 0) + (b ?? 0))
  return {
    inputTotal: add(left.inputTotal, right.inputTotal),
    inputNonCached: add(left.inputNonCached, right.inputNonCached),
    inputCacheRead: add(left.inputCacheRead, right.inputCacheRead),
    inputCacheWrite: add(left.inputCacheWrite, right.inputCacheWrite),
    outputTotal: add(left.outputTotal, right.outputTotal),
    outputReasoning: add(left.outputReasoning, right.outputReasoning),
  }
}

function classifyExit<A, E>(
  exit: Exit.Exit<A, E>,
  classify?: (exit: Exit.Exit<A, E>) => TurnOutcome | undefined,
): TurnOutcome {
  if (classify) {
    try {
      const outcome = classify(exit)
      if (outcome) return outcome
    } catch {
      // A telemetry classifier must never change host execution.
    }
  }
  if (Exit.isSuccess(exit)) return "success"
  return Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : "failed"
}

function classifyPermissionWait<A, E>(
  exit: Exit.Exit<A, E>,
  classify?: (exit: Exit.Exit<A, E>) => PermissionResolution | undefined,
): PermissionResolution {
  if (classify) {
    try {
      const resolution = classify(exit)
      if (resolution) return resolution
    } catch {
      // A telemetry classifier must never change host execution.
    }
  }
  return Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : "unknown"
}

function classifyQuestionWait<A, E>(
  exit: Exit.Exit<A, E>,
  classify?: (exit: Exit.Exit<A, E>) => QuestionResolution | undefined,
): QuestionResolution {
  if (classify) {
    try {
      const resolution = classify(exit)
      if (resolution) return resolution
    } catch {
      // A telemetry classifier must never change host execution.
    }
  }
  return Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : "unknown"
}

function tracing(relay: RelayRuntime): RelayTracing | undefined {
  if (
    !relay.ScopeType ||
    typeof relay.createScopeStack !== "function" ||
    typeof relay.capturePropagationContext !== "function" ||
    typeof relay.createScopeStackFromPropagation !== "function" ||
    typeof relay.withScopeStack !== "function" ||
    typeof relay.pushScope !== "function" ||
    typeof relay.popScope !== "function" ||
    typeof relay.llmCall !== "function" ||
    typeof relay.llmCallEnd !== "function" ||
    typeof relay.toolCall !== "function" ||
    typeof relay.toolCallEnd !== "function" ||
    typeof relay.event !== "function"
  )
    return undefined
  return relay as RelayMetrics & RelayTracing
}

function wallMicros() {
  return Date.now() * 1_000
}

function otelStatus(outcome: LlmOutcome | TurnOutcome | ToolOutcome) {
  if (outcome === "success") return "OK"
  if (["failed", "provider_error", "blocked"].includes(outcome)) return "ERROR"
  return "UNSET"
}

function traceErrorType(outcome: LlmOutcome | TurnOutcome | ToolOutcome) {
  return otelStatus(outcome) === "ERROR" ? `opencode.${outcome}` : undefined
}

function make(host: Host, lifecycle?: Lifecycle, hooks?: TestingHooks): Interface {
  if (!host.relay || host.status.state !== "active") return { ...noop, status: host.status }
  const relay = host.relay
  const trace = tracing(relay)

  const branchStack = (parent: RelayScopeContext | undefined) => {
    if (!trace || !parent) return trace?.createScopeStack()
    const propagation = trace.withScopeStack(parent.stack, () => trace.capturePropagationContext())
    return trace.createScopeStackFromPropagation({ ...propagation, parentUuid: parent.handle.uuid })
  }

  const beginRunTrace = (input: RunStarted, parent: RelayScopeContext | undefined) => {
    if (!trace) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.runtime": input.runtime,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.pushScope(
          "opencode.agent.run",
          trace.ScopeType.Agent,
          null,
          null,
          null,
          metadata,
          { runtime: input.runtime },
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ScopeHandle>
    } catch {
      return undefined
    }
  }

  const endRunTrace = (scope: RelayScopeContext<ScopeHandle> | undefined, input: RunCompleted) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        trace.withScopeStack(scope.stack, () =>
          trace.popScope(scope.handle, { outcome: input.outcome, operations: input.operations }, wallMicros(), {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.outcome": input.outcome,
            "opencode.duration_bucket": durationBucket(input.durationMs),
            "otel.status_code": otelStatus(input.outcome),
            ...(traceErrorType(input.outcome) ? { "error.type": traceErrorType(input.outcome) } : {}),
          }),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const beginTurnTrace = (input: TurnStarted, parent: RelayScopeContext | undefined) => {
    if (!trace) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.runtime": input.runtime,
        "opencode.call_role": input.role,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.pushScope(
          "opencode.agent.turn",
          trace.ScopeType.Agent,
          null,
          null,
          null,
          metadata,
          { runtime: input.runtime, call_role: input.role },
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ScopeHandle>
    } catch {
      return undefined
    }
  }

  const endTurnTrace = (
    scope: RelayScopeContext<ScopeHandle> | undefined,
    input: TurnStarted & { readonly outcome: TurnOutcome; readonly durationMs: number },
  ) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        trace.withScopeStack(scope.stack, () =>
          trace.popScope(scope.handle, { outcome: input.outcome }, wallMicros(), {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.outcome": input.outcome,
            "opencode.duration_bucket": durationBucket(input.durationMs),
            "otel.status_code": otelStatus(input.outcome),
            ...(traceErrorType(input.outcome) ? { "error.type": traceErrorType(input.outcome) } : {}),
          }),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const beginLlmTrace = (input: LlmStreamStarted, parent: RelayScopeContext | undefined, mode: LlmMode) => {
    if (!trace) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const route = {
        provider_family: providerFamily(input.provider),
        model_family: modelFamily(input.model),
        operation: llmOperation(input.protocol),
      }
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.call_role": input.role,
        "opencode.agent_runtime": input.agentRuntime,
        "opencode.llm_runtime": input.runtime,
        "opencode.llm_mode": mode,
        "opencode.provider_family": route.provider_family,
        "opencode.model_family": route.model_family,
        "opencode.llm_operation": route.operation,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.llmCall(
          "opencode.llm",
          {
            headers: {},
            content: {
              call_role: input.role,
              agent_runtime: input.agentRuntime,
              llm_runtime: input.runtime,
              llm_mode: mode,
              ...route,
            },
          },
          null,
          null,
          null,
          metadata,
          route.model_family,
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<LlmHandle>
    } catch {
      return undefined
    }
  }

  const endLlmTrace = (
    scope: RelayScopeContext<LlmHandle> | undefined,
    input: LlmStreamCompleted | LlmUnaryCompleted,
    mode: LlmMode,
  ) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        const inputTokens = integer(input.tokens?.inputTotal)
        const outputTokens = integer(input.tokens?.outputTotal)
        const totalTokens =
          inputTokens === undefined && outputTokens === undefined ? undefined : (inputTokens ?? 0) + (outputTokens ?? 0)
        const traceUsage = {
          ...(inputTokens === undefined ? {} : { input_tokens: inputTokens }),
          ...(outputTokens === undefined ? {} : { output_tokens: outputTokens }),
          ...(totalTokens === undefined ? {} : { total_tokens: totalTokens }),
          ...(integer(input.tokens?.inputCacheRead) === undefined
            ? {}
            : { cached_tokens: integer(input.tokens?.inputCacheRead) }),
          ...(integer(input.tokens?.inputCacheWrite) === undefined
            ? {}
            : { cache_write_tokens: integer(input.tokens?.inputCacheWrite) }),
          ...(integer(input.tokens?.inputNonCached) === undefined
            ? {}
            : { uncached_input_tokens: integer(input.tokens?.inputNonCached) }),
        }
        trace.withScopeStack(scope.stack, () =>
          trace.llmCallEnd(
            scope.handle,
            {
              outcome: input.outcome,
              finish_reason: finishReason(input.finish),
              model: modelFamily(input.model),
              usage: traceUsage,
            },
            null,
            {
              "opencode.trace.schema_version": SCHEMA_VERSION,
              "opencode.llm_mode": mode,
              "opencode.outcome": input.outcome,
              "opencode.finish_reason": finishReason(input.finish),
              "opencode.duration_bucket": durationBucket(input.durationMs),
              "otel.status_code": otelStatus(input.outcome),
              ...(traceErrorType(input.outcome) ? { "error.type": traceErrorType(input.outcome) } : {}),
              ...("firstOutput" in input && input.firstOutput
                ? {
                    "opencode.first_output_kind": input.firstOutput.kind,
                    "opencode.first_output_latency_bucket": toolDurationBucket(input.firstOutput.latencyMs),
                  }
                : {}),
              ...("providerError" in input && input.providerError
                ? {
                    "opencode.provider_error_classification": providerErrorClassification(
                      input.providerError.classification,
                    ),
                    "opencode.provider_error_retryable": retryable(input.providerError.retryable),
                  }
                : {}),
            },
            wallMicros(),
          ),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const beginToolTrace = (input: ToolStarted, parent: RelayScopeContext | undefined) => {
    if (!trace) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const category = input.category ?? toolCategory(input.name)
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.tool_category": category,
        "opencode.tool_execution": input.execution,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.toolCall(
          "opencode.tool",
          { category, execution: input.execution },
          null,
          null,
          null,
          metadata,
          null,
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ToolHandle>
    } catch {
      return undefined
    }
  }

  const endToolTrace = (scope: RelayScopeContext<ToolHandle> | undefined, input: ToolCompleted) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        const category = input.category ?? toolCategory(input.name)
        const terminalResult = input.execution === "local" && category === "terminal" ? input.terminalResult : undefined
        trace.withScopeStack(scope.stack, () =>
          trace.toolCallEnd(
            scope.handle,
            {
              result: {
                outcome: input.outcome,
                ...(terminalResult === undefined ? {} : { terminal_result_family: terminalResult }),
              },
            },
            null,
            {
              "opencode.trace.schema_version": SCHEMA_VERSION,
              "opencode.outcome": input.outcome,
              ...(terminalResult === undefined ? {} : { "opencode.terminal_result_family": terminalResult }),
              "otel.status_code": otelStatus(input.outcome),
              ...(traceErrorType(input.outcome) ? { "error.type": traceErrorType(input.outcome) } : {}),
              ...(input.durationMs === undefined
                ? {}
                : { "opencode.duration_bucket": toolDurationBucket(input.durationMs) }),
            },
            wallMicros(),
          ),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const permissionEvaluationTrace = (parent: RelayScopeContext | undefined, input: PermissionEvaluation) =>
    Effect.sync(() => {
      if (!trace || !parent || trace.ScopeType.Guardrail === undefined) return
      try {
        const stack = branchStack(parent)
        if (!stack) return
        const started = wallMicros()
        const handle = trace.withScopeStack(stack, () =>
          trace.pushScope(
            "opencode.permission.evaluated",
            trace.ScopeType.Guardrail,
            null,
            null,
            null,
            {
              "opencode.trace.schema_version": SCHEMA_VERSION,
              "opencode.runtime": input.runtime,
              "opencode.permission_family": input.family,
            },
            { runtime: input.runtime, permission_family: input.family },
            started,
          ),
        )
        trace.withScopeStack(stack, () =>
          trace.popScope(handle, { effect: input.effect }, wallMicros(), {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.permission_effect": input.effect,
            "otel.status_code": input.effect === "allow" ? "OK" : input.effect === "deny" ? "ERROR" : "UNSET",
            ...(input.effect === "deny" ? { "error.type": "opencode.permission.denied" } : {}),
          }),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const beginPermissionWaitTrace = (input: PermissionWaitStarted, parent: RelayScopeContext | undefined) => {
    if (!trace || trace.ScopeType.Guardrail === undefined) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.runtime": input.runtime,
        "opencode.permission_family": input.family,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.pushScope(
          "opencode.permission.wait",
          trace.ScopeType.Guardrail,
          null,
          null,
          null,
          metadata,
          { runtime: input.runtime, permission_family: input.family },
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ScopeHandle>
    } catch {
      return undefined
    }
  }

  const endPermissionWaitTrace = (scope: RelayScopeContext<ScopeHandle> | undefined, input: PermissionWaitCompleted) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        trace.withScopeStack(scope.stack, () =>
          trace.popScope(scope.handle, { resolution: input.resolution }, wallMicros(), {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.permission_resolution": input.resolution,
            "opencode.duration_bucket": toolDurationBucket(input.durationMs),
            "otel.status_code": ["reject", "corrected"].includes(input.resolution)
              ? "ERROR"
              : ["once", "always"].includes(input.resolution)
                ? "OK"
                : "UNSET",
            ...(["reject", "corrected"].includes(input.resolution)
              ? { "error.type": `opencode.permission.${input.resolution}` }
              : {}),
          }),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const beginQuestionWaitTrace = (input: QuestionWaitStarted, parent: RelayScopeContext | undefined) => {
    if (!trace || trace.ScopeType.Function === undefined) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const metadata = {
        "opencode.trace.schema_version": SCHEMA_VERSION,
        "opencode.runtime": input.runtime,
      }
      const handle = trace.withScopeStack(stack, () =>
        trace.pushScope(
          "opencode.question.wait",
          trace.ScopeType.Function,
          null,
          null,
          null,
          metadata,
          { runtime: input.runtime },
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ScopeHandle>
    } catch {
      return undefined
    }
  }

  const endQuestionWaitTrace = (scope: RelayScopeContext<ScopeHandle> | undefined, input: QuestionWaitCompleted) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      try {
        trace.withScopeStack(scope.stack, () =>
          trace.popScope(scope.handle, { resolution: input.resolution }, wallMicros(), {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.question_resolution": input.resolution,
            "opencode.duration_bucket": toolDurationBucket(input.durationMs),
            "otel.status_code": input.resolution === "answered" ? "OK" : "UNSET",
          }),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const retryTrace = (
    scope: RelayScopeContext | undefined,
    input: {
      readonly runtime: TurnRuntime
      readonly attempt: number
      readonly delayMs?: number
      readonly delaySource?: HostRetryDelaySource
      readonly errorKind?: HostRetryErrorKind
    },
  ) =>
    Effect.sync(() => {
      if (!trace || !scope) return
      const handle = scope.handle
      if (!isScopeEventParent(handle)) return
      try {
        trace.withScopeStack(scope.stack, () =>
          trace.event(
            "opencode.llm.host_retry.scheduled",
            handle,
            {
              runtime: input.runtime,
              attempt: input.attempt <= 1 ? "first" : input.attempt === 2 ? "second" : "third_or_later",
              delay_source: input.delaySource ?? "unknown",
              error_kind: input.errorKind ?? "unknown",
              ...(input.delayMs === undefined ? {} : { delay_bucket: toolDurationBucket(input.delayMs) }),
            },
            { "opencode.trace.schema_version": SCHEMA_VERSION },
            wallMicros(),
          ),
        )
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  type ProviderAttemptTraceEntry = {
    readonly scope: RelayScopeContext<ScopeHandle>
    readonly startedAt: number
    readonly completion?: {
      readonly event: Extract<AttemptEvent, { readonly type: "completed" }>
      readonly timestamp: number
    }
  }

  type ProviderAttemptTraceState = Map<AttemptOrdinal, ProviderAttemptTraceEntry>

  const beginProviderAttemptTrace = (parent: RelayScopeContext | undefined, attempt: AttemptOrdinal) => {
    if (!trace || !parent || trace.ScopeType.Function === undefined) return undefined
    try {
      const stack = branchStack(parent)
      if (!stack) return undefined
      const handle = trace.withScopeStack(stack, () =>
        trace.pushScope(
          "opencode.llm.provider_attempt",
          trace.ScopeType.Function,
          null,
          null,
          null,
          {
            "opencode.trace.schema_version": SCHEMA_VERSION,
            "opencode.provider_attempt": attempt,
          },
          { attempt },
          wallMicros(),
        ),
      )
      return { stack, handle } satisfies RelayScopeContext<ScopeHandle>
    } catch {
      return undefined
    }
  }

  const endProviderAttemptTrace = (
    scope: RelayScopeContext<ScopeHandle>,
    event: Extract<AttemptEvent, { readonly type: "completed" }>,
    timestamp = wallMicros(),
  ) => {
    if (!trace) return
    const errorKind = "errorKind" in event ? event.errorKind : "none"
    const retryableValue = "retryable" in event ? retryable(event.retryable) : "unknown"
    trace.withScopeStack(scope.stack, () =>
      trace.popScope(
        scope.handle,
        {
          outcome: event.outcome,
          status_family: event.statusFamily,
          error_kind: errorKind,
          retryable: retryableValue,
          will_retry: event.willRetry ? "true" : "false",
        },
        timestamp,
        {
          "opencode.trace.schema_version": SCHEMA_VERSION,
          "opencode.outcome": event.outcome,
          "opencode.status_family": event.statusFamily,
          "opencode.error_kind": errorKind,
          "opencode.retryable": retryableValue,
          "opencode.will_retry": event.willRetry ? "true" : "false",
          "opencode.duration_bucket": toolDurationBucket(event.durationMs),
          "otel.status_code": event.outcome === "failed" ? "ERROR" : event.outcome === "success" ? "OK" : "UNSET",
          ...(event.outcome === "failed" ? { "error.type": `opencode.provider_attempt.${errorKind}` } : {}),
        },
      ),
    )
  }

  const providerAttemptTrace = (
    state: ProviderAttemptTraceState,
    parent: RelayScopeContext | undefined,
    event: AttemptEvent,
  ) =>
    Effect.sync(() => {
      if (!trace || !parent) return
      try {
        if (event.type === "started") {
          const existing = state.get(event.attempt)
          if (existing) {
            if (existing.completion) {
              endProviderAttemptTrace(existing.scope, existing.completion.event, existing.completion.timestamp)
            } else {
              endProviderAttemptTrace(existing.scope, {
                type: "completed",
                attempt: event.attempt,
                outcome: "failed",
                durationMs: Math.max(0, performance.now() - existing.startedAt),
                statusFamily: "none",
                errorKind: "unknown",
                retryable: false,
                willRetry: false,
              })
            }
            state.delete(event.attempt)
          }
          const scope = beginProviderAttemptTrace(parent, event.attempt)
          if (scope) state.set(event.attempt, { scope, startedAt: performance.now() })
          return
        }

        const entry = state.get(event.attempt)
        if (!entry) return

        if (event.type === "completed") {
          const timestamp = wallMicros()
          if (event.willRetry) {
            state.set(event.attempt, { ...entry, completion: { event, timestamp } })
            return
          }
          endProviderAttemptTrace(entry.scope, event, timestamp)
          state.delete(event.attempt)
          return
        }

        const timestamp = wallMicros()
        if (!entry.completion) return
        const handle = entry.scope.handle
        if (isScopeEventParent(handle)) {
          trace.withScopeStack(entry.scope.stack, () =>
            trace.event(
              "opencode.llm.provider_retry.scheduled",
              handle,
              {
                attempt: event.attempt,
                next_attempt: event.nextAttempt,
                delay_source: event.delaySource,
                delay_bucket: toolDurationBucket(event.delayMs),
              },
              { "opencode.trace.schema_version": SCHEMA_VERSION },
              timestamp,
            ),
          )
        }
        endProviderAttemptTrace(entry.scope, entry.completion.event, timestamp)
        state.delete(event.attempt)
      } catch {
        // Tracing is observation-only and must never alter host execution.
      }
    })

  const finalizeProviderAttemptTraces = (state: ProviderAttemptTraceState, outcome: LlmOutcome) =>
    Effect.sync(() => {
      for (const [attempt, entry] of state) {
        try {
          if (entry.completion) {
            endProviderAttemptTrace(entry.scope, entry.completion.event, entry.completion.timestamp)
          } else {
            const durationMs = Math.max(0, performance.now() - entry.startedAt)
            if (outcome === "cancelled") {
              endProviderAttemptTrace(entry.scope, {
                type: "completed",
                attempt,
                outcome: "cancelled",
                durationMs,
                statusFamily: "none",
                willRetry: false,
              })
            } else {
              endProviderAttemptTrace(entry.scope, {
                type: "completed",
                attempt,
                outcome: "failed",
                durationMs,
                statusFamily: "none",
                errorKind: "unknown",
                retryable: false,
                willRetry: false,
              })
            }
          }
        } catch {
          // Tracing is observation-only and must never alter host execution.
        } finally {
          state.delete(attempt)
        }
      }
    })

  const emit = (name: string, measurements: ReadonlyArray<MetricMeasurement>) =>
    Effect.try({
      try: () => relay.metric(name, measurements, null, { "opencode.metric.schema_version": SCHEMA_VERSION }, null),
      catch: () => "metric_failed" as const,
    }).pipe(
      Effect.catch(() => Effect.logWarning("NeMo Relay metric emission failed", { metric: name })),
      Effect.asVoid,
    )

  const attributes = (value: MetricAttributes = {}) => ({
    ...value,
    "opencode.metric.schema_version": SCHEMA_VERSION,
  })

  const counter = (name: string, value: number, input?: MetricAttributes): MetricMeasurement => ({
    name,
    kind: relay.MetricKind.Counter,
    valueType: relay.MetricValueType.U64,
    value,
    unit: `{${name.endsWith("tokens") ? "token" : "event"}}`,
    attributes: attributes(input),
  })

  const floatCounter = (name: string, value: number, input?: MetricAttributes, unit?: string): MetricMeasurement => ({
    name,
    kind: relay.MetricKind.Counter,
    valueType: relay.MetricValueType.F64,
    value,
    unit,
    attributes: attributes(input),
  })

  const histogram = (
    name: string,
    value: number,
    boundaries: ReadonlyArray<number>,
    input?: MetricAttributes,
    unit = "ms",
  ): MetricMeasurement => ({
    name,
    kind: relay.MetricKind.Histogram,
    valueType: relay.MetricValueType.F64,
    value: Number.isFinite(value) && value >= 0 ? value : 0,
    unit,
    boundaries,
    attributes: attributes(input),
  })

  const runStarted = (input: RunStarted) =>
    hooks?.runStarted
      ? hooks.runStarted(input)
      : emit("opencode.agent.run.started", [counter("opencode.agent.run.started.count", 1, { runtime: input.runtime })])

  const runCompleted = (input: RunCompleted) => {
    if (hooks?.runCompleted) return hooks.runCompleted(input)
    const base = { runtime: input.runtime, outcome: input.outcome }
    return emit("opencode.agent.run.completed", [
      counter("opencode.agent.run.completed.count", 1, base),
      histogram(
        "opencode.agent.run.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        base,
      ),
      ...(
        [
          ["turn", input.operations.turns],
          ["llm_operation", input.operations.llmOperations],
          ["tool_call", input.operations.toolCalls],
          ["host_retry", input.operations.hostRetries],
          ["provider_attempt", input.operations.providerAttempts],
          ["provider_retry", input.operations.providerRetries],
          ["permission_wait", input.operations.permissionWaits],
          ["question_wait", input.operations.questionWaits],
        ] as const
      ).map(([kind, value]) =>
        histogram(
          "opencode.agent.run.operation_count",
          value,
          [0, 1, 2, 3, 5, 10, 20, 50, 100],
          { ...base, kind },
          "{operation}",
        ),
      ),
    ])
  }

  const llmStreamCompleted = (input: LlmStreamCompleted) => {
    if (hooks?.llmStreamCompleted) return hooks.llmStreamCompleted(input)
    const route = {
      provider_family: providerFamily(input.provider),
      model_family: modelFamily(input.model),
      operation: llmOperation(input.protocol),
    }
    const base = {
      call_role: input.role,
      agent_runtime: input.agentRuntime,
      llm_runtime: input.runtime,
      llm_mode: "stream",
      ...route,
    }
    const measurements = [
      counter("opencode.llm.host_stream.count", 1, { ...base, outcome: input.outcome }),
      histogram(
        "opencode.llm.host_stream.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        {
          ...base,
          outcome: input.outcome,
        },
      ),
      counter("opencode.model_route.count", 1, base),
    ]
    if (input.finish)
      measurements.push(
        counter("opencode.llm.finish_reason.count", 1, {
          ...base,
          finish_reason: finishReason(input.finish),
        }),
      )
    if (input.firstOutput)
      measurements.push(
        histogram(
          "opencode.llm.time_to_first_output",
          input.firstOutput.latencyMs,
          [10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000],
          { ...base, outcome: input.outcome, output_kind: input.firstOutput.kind },
        ),
      )
    if (input.providerError)
      measurements.push(
        counter("opencode.llm.provider_error.count", 1, {
          ...base,
          classification: providerErrorClassification(input.providerError.classification),
          retryable: retryable(input.providerError.retryable),
        }),
      )
    const tokens = [
      ["input_total", input.tokens?.inputTotal],
      ["input_non_cached", input.tokens?.inputNonCached],
      ["input_cache_read", input.tokens?.inputCacheRead],
      ["input_cache_write", input.tokens?.inputCacheWrite],
      ["output_total", input.tokens?.outputTotal],
      ["output_reasoning", input.tokens?.outputReasoning],
    ] as const
    for (const [kind, value] of tokens) {
      const count = integer(value)
      if (count === undefined) continue
      measurements.push(counter("opencode.llm.tokens", count, { ...base, kind }))
    }
    for (const utilization of input.inputContextUtilizations ?? []) {
      if (!Number.isFinite(utilization) || utilization <= 0) continue
      measurements.push(
        histogram(
          "opencode.llm.input_context_utilization",
          utilization,
          [0.25, 0.5, 0.75, 0.85, 0.9, 0.95, 1, 1.1],
          { ...base, scope: "provider_step" },
          "1",
        ),
      )
    }
    return emit("opencode.llm.stream.completed", measurements)
  }

  const llmUnaryCompleted = (input: LlmUnaryCompleted) => {
    if (hooks?.llmUnaryCompleted) return hooks.llmUnaryCompleted(input)
    const base = {
      call_role: input.role,
      agent_runtime: input.agentRuntime,
      llm_runtime: input.runtime,
      llm_mode: "unary",
      provider_family: providerFamily(input.provider),
      model_family: modelFamily(input.model),
      operation: llmOperation(input.protocol),
    }
    const measurements = [
      counter("opencode.llm.unary.count", 1, { ...base, outcome: input.outcome }),
      histogram(
        "opencode.llm.unary.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        { ...base, outcome: input.outcome },
      ),
      counter("opencode.model_route.count", 1, base),
    ]
    if (input.finish)
      measurements.push(
        counter("opencode.llm.finish_reason.count", 1, {
          ...base,
          finish_reason: finishReason(input.finish),
        }),
      )
    const tokens = [
      ["input_total", input.tokens?.inputTotal],
      ["input_non_cached", input.tokens?.inputNonCached],
      ["input_cache_read", input.tokens?.inputCacheRead],
      ["input_cache_write", input.tokens?.inputCacheWrite],
      ["output_total", input.tokens?.outputTotal],
      ["output_reasoning", input.tokens?.outputReasoning],
    ] as const
    for (const [kind, value] of tokens) {
      const count = integer(value)
      if (count === undefined) continue
      measurements.push(counter("opencode.llm.tokens", count, { ...base, kind }))
    }
    const utilization = inputContextUtilization(input.tokens?.inputTotal, input.contextLimit)
    if (utilization !== undefined)
      measurements.push(
        histogram(
          "opencode.llm.input_context_utilization",
          utilization,
          [0.25, 0.5, 0.75, 0.85, 0.9, 0.95, 1, 1.1],
          { ...base, scope: "unary_call" },
          "1",
        ),
      )
    return emit("opencode.llm.unary.completed", measurements)
  }

  const llmCostRecorded: Interface["llmCostRecorded"] = (input) => {
    if (hooks?.llmCostRecorded) return hooks.llmCostRecorded(input)
    if (!Number.isFinite(input.costUsd) || input.costUsd < 0) return Effect.void
    return emit("opencode.llm.cost.recorded", [
      floatCounter(
        "opencode.llm.cost_usd",
        input.costUsd,
        {
          call_role: input.role,
          agent_runtime: input.agentRuntime,
          llm_mode: "stream",
          provider_family: providerFamily(input.provider),
          model_family: modelFamily(input.model),
          source: input.source,
          scope: "provider_step",
        },
        "USD",
      ),
    ])
  }

  const turnCompleted: Interface["turnCompleted"] = (input) =>
    hooks?.turnCompleted
      ? hooks.turnCompleted(input)
      : emit("opencode.agent.turn.completed", [
          counter("opencode.agent.turn.count", 1, {
            runtime: input.runtime,
            call_role: input.role,
            outcome: input.outcome,
          }),
          histogram(
            "opencode.agent.turn.duration",
            input.durationMs,
            [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
            {
              runtime: input.runtime,
              call_role: input.role,
              outcome: input.outcome,
            },
          ),
        ])

  const toolCompleted = (input: ToolCompleted) => {
    if (hooks?.toolCompleted) return hooks.toolCompleted(input)
    const category = input.category ?? toolCategory(input.name)
    const measurements = [
      counter("opencode.tool_call.count", 1, {
        category,
        execution: input.execution,
        outcome: input.outcome,
      }),
    ]
    if (input.execution === "local" && category === "terminal" && input.terminalResult !== undefined)
      measurements.push(
        counter("opencode.tool.terminal_result.count", 1, {
          result_family: input.terminalResult,
          outcome: input.outcome,
        }),
      )
    if (input.execution === "local" && input.durationMs !== undefined)
      measurements.push(
        histogram(
          "opencode.tool_call.duration",
          input.durationMs,
          [10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000],
          {
            category,
            outcome: input.outcome,
          },
        ),
      )
    return emit("opencode.tool.completed", measurements)
  }

  const permissionEvaluated: Interface["permissionEvaluated"] = (input) =>
    Effect.gen(function* () {
      const scope = yield* CurrentRelayScope
      yield* emit("opencode.permission.evaluated", [
        counter("opencode.permission.evaluation.count", 1, {
          runtime: input.runtime,
          permission_family: input.family,
          effect: input.effect,
        }),
      ]).pipe(Effect.ensuring(permissionEvaluationTrace(scope, input)))
    })

  const permissionWaitCompleted = (input: PermissionWaitCompleted) => {
    if (hooks?.permissionWaitCompleted) return hooks.permissionWaitCompleted(input)
    const base = {
      runtime: input.runtime,
      permission_family: input.family,
      resolution: input.resolution,
    }
    return emit("opencode.permission.wait.completed", [
      counter("opencode.permission.wait.count", 1, base),
      histogram(
        "opencode.permission.wait.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        base,
      ),
    ])
  }

  const questionWaitCompleted = (input: QuestionWaitCompleted) => {
    if (hooks?.questionWaitCompleted) return hooks.questionWaitCompleted(input)
    const attributes = {
      runtime: input.runtime,
      resolution: input.resolution,
    }
    return emit("opencode.question.wait.completed", [
      counter("opencode.question.wait.count", 1, attributes),
      histogram(
        "opencode.question.wait.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        attributes,
      ),
    ])
  }

  const compactionCompleted: Interface["compactionCompleted"] = (input) => {
    if (hooks?.compactionCompleted) return hooks.compactionCompleted(input)
    const measurements = [counter("opencode.compaction.completed.count", 1, { runtime: input.runtime })]
    const estimates = [
      ["source", input.sourceEstimatedTokens],
      ["summary", input.summaryEstimatedTokens],
      ["retained_recent", input.retainedRecentEstimatedTokens],
    ] as const
    for (const [kind, value] of estimates) {
      const count = integer(value)
      if (count === undefined) continue
      measurements.push(counter("opencode.compaction.estimated_tokens", count, { runtime: input.runtime, kind }))
    }
    return emit("opencode.compaction.completed", measurements)
  }

  const compactionAttemptCompleted: Interface["compactionAttemptCompleted"] = (input) => {
    if (hooks?.compactionAttemptCompleted) return hooks.compactionAttemptCompleted(input)
    const attributes = {
      runtime: input.runtime,
      trigger: input.trigger,
      outcome: input.outcome,
    }
    return emit("opencode.compaction.attempt.completed", [
      counter("opencode.compaction.attempt.count", 1, attributes),
      histogram(
        "opencode.compaction.duration",
        input.durationMs,
        [100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
        attributes,
      ),
    ])
  }

  const providerAttemptObserved = (
    input: LlmStreamStarted,
    event: AttemptEvent,
    scope: RelayScopeContext | undefined,
    traceState: ProviderAttemptTraceState,
  ) =>
    Effect.gen(function* () {
      const run = yield* CurrentRelayRun
      const route = {
        call_role: input.role,
        agent_runtime: input.agentRuntime,
        llm_runtime: input.runtime,
        llm_mode: "stream",
        provider_family: providerFamily(input.provider),
        model_family: modelFamily(input.model),
        operation: llmOperation(input.protocol),
      }
      let publication: Effect.Effect<void>
      if (event.type === "started") {
        if (run) run.providerAttempts++
        publication = emit("opencode.llm.provider_attempt.started", [
          counter("opencode.llm.provider_attempt.started.count", 1, {
            ...route,
            attempt: event.attempt,
          }),
        ])
      } else if (event.type === "retry-scheduled") {
        if (run) run.providerRetries++
        const attributes = {
          ...route,
          attempt: event.attempt,
          next_attempt: event.nextAttempt,
          delay_source: event.delaySource,
        }
        publication = emit("opencode.llm.provider_retry.scheduled", [
          counter("opencode.llm.provider_retry.scheduled.count", 1, attributes),
          histogram(
            "opencode.llm.provider_retry.delay",
            event.delayMs,
            [10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000],
            attributes,
          ),
        ])
      } else {
        const attributes = {
          ...route,
          attempt: event.attempt,
          outcome: event.outcome,
          status_family: event.statusFamily,
          error_kind: "errorKind" in event ? event.errorKind : "none",
          retryable: "retryable" in event ? retryable(event.retryable) : "unknown",
          will_retry: event.willRetry ? "true" : "false",
        }
        publication = emit("opencode.llm.provider_attempt.completed", [
          counter("opencode.llm.provider_attempt.completed.count", 1, attributes),
          histogram(
            "opencode.llm.provider_attempt.time_to_headers",
            event.durationMs,
            [10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000],
            attributes,
          ),
        ])
      }
      yield* publication.pipe(Effect.ensuring(providerAttemptTrace(traceState, scope, event)))
    })

  const admit = () => (lifecycle ? lifecycle.admit(host) : host.accepting)
  const finish = () => {
    if (lifecycle) lifecycle.finish(host)
  }

  const beginTool: Interface["beginTool"] = (input) =>
    Effect.gen(function* () {
      if (!admit()) return noopTool
      if (!lifecycle) host.operations++
      const run = yield* CurrentRelayRun
      if (run) run.toolCalls++
      const parent = yield* CurrentRelayScope
      const traceScope = beginToolTrace(input, parent)
      const started = performance.now()
      let completed = false
      return {
        run(effect) {
          return traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        },
        complete(outcome, details) {
          return Effect.suspend(() => {
            if (completed) return Effect.void
            completed = true
            const category = input.category ?? toolCategory(input.name)
            const completedInput = {
              ...input,
              outcome,
              ...(input.execution === "local" && category === "terminal" && details?.terminalResult !== undefined
                ? { terminalResult: details.terminalResult }
                : {}),
              ...(input.execution === "local" ? { durationMs: performance.now() - started } : {}),
            }
            return toolCompleted(completedInput).pipe(
              Effect.ensuring(endToolTrace(traceScope, completedInput)),
              Effect.ensuring(
                Effect.sync(() => {
                  if (lifecycle) finish()
                  else host.operations = Math.max(0, host.operations - 1)
                }),
              ),
            )
          })
        },
      }
    })

  const observeLlmStream = <A extends LlmEventLike, E, R>(
    input: LlmStreamStarted,
    source: Stream.Stream<A, E, R>,
  ): Stream.Stream<A, E, R> =>
    Stream.unwrap(
      Effect.gen(function* () {
        // Acquire at subscription time: an unconsumed lazy stream must not
        // hold shutdown open, and each subscription is a distinct attempt.
        if (!admit()) return source
        if (!lifecycle) host.operations++
        const run = yield* CurrentRelayRun
        if (run) run.llmOperations++
        const parent = yield* CurrentRelayScope
        const traceScope = beginLlmTrace(input, parent, "stream")
        const providerAttemptTraceState: ProviderAttemptTraceState = new Map()
        const started = performance.now()
        let outcome: LlmOutcome = "unknown"
        let finishValue: string | undefined
        let stepUsage: TokenUsage | undefined
        let finalUsage: TokenUsage | undefined
        const inputContextUtilizations: number[] = []
        let firstOutput: LlmStreamCompleted["firstOutput"]
        let providerError: LlmStreamCompleted["providerError"]
        let terminal = false
        const observedSource =
          input.runtime === "native"
            ? source.pipe(
                Stream.provideService(RequestExecutor.CurrentAttemptObserver, (event) =>
                  providerAttemptObserved(input, event, traceScope, providerAttemptTraceState),
                ),
              )
            : source
        return observedSource.pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              const kind = firstOutputKind(event)
              if (!firstOutput && kind) {
                firstOutput = { kind, latencyMs: performance.now() - started }
              }
              if (event.type === "provider-error") {
                outcome = "provider_error"
                providerError = {
                  classification: event.classification ?? "",
                  retryable: event.retryable,
                }
                terminal = true
                return
              }
              if (event.type === "step-finish") {
                if (outcome !== "provider_error")
                  outcome = event.reason === "error" || event.reason === "content-filter" ? "failed" : "success"
                finishValue = event.reason
                const observedUsage = usage(event.usage)
                stepUsage = addUsage(stepUsage, observedUsage)
                const utilization = inputContextUtilization(observedUsage?.inputTotal, input.contextLimit)
                if (utilization !== undefined) inputContextUtilizations.push(utilization)
                return
              }
              if (event.type === "finish") {
                terminal = true
                if (outcome !== "provider_error")
                  outcome = event.reason === "error" || event.reason === "content-filter" ? "failed" : "success"
                finishValue = event.reason
                finalUsage = usage(event.usage)
              }
            }),
          ),
          Stream.onExit((exit) => {
            if (Exit.isFailure(exit)) {
              if (Cause.hasInterruptsOnly(exit.cause)) {
                if (outcome !== "provider_error") outcome = "cancelled"
              } else if (outcome !== "provider_error") outcome = "failed"
            } else if (!terminal) {
              outcome = "incomplete"
            }
            const completedInput = {
              ...input,
              outcome,
              finish: finishValue,
              durationMs: performance.now() - started,
              tokens: finalUsage ?? stepUsage,
              inputContextUtilizations,
              firstOutput,
              providerError,
            }
            return llmStreamCompleted(completedInput).pipe(
              Effect.ensuring(finalizeProviderAttemptTraces(providerAttemptTraceState, outcome)),
              Effect.ensuring(endLlmTrace(traceScope, completedInput, "stream")),
            )
          }),
          Stream.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      }),
    )

  const observeLlmUnary: Interface["observeLlmUnary"] = (input, effect, project) =>
    Effect.suspend(() => {
      if (!admit()) return effect
      if (!lifecycle) host.operations++
      const started = performance.now()
      return Effect.gen(function* () {
        const run = yield* CurrentRelayRun
        if (run) run.llmOperations++
        const parent = yield* CurrentRelayScope
        const traceScope = beginLlmTrace(input, parent, "unary")
        const observed = traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        return yield* observed.pipe(
          Effect.onExit((exit) => {
            let projected: { readonly finish?: string; readonly tokens?: TokenUsage } = {}
            if (Exit.isSuccess(exit) && project) {
              try {
                projected = project(exit.value)
              } catch {
                // A telemetry projector must never change host execution.
              }
            }
            const completedInput: LlmUnaryCompleted = {
              ...input,
              ...projected,
              outcome: Exit.isSuccess(exit) ? "success" : Cause.hasInterruptsOnly(exit.cause) ? "cancelled" : "failed",
              durationMs: performance.now() - started,
            }
            return llmUnaryCompleted(completedInput).pipe(
              Effect.ensuring(endLlmTrace(traceScope, completedInput, "unary")),
            )
          }),
          Effect.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      })
    })

  const observePermissionWait: Interface["observePermissionWait"] = (input, effect, classify) =>
    Effect.suspend(() => {
      if (!admit()) return effect
      if (!lifecycle) host.operations++
      const started = performance.now()
      return Effect.gen(function* () {
        const run = yield* CurrentRelayRun
        if (run) run.permissionWaits++
        const parent = yield* CurrentRelayScope
        const traceScope = beginPermissionWaitTrace(input, parent)
        const observed = traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        return yield* observed.pipe(
          Effect.onExit((exit) => {
            const completedInput = {
              ...input,
              resolution: classifyPermissionWait(exit, classify),
              durationMs: performance.now() - started,
            }
            return permissionWaitCompleted(completedInput).pipe(
              Effect.ensuring(endPermissionWaitTrace(traceScope, completedInput)),
            )
          }),
          Effect.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      })
    })

  const observeQuestionWait: Interface["observeQuestionWait"] = (input, effect, classify) =>
    Effect.suspend(() => {
      if (!admit()) return effect
      if (!lifecycle) host.operations++
      const started = performance.now()
      return Effect.gen(function* () {
        const run = yield* CurrentRelayRun
        if (run) run.questionWaits++
        const parent = yield* CurrentRelayScope
        const traceScope = beginQuestionWaitTrace(input, parent)
        const observed = traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        return yield* observed.pipe(
          Effect.onExit((exit) => {
            const completedInput = {
              ...input,
              resolution: classifyQuestionWait(exit, classify),
              durationMs: performance.now() - started,
            }
            return questionWaitCompleted(completedInput).pipe(
              Effect.ensuring(endQuestionWaitTrace(traceScope, completedInput)),
            )
          }),
          Effect.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      })
    })

  const observeRun: Interface["observeRun"] = (input, effect, classify) =>
    Effect.suspend(() => {
      if (!admit()) return effect
      if (!lifecycle) host.operations++
      const started = performance.now()
      const operations: MutableRunOperationCounts = {
        turns: 0,
        llmOperations: 0,
        toolCalls: 0,
        hostRetries: 0,
        providerAttempts: 0,
        providerRetries: 0,
        permissionWaits: 0,
        questionWaits: 0,
      }
      return Effect.gen(function* () {
        const parent = yield* CurrentRelayScope
        const traceScope = beginRunTrace(input, parent)
        const scoped = traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        const observed = scoped.pipe(Effect.provideService(CurrentRelayRun, operations))
        yield* runStarted(input).pipe(Effect.catchCause(() => Effect.void))
        return yield* observed.pipe(
          Effect.onExit((exit) => {
            const classified = classifyExit(exit, classify)
            const outcome = classified === "success" ? (operations.lastTurnOutcome ?? classified) : classified
            const completedInput = {
              ...input,
              outcome,
              durationMs: performance.now() - started,
              operations: {
                turns: operations.turns,
                llmOperations: operations.llmOperations,
                toolCalls: operations.toolCalls,
                hostRetries: operations.hostRetries,
                providerAttempts: operations.providerAttempts,
                providerRetries: operations.providerRetries,
                permissionWaits: operations.permissionWaits,
                questionWaits: operations.questionWaits,
              },
            }
            return runCompleted(completedInput).pipe(Effect.ensuring(endRunTrace(traceScope, completedInput)))
          }),
          Effect.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      })
    })

  const observeTurn = <A, E, R>(
    input: TurnStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => TurnOutcome | undefined,
  ): Effect.Effect<A, E, R> =>
    Effect.suspend(() => {
      // Effects are lazy too; lease the actual execution rather than the
      // constructed Effect value.
      if (!admit()) return effect
      if (!lifecycle) host.operations++
      const started = performance.now()
      return Effect.gen(function* () {
        const run = yield* CurrentRelayRun
        if (run) run.turns++
        const parent = yield* CurrentRelayScope
        const traceScope = beginTurnTrace(input, parent)
        const observed = traceScope ? effect.pipe(Effect.provideService(CurrentRelayScope, traceScope)) : effect
        return yield* observed.pipe(
          Effect.onExit((exit) => {
            const outcome = classifyExit(exit, classify)
            if (run) run.lastTurnOutcome = outcome
            const completedInput = {
              ...input,
              outcome,
              durationMs: performance.now() - started,
            }
            return turnCompleted(completedInput).pipe(Effect.ensuring(endTurnTrace(traceScope, completedInput)))
          }),
          Effect.ensuring(
            Effect.sync(() => {
              if (lifecycle) finish()
              else host.operations = Math.max(0, host.operations - 1)
            }),
          ),
        )
      })
    })

  return {
    status: host.status,
    detached: (effect) =>
      effect.pipe(
        Effect.provideService(CurrentRelayScope, undefined),
        Effect.provideService(CurrentRelayRun, undefined),
      ),
    observeRun,
    observeLlmStream,
    observeLlmUnary,
    observeTurn,
    observePermissionWait,
    observeQuestionWait,
    beginTool,
    runStarted,
    runCompleted,
    llmStreamCompleted,
    llmUnaryCompleted,
    llmCostRecorded,
    turnCompleted,
    toolCompleted,
    permissionEvaluated,
    permissionWaitCompleted,
    questionWaitCompleted,
    compactionAttemptCompleted,
    compactionCompleted,
    retryScheduled: (input) =>
      Effect.gen(function* () {
        const run = yield* CurrentRelayRun
        if (run) run.hostRetries++
        const scope = yield* CurrentRelayScope
        const attributes = {
          runtime: input.runtime,
          attempt: input.attempt <= 1 ? "first" : input.attempt === 2 ? "second" : "third_or_later",
          delay_source: input.delaySource ?? "unknown",
          error_kind: input.errorKind ?? "unknown",
        }
        const measurements = [counter("opencode.llm.host_retry_scheduled.count", 1, attributes)]
        if (input.delayMs !== undefined && Number.isFinite(input.delayMs) && input.delayMs >= 0)
          measurements.push(
            histogram(
              "opencode.llm.host_retry.delay",
              input.delayMs,
              [10, 25, 50, 100, 250, 500, 1_000, 2_000, 5_000, 10_000, 30_000, 120_000, 600_000],
              attributes,
            ),
          )
        yield* emit("opencode.llm.host_retry.scheduled", measurements).pipe(Effect.ensuring(retryTrace(scope, input)))
      }),
  }
}

export function makeForTesting(relay: RelayMetrics, hooks?: TestingHooks): Interface {
  const status: Status = {
    state: "active",
    activation: "healthy",
    configuration: "present",
    report: {
      configPathCount: 1,
      componentCount: 1,
      dynamicSelectedCount: 0,
      warningCount: 0,
      errorCount: 0,
      runtimeDiagnosticCount: 0,
      dynamicFailureCount: 0,
    },
  }
  return make(activeHost(status, relay), undefined, hooks)
}

const lifecycle = new Lifecycle({
  loader: defaultLoader,
  environment: defaultEnvironment,
  platform: process.platform,
  arch: process.arch,
  startupTimeoutMs: DEFAULT_STARTUP_TIMEOUT_MS,
})

/** Process-global, privacy-safe Relay adapter health. Exporter delivery must be checked separately. */
export function health() {
  return lifecycle.health()
}

/** Stop admission, drain accepted observations, flush subscribers, and close the plugin host. */
export async function shutdown(timeoutMs = DEFAULT_TEARDOWN_TIMEOUT_MS) {
  return lifecycle.shutdown(timeoutMs)
}

/** Test-only lifecycle factory. It does not mutate the process-global adapter. */
export function createLifecycleForTesting(options: {
  readonly loader: Loader
  readonly environment: Readonly<Record<string, string | undefined>>
  readonly platform?: string
  readonly arch?: string
  readonly startupTimeoutMs?: number
}) {
  const instance = new Lifecycle({
    loader: options.loader,
    environment: () => options.environment,
    platform: options.platform ?? process.platform,
    arch: options.arch ?? process.arch,
    startupTimeoutMs: options.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS,
  })
  return {
    async acquire() {
      return make(await instance.acquire(), instance)
    },
    release: () => instance.release(),
    shutdown: (timeoutMs?: number) => instance.shutdown(timeoutMs),
    health: () => instance.health(),
  }
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const host = yield* Effect.acquireRelease(
      Effect.promise(() => lifecycle.acquire()),
      () => Effect.promise(() => lifecycle.release()).pipe(Effect.catch(() => Effect.void)),
    )
    if (host.status.state === "active")
      yield* Effect.logInfo("NeMo Relay plugin host is active", {
        activation: host.status.activation,
        configuration: host.status.configuration,
        diagnostics: host.status.report.warningCount + host.status.report.errorCount,
      })
    if (host.status.state === "unavailable")
      yield* Effect.logWarning("NeMo Relay is unavailable; continuing without Relay instrumentation", {
        reason: host.status.reason,
      })
    return Service.of(make(host, lifecycle))
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

export const Environment = {
  enabled: ENABLE_ENV,
  pluginsToml: CONFIG_ENV,
  runtimeModule: RUNTIME_MODULE_ENV,
} as const
