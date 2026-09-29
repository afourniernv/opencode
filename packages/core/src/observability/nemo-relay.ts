import { Cause, Context, Effect, Exit, Layer, Stream } from "effect"
import { makeGlobalNode } from "../effect/app-node"

const ENABLE_ENV = "OPENCODE_NEMO_RELAY"
const CONFIG_ENV = "OPENCODE_NEMO_RELAY_PLUGINS_TOML"
const RUNTIME_MODULE_ENV = "OPENCODE_NEMO_RELAY_RUNTIME_MODULE"
const RUNTIME_MODULE = "nemo-relay-node"
const SCHEMA_VERSION = "2"
const DEFAULT_STARTUP_TIMEOUT_MS = 5_000
const DEFAULT_TEARDOWN_TIMEOUT_MS = 5_000

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

type RelayModule = RelayMetrics & {
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
  relay?: RelayMetrics
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
export type LlmOutcome = "success" | "provider_error" | "failed" | "cancelled" | "incomplete" | "unknown"
export type TurnOutcome = "success" | "failed" | "blocked" | "cancelled"
export type ToolOutcome = "success" | "failed" | "blocked" | "cancelled" | "unknown"
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
}

export type LlmStreamCompleted = LlmStreamStarted & {
  readonly outcome: LlmOutcome
  readonly finish?: string
  readonly durationMs: number
  readonly tokens?: TokenUsage
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
}

export interface ToolObservation {
  readonly complete: (outcome: ToolOutcome) => Effect.Effect<void>
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
  readonly reason?: string
  readonly usage?: UsageLike
}

export interface Interface {
  readonly status: Status
  readonly observeLlmStream: <A extends LlmEventLike, E, R>(
    input: LlmStreamStarted,
    source: Stream.Stream<A, E, R>,
  ) => Stream.Stream<A, E, R>
  readonly observeTurn: <A, E, R>(
    input: TurnStarted,
    effect: Effect.Effect<A, E, R>,
    classify?: (exit: Exit.Exit<A, E>) => TurnOutcome | undefined,
  ) => Effect.Effect<A, E, R>
  readonly beginTool: (input: ToolStarted) => Effect.Effect<ToolObservation>
  readonly llmStreamCompleted: (input: LlmStreamCompleted) => Effect.Effect<void>
  readonly turnCompleted: (
    input: TurnStarted & { readonly outcome: TurnOutcome; readonly durationMs: number },
  ) => Effect.Effect<void>
  readonly toolCompleted: (input: ToolCompleted) => Effect.Effect<void>
  readonly retryScheduled: (input: { readonly runtime: TurnRuntime; readonly attempt: number }) => Effect.Effect<void>
}

export type TestingHooks = {
  readonly llmStreamCompleted?: (input: LlmStreamCompleted) => Effect.Effect<void>
  readonly turnCompleted?: (
    input: TurnStarted & { readonly outcome: TurnOutcome; readonly durationMs: number },
  ) => Effect.Effect<void>
  readonly toolCompleted?: (input: ToolCompleted) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/NemoRelay") {}

const disabledStatus: Status = { state: "disabled", reason: "not_requested" }
const noopTool: ToolObservation = { complete: () => Effect.void }
const noop: Interface = {
  status: disabledStatus,
  observeLlmStream: (_input, source) => source,
  observeTurn: (_input, effect) => effect,
  beginTool: () => Effect.succeed(noopTool),
  llmStreamCompleted: () => Effect.void,
  turnCompleted: () => Effect.void,
  toolCompleted: () => Effect.void,
  retryScheduled: () => Effect.void,
}

const defaultLoader: Loader = (specifier) => import(specifier)
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
  if (typeof candidate.flushSubscribers !== "function")
    throw new Error("NeMo Relay runtime module does not expose flushSubscribers")
  if (typeof candidate.initialize !== "function")
    throw new Error("NeMo Relay runtime module does not expose initialize")
  return candidate as RelayModule
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
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

function activeHost(status: Status, relay: RelayMetrics, activation?: PluginHostActivation): Host {
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
    ["anthropic", "anthropic"],
    ["azure", "azure"],
    ["openai", "openai"],
    ["google", "google"],
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
    [/(^|[/_-])gpt|(^|[/_-])o[134]($|[/_.-])|codex/, "gpt"],
    [/llama/, "llama"],
    [/qwen/, "qwen"],
    [/deepseek/, "deepseek"],
    [/mistral|codestral|ministral/, "mistral"],
    [/grok/, "grok"],
    [/(^|[/_.-])glm/, "glm"],
    [/kimi|moonshot/, "kimi"],
  ]
  return patterns.find(([pattern]) => pattern.test(model))?.[1] ?? "custom"
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

export function finishReason(value: string | undefined) {
  const finish = value?.toLowerCase().replaceAll("_", "-")
  if (!finish) return "unknown"
  if (finish === "tool-calls") return "tool_calls"
  if (["stop", "length", "content-filter", "error"].includes(finish)) return finish.replace("-", "_")
  return "unknown"
}

function usage(value: UsageLike | undefined): TokenUsage | undefined {
  if (!value) return undefined
  const result = {
    inputTotal: value.inputTokens,
    inputNonCached: value.nonCachedInputTokens,
    inputCacheRead: value.cacheReadInputTokens,
    inputCacheWrite: value.cacheWriteInputTokens,
    outputTotal: value.outputTokens,
    outputReasoning: value.reasoningTokens,
  }
  return Object.values(result).some((token) => token !== undefined && Number.isFinite(token)) ? result : undefined
}

function addUsage(left: TokenUsage | undefined, right: TokenUsage | undefined): TokenUsage | undefined {
  if (!left) return right
  if (!right) return left
  const add = (a: number | undefined, b: number | undefined) =>
    a === undefined && b === undefined ? undefined : (a ?? 0) + (b ?? 0)
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

function make(host: Host, lifecycle?: Lifecycle, hooks?: TestingHooks): Interface {
  if (!host.relay || host.status.state !== "active") return { ...noop, status: host.status }
  const relay = host.relay

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

  const histogram = (
    name: string,
    value: number,
    boundaries: ReadonlyArray<number>,
    input?: MetricAttributes,
  ): MetricMeasurement => ({
    name,
    kind: relay.MetricKind.Histogram,
    valueType: relay.MetricValueType.F64,
    value: Number.isFinite(value) && value >= 0 ? value : 0,
    unit: "ms",
    boundaries,
    attributes: attributes(input),
  })

  const llmStreamCompleted = (input: LlmStreamCompleted) => {
    if (hooks?.llmStreamCompleted) return hooks.llmStreamCompleted(input)
    const route = {
      provider_family: providerFamily(input.provider),
      model_family: modelFamily(input.model),
    }
    const base = {
      call_role: input.role,
      agent_runtime: input.agentRuntime,
      llm_runtime: input.runtime,
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
    return emit("opencode.llm.stream.completed", measurements)
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
    const category = input.execution === "provider" ? "provider" : (input.category ?? toolCategory(input.name))
    const measurements = [
      counter("opencode.tool_call.count", 1, {
        category,
        execution: input.execution,
        outcome: input.outcome,
      }),
    ]
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

  const admit = () => (lifecycle ? lifecycle.admit(host) : host.accepting)
  const finish = () => {
    if (lifecycle) lifecycle.finish(host)
  }

  const beginTool: Interface["beginTool"] = (input) =>
    Effect.sync(() => {
      if (!admit()) return noopTool
      if (!lifecycle) host.operations++
      const started = performance.now()
      let completed = false
      return {
        complete(outcome) {
          return Effect.suspend(() => {
            if (completed) return Effect.void
            completed = true
            return toolCompleted({
              ...input,
              outcome,
              ...(input.execution === "local" ? { durationMs: performance.now() - started } : {}),
            }).pipe(
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
      Effect.sync(() => {
        // Acquire at subscription time: an unconsumed lazy stream must not
        // hold shutdown open, and each subscription is a distinct attempt.
        if (!admit()) return source
        if (!lifecycle) host.operations++
        const started = performance.now()
        let outcome: LlmOutcome = "unknown"
        let finishValue: string | undefined
        let stepUsage: TokenUsage | undefined
        let finalUsage: TokenUsage | undefined
        let terminal = false
        return source.pipe(
          Stream.tap((event) =>
            Effect.sync(() => {
              if (event.type === "provider-error") {
                outcome = "provider_error"
                terminal = true
                return
              }
              if (event.type === "step-finish") {
                if (outcome !== "provider_error") outcome = event.reason === "error" ? "failed" : "success"
                finishValue = event.reason
                stepUsage = addUsage(stepUsage, usage(event.usage))
                return
              }
              if (event.type === "finish") {
                terminal = true
                if (outcome !== "provider_error") outcome = event.reason === "error" ? "failed" : "success"
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
            return llmStreamCompleted({
              ...input,
              outcome,
              finish: finishValue,
              durationMs: performance.now() - started,
              tokens: finalUsage ?? stepUsage,
            })
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
      return effect.pipe(
        Effect.onExit((exit) =>
          turnCompleted({
            ...input,
            outcome: classifyExit(exit, classify),
            durationMs: performance.now() - started,
          }),
        ),
        Effect.ensuring(
          Effect.sync(() => {
            if (lifecycle) finish()
            else host.operations = Math.max(0, host.operations - 1)
          }),
        ),
      )
    })

  return {
    status: host.status,
    observeLlmStream,
    observeTurn,
    beginTool,
    llmStreamCompleted,
    turnCompleted,
    toolCompleted,
    retryScheduled: (input) =>
      emit("opencode.llm.host_retry.scheduled", [
        counter("opencode.llm.host_retry_scheduled.count", 1, {
          runtime: input.runtime,
          attempt: input.attempt <= 1 ? "first" : input.attempt === 2 ? "second" : "third_or_later",
        }),
      ]),
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
