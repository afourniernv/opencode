import { Context, Effect, Layer } from "effect"
import { makeGlobalNode } from "../effect/app-node"

const ENABLE_ENV = "OPENCODE_NEMO_RELAY"
const CONFIG_ENV = "OPENCODE_NEMO_RELAY_PLUGINS_TOML"
const RUNTIME_MODULE_ENV = "OPENCODE_NEMO_RELAY_RUNTIME_MODULE"
const RUNTIME_MODULE = "nemo-relay-node"
const SCHEMA_VERSION = "1"

type MetricAttributes = Record<string, string | number | boolean>

type MetricMeasurement = {
  readonly name: string
  readonly kind: unknown
  readonly valueType: unknown
  readonly value: number
  readonly unit?: string
  readonly attributes?: MetricAttributes
}

type RelayMetrics = {
  readonly MetricKind: { readonly Counter: unknown }
  readonly MetricValueType: { readonly U64: unknown }
  readonly metric: (
    name: string,
    measurements: ReadonlyArray<MetricMeasurement>,
    handle?: null,
    metadata?: MetricAttributes | null,
    categoryProfile?: null,
  ) => void
  readonly flushSubscribers: () => Promise<void>
}

type PluginHostActivation = {
  readonly isActive: boolean
  readonly close: () => Promise<void>
}

type RelayModule = RelayMetrics & {
  readonly initialize: (
    config: { readonly version: 1; readonly components: readonly [] },
    additionalPluginsToml?: string,
  ) => Promise<PluginHostActivation>
}

type Loader = (specifier: string) => Promise<unknown>

type Host = {
  readonly status: Status
  readonly relay?: RelayMetrics
  readonly activation?: PluginHostActivation
}

export type Status =
  | { readonly state: "disabled" }
  | { readonly state: "active" }
  | { readonly state: "unavailable"; readonly reason: "unsupported_platform" | "load_failed" }

export type Runtime = "native" | "ai_sdk" | "workflow" | "unknown"
export type CallRole = "primary" | "compaction" | "title" | "summary" | "agent_generation" | "other"
export type LlmOutcome = "success" | "provider_error" | "failed" | "cancelled" | "unknown"
export type ToolOutcome = "success" | "failed" | "blocked" | "cancelled" | "unknown"
export type ToolCategory =
  | "code_search"
  | "file_read"
  | "file_write"
  | "terminal"
  | "code_execution"
  | "delegation"
  | "planning"
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

export type LlmCompleted = {
  readonly role: CallRole
  readonly runtime: Runtime
  readonly provider: string
  readonly model: string
  readonly outcome: LlmOutcome
  readonly finish?: string
  readonly durationMs: number
  readonly tokens?: TokenUsage
}

export type ToolCompleted = {
  readonly name?: string
  readonly category?: ToolCategory
  readonly outcome: ToolOutcome
  readonly execution: "local" | "provider"
  readonly durationMs?: number
}

export interface Interface {
  readonly status: Status
  readonly llmCompleted: (input: LlmCompleted) => Effect.Effect<void>
  readonly toolCompleted: (input: ToolCompleted) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/NemoRelay") {}

const noop: Interface = {
  status: { state: "disabled" },
  llmCompleted: () => Effect.void,
  toolCompleted: () => Effect.void,
}

let shared: Promise<Host> | undefined
let references = 0
let teardown: Promise<void> | undefined
let shuttingDown = false

const defaultLoader: Loader = (specifier) => import(specifier)

function relayModule(value: unknown): RelayModule {
  if (typeof value !== "object" || value === null) throw new Error("NeMo Relay runtime module is not an object")
  const candidate = value as Partial<RelayModule>
  if (!candidate.MetricKind || !candidate.MetricValueType || typeof candidate.metric !== "function")
    throw new Error("NeMo Relay runtime module does not expose the metric API")
  if (typeof candidate.flushSubscribers !== "function")
    throw new Error("NeMo Relay runtime module does not expose flushSubscribers")
  if (typeof candidate.initialize !== "function")
    throw new Error("NeMo Relay runtime module does not expose initialize")
  return candidate as RelayModule
}

function enabled() {
  const value = process.env[ENABLE_ENV]?.trim().toLowerCase()
  return value === "1" || value === "true" || value === "yes" || value === "on"
}

function unsupportedPlatform() {
  return process.platform === "darwin" && process.arch === "x64"
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
      },
    ],
    null,
    { "opencode.metric.schema_version": SCHEMA_VERSION },
    null,
  )
}

async function open(loader: Loader = defaultLoader): Promise<Host> {
  if (!enabled()) return { status: { state: "disabled" } }
  const config = process.env[CONFIG_ENV]?.trim()
  if (unsupportedPlatform()) return { status: { state: "unavailable", reason: "unsupported_platform" } }

  try {
    // Keep these specifiers runtime-dynamic. A literal import makes Bun embed every
    // installed native addon in each standalone OpenCode binary.
    const runtimeSpecifier = process.env[RUNTIME_MODULE_ENV]?.trim() || RUNTIME_MODULE
    const relay = relayModule(await loader(runtimeSpecifier))
    const activation = await relay.initialize({ version: 1, components: [] }, config)
    if (!activation || activation.isActive !== true || typeof activation.close !== "function") {
      if (activation && typeof activation.close === "function") await activation.close().catch(() => undefined)
      throw new Error("NeMo Relay plugin activation is not active")
    }
    try {
      activationMetric(relay)
    } catch {
      // Metrics are fail-open; plugin activation remains useful for later managed execution.
    }
    return { status: { state: "active" }, relay, activation }
  } catch {
    return { status: { state: "unavailable", reason: "load_failed" } }
  }
}

async function acquire(loader: Loader = defaultLoader) {
  if (shuttingDown) return { status: { state: "unavailable", reason: "load_failed" } } satisfies Host
  references++
  if (!shared) {
    const previous = teardown
    shared = (async () => {
      if (previous) await previous.catch(() => undefined)
      return open(loader)
    })()
  }
  return shared
}

async function close(host: Host) {
  if (!host.relay || !host.activation) return
  try {
    await host.relay.flushSubscribers()
  } finally {
    await host.activation.close()
  }
}

async function release() {
  references = Math.max(0, references - 1)
  if (references !== 0 || !shared) return
  const current = shared
  shared = undefined
  const task = current.then(close)
  teardown = task
  try {
    await task
  } finally {
    if (teardown === task) teardown = undefined
  }
}

/** Best-effort bounded flush for OpenCode's explicit process-exit path. */
export async function shutdown(timeoutMs = 2_000) {
  shuttingDown = true
  references = 0
  const current = shared
  shared = undefined
  const pending = current ? current.then(close) : teardown
  if (!pending) return
  await Promise.race([
    pending.catch(() => undefined),
    new Promise<void>((resolve) => setTimeout(resolve, Math.max(0, timeoutMs))),
  ])
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
    ["openai", "openai"],
    ["azure", "azure"],
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

function make(host: Host): Interface {
  if (!host.relay) return { ...noop, status: host.status }
  const relay = host.relay

  const emit = (name: string, measurements: ReadonlyArray<MetricMeasurement>) =>
    Effect.try({
      try: () => relay.metric(name, measurements, null, { "opencode.metric.schema_version": SCHEMA_VERSION }, null),
      catch: () => undefined,
    }).pipe(
      Effect.catch(() => Effect.logWarning("NeMo Relay metric emission failed", { metric: name })),
      Effect.asVoid,
    )

  const counter = (name: string, value: number, attributes?: MetricAttributes): MetricMeasurement => ({
    name,
    kind: relay.MetricKind.Counter,
    valueType: relay.MetricValueType.U64,
    value,
    unit: `{${name.endsWith("tokens") ? "token" : "event"}}`,
    attributes,
  })

  return {
    status: host.status,
    llmCompleted: (input) => {
      const route = {
        provider_family: providerFamily(input.provider),
        model_family: modelFamily(input.model),
      }
      const base = { call_role: input.role }
      const measurements = [
        counter("opencode.llm.logical_call.count", 1, {
          ...base,
          runtime: input.runtime,
          outcome: input.outcome,
        }),
        counter("opencode.llm.duration_bucket.count", 1, {
          ...base,
          bucket: durationBucket(input.durationMs),
        }),
        counter("opencode.model_route.count", 1, route),
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
      return emit("opencode.llm.completed", measurements)
    },
    toolCompleted: (input) => {
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
          counter("opencode.tool_call.duration_bucket.count", 1, {
            category,
            bucket: toolDurationBucket(input.durationMs),
          }),
        )
      return emit("opencode.tool.completed", measurements)
    },
  }
}

export function makeForTesting(relay: RelayMetrics): Interface {
  return make({ status: { state: "active" }, relay })
}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const host = yield* Effect.promise(() => acquire())
    if (host.status.state === "unavailable")
      yield* Effect.logWarning("NeMo Relay is unavailable; continuing without Relay instrumentation", {
        reason: host.status.reason,
      })
    yield* Effect.addFinalizer(() => Effect.promise(release).pipe(Effect.catch(() => Effect.void)))
    return Service.of(make(host))
  }),
)

export const node = makeGlobalNode({ service: Service, layer, deps: [] })

export const Environment = {
  enabled: ENABLE_ENV,
  pluginsToml: CONFIG_ENV,
  runtimeModule: RUNTIME_MODULE_ENV,
} as const
