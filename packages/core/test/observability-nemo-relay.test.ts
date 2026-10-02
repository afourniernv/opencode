import { describe, expect, test } from "bun:test"
import { Effect, Exit, Stream } from "effect"
import {
  createLifecycleForTesting,
  durationBucket,
  finishReason,
  makeForTesting,
  modelFamily,
  providerFamily,
  toolCategory,
  toolDurationBucket,
  type LlmStreamCompleted,
  type ToolCompleted,
} from "@opencode-ai/core/observability/nemo-relay"

type Measurement = {
  readonly name: string
  readonly kind: unknown
  readonly valueType: unknown
  readonly value: number
  readonly attributes?: Record<string, string | number | boolean>
  readonly boundaries?: ReadonlyArray<number>
}

type Emission = {
  readonly name: string
  readonly measurements: ReadonlyArray<Measurement>
  readonly metadata?: Record<string, string | number | boolean> | null
}

function driver(emissions: Emission[] = []) {
  return {
    MetricKind: { Counter: "counter", Histogram: "histogram" },
    MetricValueType: { U64: "u64", F64: "f64" },
    metric(name: string, measurements: ReadonlyArray<Measurement>, _handle?: null, metadata?: Emission["metadata"]) {
      emissions.push({ name, measurements, metadata })
    },
    flushSubscribers: async () => {},
  }
}

type TraceRecord = {
  readonly phase: "start" | "end" | "event"
  readonly kind: "scope" | "llm" | "tool" | "event"
  readonly id?: string
  readonly parent?: string
  readonly name: string
  readonly payload?: unknown
  readonly metadata?: unknown
}

function traceDriver(records: TraceRecord[] = []) {
  type Handle = { readonly id: string; readonly parent?: Handle; readonly name: string }
  type Stack = { current?: Handle }
  let active: Stack | undefined
  let next = 0
  const handle = (name: string) => ({ id: `trace-${++next}`, parent: active?.current, name })
  const start = (kind: "scope" | "llm" | "tool", name: string, payload?: unknown, metadata?: unknown) => {
    const value = handle(name)
    records.push({ phase: "start", kind, id: value.id, parent: value.parent?.id, name, payload, metadata })
    return value
  }
  return {
    ScopeType: { Agent: "agent", Llm: "llm", Tool: "tool" },
    createScopeStack: () => ({ current: handle("implicit-root") }) satisfies Stack,
    capturePropagationContext: () => ({ parent: active?.current }),
    createScopeStackFromPropagation: (context: unknown) => ({
      current: (context as { readonly parent?: Handle }).parent,
    }),
    withScopeStack(stack: unknown, callback: () => unknown) {
      const previous = active
      active = stack as Stack
      try {
        return callback()
      } finally {
        active = previous
      }
    },
    pushScope(
      name: string,
      _scopeType: unknown,
      _handle?: unknown,
      _attributes?: number | null,
      _data?: unknown,
      metadata?: unknown,
      payload?: unknown,
    ) {
      const value = start("scope", name, payload, metadata)
      if (active) active.current = value
      return value
    },
    popScope(value: unknown, payload?: unknown, _timestamp?: number | null, metadata?: unknown) {
      const item = value as Handle
      records.push({
        phase: "end",
        kind: "scope",
        id: item.id,
        parent: item.parent?.id,
        name: item.name,
        payload,
        metadata,
      })
      if (active) active.current = item.parent
    },
    llmCall(
      name: string,
      payload: unknown,
      _handle?: unknown,
      _attributes?: number | null,
      _data?: unknown,
      metadata?: unknown,
    ) {
      return start("llm", name, payload, metadata)
    },
    llmCallEnd(value: unknown, payload?: unknown, _data?: unknown, metadata?: unknown) {
      const item = value as Handle
      records.push({
        phase: "end",
        kind: "llm",
        id: item.id,
        parent: item.parent?.id,
        name: item.name,
        payload,
        metadata,
      })
    },
    toolCall(
      name: string,
      payload: unknown,
      _handle?: unknown,
      _attributes?: number | null,
      _data?: unknown,
      metadata?: unknown,
    ) {
      return start("tool", name, payload, metadata)
    },
    toolCallEnd(value: unknown, payload?: unknown, _data?: unknown, metadata?: unknown) {
      const item = value as Handle
      records.push({
        phase: "end",
        kind: "tool",
        id: item.id,
        parent: item.parent?.id,
        name: item.name,
        payload,
        metadata,
      })
    },
    event(name: string, value?: unknown, payload?: unknown, metadata?: unknown) {
      const item = value as Handle | undefined
      records.push({ phase: "event", kind: "event", parent: item?.id, name, payload, metadata })
    },
  }
}

function deferred() {
  let resolve!: () => void
  const promise = new Promise<void>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const report = (input?: {
  readonly components?: number
  readonly diagnostics?: ReadonlyArray<{ readonly level: "warning" | "error"; readonly code: string }>
  readonly runtimeDiagnostics?: ReadonlyArray<Record<string, unknown>>
  readonly dynamicPlugins?: ReadonlyArray<Record<string, unknown>>
  readonly configPaths?: ReadonlyArray<string>
}) => ({
  config: { diagnostics: input?.diagnostics ?? [], runtime_diagnostics: input?.runtimeDiagnostics ?? [] },
  dynamic_plugins: input?.dynamicPlugins ?? [],
  config_paths: input?.configPaths ?? [],
  resolved_config: {
    version: 1,
    components: Array.from({ length: input?.components ?? 1 }, () => ({ kind: "test", config: {} })),
  },
})

function fakeRuntime(input?: {
  readonly report?: unknown
  readonly flushFails?: boolean
  readonly closeFailsOnce?: boolean
  readonly initializeFails?: Error
  readonly initializeWait?: Promise<void>
  readonly flushWait?: Promise<void>
  readonly closeWait?: Promise<void>
  readonly isActiveThrows?: boolean
  readonly reportThrows?: boolean
  readonly flushThrowsSynchronously?: boolean
}) {
  const state = {
    initialize: 0,
    flush: 0,
    close: 0,
    active: false,
    closing: false,
    path: undefined as string | undefined,
    report: input?.report ?? report(),
  }
  const metrics = driver()
  const activation = {
    get isActive() {
      if (input?.isActiveThrows) throw new Error("isActive getter failed")
      return state.active && !state.closing
    },
    get report() {
      if (input?.reportThrows) throw new Error("report getter failed")
      return state.report
    },
    async close() {
      state.close++
      state.closing = true
      await input?.closeWait
      if (input?.closeFailsOnce && state.close === 1) {
        state.closing = false
        throw new Error("close failed")
      }
      state.active = false
      state.closing = false
    },
  }
  return {
    state,
    module: {
      ...metrics,
      ...traceDriver(),
      flushSubscribers() {
        state.flush++
        if (input?.flushThrowsSynchronously) throw new Error("flush failed synchronously")
        if (input?.flushFails) return Promise.reject(new Error("flush failed"))
        return input?.flushWait ?? Promise.resolve()
      },
      async initialize(_config: unknown, path?: string) {
        state.initialize++
        state.path = path
        if (input?.initializeFails) throw input.initializeFails
        await input?.initializeWait
        state.active = true
        return activation
      },
    },
  }
}

describe("NeMo Relay observability", () => {
  test("maps arbitrary identifiers into closed families and categories", () => {
    expect(providerFamily("openrouter-team-secret")).toBe("openrouter")
    expect(providerFamily("azure-openai-prod")).toBe("azure")
    expect(providerFamily("nvidia")).toBe("nvidia")
    expect(providerFamily("customer-provider-with-private-name")).toBe("custom")
    expect(modelFamily("tenant/private-model-name")).toBe("custom")
    expect(modelFamily("anthropic/claude-sonnet-4")).toBe("claude")
    expect(toolCategory("read")).toBe("file_read")
    expect(toolCategory("question")).toBe("human_input")
    expect(toolCategory("private_customer_tool")).toBe("extension")
    expect(finishReason("tool-calls")).toBe("tool_calls")
    expect(finishReason("provider-private-reason")).toBe("unknown")
  })

  test("keeps the legacy bucket helpers bounded", () => {
    expect(durationBucket(999)).toBe("lt_1s")
    expect(durationBucket(1_000)).toBe("1s_to_5s")
    expect(durationBucket(600_000)).toBe("gte_10m")
    expect(durationBucket(Number.NaN)).toBe("unknown")
    expect(toolDurationBucket(99)).toBe("lt_100ms")
    expect(toolDurationBucket(30_000)).toBe("gte_30s")
  })

  test("emits an allowlisted host-stream projection without raw identifiers", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.llmStreamCompleted({
        role: "primary",
        agentRuntime: "v1",
        runtime: "ai_sdk",
        provider: "customer-provider-with-private-name",
        model: "tenant/private-model-name",
        outcome: "success",
        finish: "stop",
        durationMs: 1_250,
        tokens: {
          inputTotal: 21,
          inputNonCached: 13,
          inputCacheRead: 5,
          inputCacheWrite: 3,
          outputTotal: 8,
          outputReasoning: 2,
        },
      }),
    )

    expect(emissions).toHaveLength(1)
    expect(emissions[0]?.name).toBe("opencode.llm.stream.completed")
    expect(emissions[0]?.measurements.map((item) => item.name)).toEqual([
      "opencode.llm.host_stream.count",
      "opencode.llm.host_stream.duration",
      "opencode.model_route.count",
      "opencode.llm.finish_reason.count",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
    ])
    expect(emissions[0]?.measurements[1]?.kind).toBe("histogram")
    expect(emissions[0]?.measurements[0]?.attributes).toMatchObject({
      provider_family: "custom",
      model_family: "custom",
    })
    expect(emissions[0]?.measurements[1]?.attributes).toMatchObject({
      provider_family: "custom",
      model_family: "custom",
    })
    expect(emissions[0]?.measurements[2]?.attributes).toMatchObject({
      provider_family: "custom",
      model_family: "custom",
    })
    expect(emissions[0]?.measurements[4]?.attributes).toMatchObject({
      provider_family: "custom",
      model_family: "custom",
      "opencode.metric.schema_version": "2",
    })
    expect(JSON.stringify(emissions)).not.toContain("customer-provider-with-private-name")
    expect(JSON.stringify(emissions)).not.toContain("tenant/private-model-name")
  })

  test("prefers aggregate finish usage, sanitizes step fields before summing, and marks missing finish incomplete", async () => {
    const completed: Array<{ readonly outcome: string; readonly tokens?: { readonly inputTotal?: number } }> = []
    const relay = makeForTesting(driver(), {
      llmStreamCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const events = [
      { type: "step-finish", reason: "tool-calls", usage: { inputTokens: 3 } },
      { type: "finish", reason: "stop", usage: { inputTokens: 11 } },
    ]
    await Effect.runPromise(
      relay
        .observeLlmStream(
          { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
          Stream.fromIterable(events),
        )
        .pipe(Stream.runDrain),
    )
    await Effect.runPromise(
      relay
        .observeLlmStream(
          { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
          Stream.fromIterable([
            { type: "step-finish", reason: "tool-calls", usage: { inputTokens: 2 } },
            { type: "step-finish", reason: "stop", usage: { inputTokens: 5 } },
          ]),
        )
        .pipe(Stream.runDrain),
    )
    await Effect.runPromise(
      relay
        .observeLlmStream(
          { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
          Stream.fromIterable([
            { type: "step-finish", reason: "stop", usage: { inputTokens: 13 } },
            { type: "finish", reason: "stop", usage: {} },
          ]),
        )
        .pipe(Stream.runDrain),
    )
    await Effect.runPromise(
      relay
        .observeLlmStream(
          { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
          Stream.fromIterable([
            {
              type: "step-finish",
              reason: "tool-calls",
              usage: { inputTokens: -2, outputTokens: Number.NaN, cacheReadInputTokens: Number.POSITIVE_INFINITY },
            },
            { type: "step-finish", reason: "stop", usage: { inputTokens: 5, outputTokens: 1.9 } },
          ]),
        )
        .pipe(Stream.runDrain),
    )
    expect(completed).toEqual([
      expect.objectContaining({ outcome: "success", tokens: expect.objectContaining({ inputTotal: 11 }) }),
      expect.objectContaining({ outcome: "incomplete", tokens: expect.objectContaining({ inputTotal: 7 }) }),
      expect.objectContaining({ outcome: "success", tokens: expect.objectContaining({ inputTotal: 13 }) }),
      expect.objectContaining({
        outcome: "incomplete",
        tokens: {
          inputTotal: 5,
          inputNonCached: undefined,
          inputCacheRead: undefined,
          inputCacheWrite: undefined,
          outputTotal: 1,
          outputReasoning: undefined,
        },
      }),
    ])
  })

  test("preserves an observed provider error when the stream is subsequently interrupted", async () => {
    const completed: LlmStreamCompleted[] = []
    const relay = makeForTesting(driver(), {
      llmStreamCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const source = Stream.make({ type: "provider-error" as const }).pipe(
      Stream.concat(Stream.fromEffect(Effect.interrupt)),
    )

    const exit = await Effect.runPromiseExit(
      relay
        .observeLlmStream(
          { role: "primary", agentRuntime: "v2", runtime: "native", provider: "openai", model: "gpt" },
          source,
        )
        .pipe(Stream.runDrain),
    )

    expect(Exit.isFailure(exit)).toBe(true)
    expect(completed).toEqual([expect.objectContaining({ outcome: "provider_error" })])
  })

  test("classifies logical turn exits without changing the host exit", async () => {
    const completed: Array<{ readonly outcome: string }> = []
    const relay = makeForTesting(driver(), {
      turnCompleted: (input) => Effect.sync(() => completed.push(input)),
    })

    const blocked = await Effect.runPromiseExit(
      relay.observeTurn({ role: "primary", runtime: "v2" }, Effect.interrupt, () => "blocked"),
    )
    const cancelled = await Effect.runPromiseExit(
      relay.observeTurn({ role: "primary", runtime: "v2" }, Effect.interrupt),
    )

    expect(Exit.isFailure(blocked)).toBe(true)
    expect(Exit.isFailure(cancelled)).toBe(true)
    expect(completed.map((item) => item.outcome)).toEqual(["blocked", "cancelled"])
  })

  test("creates independent observations per subscription and identifies downstream early close", async () => {
    const completed: LlmStreamCompleted[] = []
    const relay = makeForTesting(driver(), {
      llmStreamCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const observed = relay.observeLlmStream(
      { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
      Stream.fromIterable([{ type: "text-delta" }, { type: "finish", reason: "stop", usage: { inputTokens: 1 } }]),
    )

    await Effect.runPromise(observed.pipe(Stream.take(1), Stream.runDrain))
    await Effect.runPromise(observed.pipe(Stream.runDrain))

    expect(completed.map((item) => item.outcome)).toEqual(["incomplete", "success"])
  })

  test("records provider tools without claiming local execution latency", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))
    const observation = await Effect.runPromise(
      relay.beginTool({ name: "private_provider_tool", execution: "provider" }),
    )
    await Effect.runPromise(observation.complete("success"))

    expect(emissions[0]?.measurements).toHaveLength(1)
    expect(emissions[0]?.measurements[0]?.attributes).toMatchObject({
      category: "provider",
      execution: "provider",
      outcome: "success",
    })
    expect(JSON.stringify(emissions)).not.toContain("private_provider_tool")
  })

  test("claims tool completion when the completion effect runs and emits once", async () => {
    const completed: ToolCompleted[] = []
    const relay = makeForTesting(driver(), {
      toolCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const observation = await Effect.runPromise(relay.beginTool({ name: "read", execution: "local" }))
    const success = observation.complete("success")
    const failed = observation.complete("failed")

    await Effect.runPromise(failed)
    await Effect.runPromise(success)

    expect(completed).toHaveLength(1)
    expect(completed[0]).toMatchObject({ name: "read", execution: "local", outcome: "failed" })
  })

  test("keeps metric failures fail-open", async () => {
    const relay = makeForTesting({
      ...driver(),
      metric() {
        throw new Error("collector unavailable")
      },
    })
    await expect(
      Effect.runPromise(relay.toolCompleted({ name: "read", execution: "local", outcome: "success", durationMs: 10 })),
    ).resolves.toBeUndefined()
  })

  test("emits privacy-bounded turn, LLM, tool, and retry traces on isolated child stacks", async () => {
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(), ...traceDriver(records) })

    await Effect.runPromise(
      relay.observeTurn(
        { role: "primary", runtime: "v2" },
        Effect.gen(function* () {
          yield* Effect.all(
            ["first-private-model", "second-private-model"].map((model) =>
              relay
                .observeLlmStream(
                  {
                    role: "primary",
                    agentRuntime: "v2",
                    runtime: "native",
                    provider: "private-provider-name",
                    model,
                  },
                  Stream.make({ type: "finish", reason: "stop", usage: { inputTokens: 2, outputTokens: 1 } }),
                )
                .pipe(Stream.runDrain),
            ),
            { concurrency: "unbounded", discard: true },
          )
          const tool = yield* relay.beginTool({ name: "private-tool-name", execution: "local" })
          yield* tool.complete("success")
          yield* relay.retryScheduled({ runtime: "v2", attempt: 2 })
        }),
      ),
    )

    const turn = records.find((item) => item.phase === "start" && item.kind === "scope")
    const children = records.filter((item) => item.phase === "start" && ["llm", "tool"].includes(item.kind))
    expect(turn?.name).toBe("opencode.agent.turn")
    expect(children).toHaveLength(3)
    expect(children.every((item) => item.parent === turn?.id)).toBe(true)
    expect(records.find((item) => item.phase === "event")?.parent).toBe(turn?.id)
    expect(records.at(-1)).toMatchObject({ phase: "end", kind: "scope", id: turn?.id })
    expect(JSON.stringify(records)).not.toContain("private-provider-name")
    expect(JSON.stringify(records)).not.toContain("private-tool-name")
    expect(JSON.stringify(records)).not.toContain("first-private-model")
    expect(JSON.stringify(records)).toContain("provider_family")
    expect(JSON.stringify(records)).toContain("model_family")
  })
})

describe("NeMo Relay process lifecycle", () => {
  test("disabled and invalid activation requests never load the native runtime", async () => {
    let loads = 0
    const disabled = createLifecycleForTesting({
      loader: async () => {
        loads++
        return fakeRuntime().module
      },
      environment: {},
    })
    expect((await disabled.acquire()).status).toEqual({ state: "disabled", reason: "not_requested" })
    await disabled.release()

    const invalid = createLifecycleForTesting({
      loader: async () => {
        loads++
        return fakeRuntime().module
      },
      environment: { OPENCODE_NEMO_RELAY: "treu" },
    })
    expect((await invalid.acquire()).status).toEqual({ state: "unavailable", reason: "invalid_enable_value" })
    await invalid.release()

    const orphanConfig = createLifecycleForTesting({
      loader: async () => {
        loads++
        return fakeRuntime().module
      },
      environment: { OPENCODE_NEMO_RELAY_PLUGINS_TOML: "/private/config.toml" },
    })
    expect((await orphanConfig.acquire()).status).toEqual({
      state: "unavailable",
      reason: "configuration_without_enable",
    })
    await orphanConfig.release()
    expect(loads).toBe(0)
  })

  test("a custom runtime override may load on a platform unsupported by the default package", async () => {
    const runtime = fakeRuntime()
    let specifier = ""
    const lifecycle = createLifecycleForTesting({
      loader: async (value) => {
        specifier = value
        return runtime.module
      },
      environment: {
        OPENCODE_NEMO_RELAY: "1",
        OPENCODE_NEMO_RELAY_RUNTIME_MODULE: "custom-relay-runtime",
      },
      platform: "darwin",
      arch: "x64",
    })
    expect((await lifecycle.acquire()).status.state).toBe("active")
    expect(specifier).toBe("custom-relay-runtime")
    await lifecycle.release()
  })

  test("pure mode and the unsupported default package do not load native code", async () => {
    let loads = 0
    const loader = async () => {
      loads++
      return fakeRuntime().module
    }
    const pure = createLifecycleForTesting({
      loader,
      environment: { OPENCODE_NEMO_RELAY: "1", OPENCODE_PURE: "true" },
    })
    expect((await pure.acquire()).status).toEqual({ state: "disabled", reason: "pure_mode" })
    await pure.release()

    const unsupported = createLifecycleForTesting({
      loader,
      environment: { OPENCODE_NEMO_RELAY: "1" },
      platform: "darwin",
      arch: "x64",
    })
    expect((await unsupported.acquire()).status).toEqual({ state: "unavailable", reason: "unsupported_platform" })
    await unsupported.release()
    expect(loads).toBe(0)
  })

  test("fails open when runtime loading exceeds the startup deadline", async () => {
    const loading = deferred()
    const lifecycle = createLifecycleForTesting({
      loader: async () => {
        await loading.promise
        return fakeRuntime().module
      },
      environment: { OPENCODE_NEMO_RELAY: "1" },
      startupTimeoutMs: 5,
    })

    expect((await lifecycle.acquire()).status).toEqual({ state: "unavailable", reason: "startup_timeout" })
    await lifecycle.release()
  })

  test("closes a plugin host that activates after the startup deadline", async () => {
    const initialize = deferred()
    const runtime = fakeRuntime({ initializeWait: initialize.promise })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
      startupTimeoutMs: 5,
    })

    expect((await lifecycle.acquire()).status).toEqual({ state: "unavailable", reason: "startup_timeout" })
    expect(runtime.state.close).toBe(0)
    initialize.resolve()
    for (let attempt = 0; attempt < 20 && runtime.state.close === 0; attempt++)
      await new Promise((resolve) => setTimeout(resolve, 1))

    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
    expect(runtime.state.active).toBe(false)
    await lifecycle.release()

    // Releasing the owner after autonomous late cleanup must not close twice.
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
  })

  test("separates activation conflicts and incompatible runtime modules", async () => {
    const conflictRuntime = fakeRuntime({
      initializeFails: new Error("plugin configuration is owned by an active dynamic plugin host"),
    })
    const conflict = createLifecycleForTesting({
      loader: async () => conflictRuntime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    expect((await conflict.acquire()).status).toEqual({ state: "unavailable", reason: "activation_conflict" })
    await conflict.release()

    const incompatible = createLifecycleForTesting({
      loader: async () => ({ initialize: async () => undefined }),
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    expect((await incompatible.acquire()).status).toEqual({ state: "unavailable", reason: "incompatible_runtime" })
    await incompatible.release()
  })

  test("an explicitly missing configuration is not reported active", async () => {
    const runtime = fakeRuntime({
      report: report({
        diagnostics: [{ level: "warning", code: "plugin.configuration_file_missing" }],
      }),
    })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: {
        OPENCODE_NEMO_RELAY: "1",
        OPENCODE_NEMO_RELAY_PLUGINS_TOML: "/missing/plugins.toml",
      },
    })
    expect((await lifecycle.acquire()).status).toEqual({ state: "unavailable", reason: "configuration_failed" })
    expect(runtime.state.path).toBe("/missing/plugins.toml")
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
    await lifecycle.release()
  })

  test("summarizes empty, configured, and degraded activation reports without exposing paths", async () => {
    const emptyRuntime = fakeRuntime({ report: report({ components: 0 }) })
    const empty = createLifecycleForTesting({
      loader: async () => emptyRuntime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    expect((await empty.acquire()).status).toMatchObject({
      state: "active",
      activation: "healthy",
      configuration: "empty",
      report: { componentCount: 0, configPathCount: 0 },
    })
    await empty.release()

    const degradedRuntime = fakeRuntime({
      report: report({
        diagnostics: [{ level: "warning", code: "test.warning" }],
        runtimeDiagnostics: [{ code: "runtime.warning", message: "private detail" }],
        dynamicPlugins: [{ selected: false, failure: { code: "optional_plugin_failed" } }],
        configPaths: ["/private/plugins.toml"],
      }),
    })
    const degraded = createLifecycleForTesting({
      loader: async () => degradedRuntime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const status = (await degraded.acquire()).status
    expect(status).toMatchObject({
      state: "active",
      activation: "degraded",
      configuration: "present",
      report: {
        configPathCount: 1,
        warningCount: 1,
        runtimeDiagnosticCount: 1,
        dynamicFailureCount: 1,
      },
    })
    expect(JSON.stringify(status)).not.toContain("/private/plugins.toml")
    expect(JSON.stringify(status)).not.toContain("private detail")
    await degraded.release()
  })

  test("refreshes bounded process health from Relay's live activation report", async () => {
    const runtime = fakeRuntime({ report: report({ components: 0 }) })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    expect((await lifecycle.acquire()).status).toMatchObject({ state: "active", activation: "healthy" })

    runtime.state.report = report({
      components: 0,
      runtimeDiagnostics: [{ code: "runtime.warning", message: "private detail" }],
    })
    const status = lifecycle.health().status

    expect(status).toMatchObject({
      state: "active",
      activation: "degraded",
      report: { runtimeDiagnosticCount: 1 },
    })
    expect(JSON.stringify(status)).not.toContain("private detail")

    runtime.state.report = new Proxy(
      {},
      {
        get: () => {
          throw new Error("late report failure")
        },
      },
    )
    expect(() => lifecycle.health()).not.toThrow()
    expect(lifecycle.health().status).toMatchObject({ state: "active", activation: "degraded" })
    await lifecycle.release()
  })

  test("throwing activation getters fail closed without escaping into the host", async () => {
    for (const runtime of [fakeRuntime({ isActiveThrows: true }), fakeRuntime({ reportThrows: true })]) {
      const lifecycle = createLifecycleForTesting({
        loader: async () => runtime.module,
        environment: { OPENCODE_NEMO_RELAY: "1" },
      })
      expect((await lifecycle.acquire()).status).toEqual({ state: "unavailable", reason: "invalid_activation" })
      expect(runtime.state.close).toBe(1)
      await lifecycle.release()
    }
  })

  test("concurrent owners initialize once and the final owner closes once", async () => {
    const runtime = fakeRuntime()
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const [first, second] = await Promise.all([lifecycle.acquire(), lifecycle.acquire()])
    expect(first.status.state).toBe("active")
    expect(second.status.state).toBe("active")
    expect(runtime.state.initialize).toBe(1)
    await lifecycle.release()
    expect(runtime.state.close).toBe(0)
    await lifecycle.release()
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
  })

  test("failed close retains and retries the same activation before reopening", async () => {
    const runtime = fakeRuntime({ closeFailsOnce: true })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    await lifecycle.acquire()
    await lifecycle.release()
    expect(lifecycle.health().phase).toBe("teardown_failed")
    expect(runtime.state.initialize).toBe(1)
    expect(runtime.state.close).toBe(1)

    expect((await lifecycle.acquire()).status.state).toBe("active")
    expect(runtime.state.close).toBe(2)
    expect(runtime.state.initialize).toBe(2)
    await lifecycle.release()
  })

  test("shutdown is single-flight and drains an admitted tool before flush and close", async () => {
    const runtime = fakeRuntime()
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const relay = await lifecycle.acquire()
    const tool = await Effect.runPromise(relay.beginTool({ name: "read", execution: "local" }))
    const first = lifecycle.shutdown(1_000)
    const second = lifecycle.shutdown(1_000)
    expect(first).toBe(second)
    expect(runtime.state.close).toBe(0)
    await Effect.runPromise(tool.complete("success"))
    expect(await first).toEqual({ drained: true, flushed: true, closed: true })
    expect(runtime.state.close).toBe(1)
  })

  test("lazy observations that never execute do not hold process shutdown open", async () => {
    const runtime = fakeRuntime()
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const relay = await lifecycle.acquire()
    relay.observeLlmStream(
      { role: "primary", agentRuntime: "v1", runtime: "native", provider: "openai", model: "gpt" },
      Stream.never,
    )
    relay.observeTurn({ role: "primary", runtime: "v1" }, Effect.never)

    expect(await lifecycle.shutdown(100)).toEqual({ drained: true, flushed: true, closed: true })
    expect(runtime.state.close).toBe(1)
  })

  test("bounded shutdown abandons an active operation without closing underneath it", async () => {
    const runtime = fakeRuntime()
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const relay = await lifecycle.acquire()
    const tool = await Effect.runPromise(relay.beginTool({ name: "read", execution: "local" }))

    expect(await lifecycle.shutdown(1)).toEqual({ drained: false, flushed: false, closed: false })
    expect(runtime.state.flush).toBe(0)
    expect(runtime.state.close).toBe(0)
    expect(lifecycle.health().phase).toBe("teardown_failed")

    await Effect.runPromise(tool.complete("cancelled"))
    expect(lifecycle.health().operations).toBe(0)
  })

  test("a timed-out close retains and retries the same closing activation", async () => {
    const close = deferred()
    const runtime = fakeRuntime({ closeWait: close.promise })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    await lifecycle.acquire()

    expect(await lifecycle.shutdown(20)).toEqual({ drained: true, flushed: true, closed: false })
    expect(runtime.state.close).toBe(1)
    expect(runtime.state.closing).toBe(true)

    close.resolve()
    expect(await lifecycle.shutdown(1_000)).toEqual({ drained: true, flushed: true, closed: true })
    expect(runtime.state.close).toBe(1)
  })

  test("a timed-out flush is awaited once before the retained activation closes", async () => {
    const flush = deferred()
    const runtime = fakeRuntime({ flushWait: flush.promise })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    await lifecycle.acquire()

    expect(await lifecycle.shutdown(20)).toEqual({ drained: true, flushed: false, closed: false })
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(0)

    flush.resolve()
    expect(await lifecycle.shutdown(1_000)).toEqual({ drained: true, flushed: true, closed: true })
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
  })

  test("shutdown racing an in-flight runtime load retains and closes the late activation", async () => {
    const initialize = deferred()
    const runtime = fakeRuntime({ initializeWait: initialize.promise })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    const acquiring = lifecycle.acquire()
    while (runtime.state.initialize === 0) await Promise.resolve()

    expect(await lifecycle.shutdown(1)).toEqual({ drained: false, flushed: false, closed: false })
    initialize.resolve()
    expect((await acquiring).status).toEqual({ state: "unavailable", reason: "stopped" })
    expect(await lifecycle.shutdown(1_000)).toEqual({ drained: true, flushed: true, closed: true })
    expect(runtime.state.close).toBe(1)
  })

  test("flush failure is reported but still closes the activation", async () => {
    const runtime = fakeRuntime({ flushFails: true })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    await lifecycle.acquire()
    const result = await lifecycle.shutdown(1_000)
    expect(result).toEqual({ drained: true, flushed: false, closed: true })
    expect(runtime.state.close).toBe(1)
    expect(lifecycle.health()).toMatchObject({
      phase: "teardown_failed",
      status: { state: "unavailable", reason: "teardown_failed" },
    })
  })

  test("release preserves a failed flush for root shutdown reporting", async () => {
    const runtime = fakeRuntime({ flushThrowsSynchronously: true })
    const lifecycle = createLifecycleForTesting({
      loader: async () => runtime.module,
      environment: { OPENCODE_NEMO_RELAY: "1" },
    })
    await lifecycle.acquire()
    await lifecycle.release()

    expect(lifecycle.health().phase).toBe("teardown_failed")
    expect(await lifecycle.shutdown(1_000)).toEqual({ drained: true, flushed: false, closed: true })
    expect(runtime.state.flush).toBe(1)
    expect(runtime.state.close).toBe(1)
  })
})
