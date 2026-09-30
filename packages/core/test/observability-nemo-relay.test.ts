import { describe, expect, test } from "bun:test"
import { Effect, Exit, Fiber, Stream } from "effect"
import { RequestExecutor } from "@opencode-ai/llm/route"
import {
  createLifecycleForTesting,
  durationBucket,
  finishReason,
  llmOperation,
  makeForTesting,
  modelFamily,
  permissionFamily,
  providerFamily,
  terminalResultFamily,
  toolCategory,
  toolDurationBucket,
  type LlmStreamCompleted,
  type LlmUnaryCompleted,
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
  readonly scopeType?: unknown
}

function traceDriver(records: TraceRecord[] = []) {
  type Handle = {
    readonly id: string
    readonly uuid: string
    readonly parent?: Handle
    readonly name: string
    readonly scopeType?: unknown
    readonly handleKind: "implicit" | "scope" | "llm" | "tool"
  }
  type Stack = { current?: Handle }
  let active: Stack | undefined
  let next = 0
  const handles = new Map<string, Handle>()
  const handle = (name: string, scopeType?: unknown, handleKind: Handle["handleKind"] = "implicit") => {
    const id = `trace-${++next}`
    const value = { id, uuid: id, parent: active?.current, name, scopeType, handleKind }
    handles.set(id, value)
    return value
  }
  const start = (
    kind: "scope" | "llm" | "tool",
    name: string,
    payload?: unknown,
    metadata?: unknown,
    scopeType?: unknown,
  ) => {
    const value = handle(name, scopeType, kind)
    records.push({ phase: "start", kind, id: value.id, parent: value.parent?.id, name, payload, metadata, scopeType })
    return value
  }
  return {
    ScopeType: { Agent: "agent", Function: "function", Llm: "llm", Tool: "tool", Guardrail: "guardrail" },
    createScopeStack: () => ({ current: handle("implicit-root") }) satisfies Stack,
    capturePropagationContext: () => ({
      version: 1,
      parent: active?.current,
      parentUuid: active?.current?.uuid ?? "trace-root",
    }),
    createScopeStackFromPropagation: (context: unknown) => {
      const value = context as { readonly parent?: Handle; readonly parentUuid?: string }
      return { current: value.parentUuid ? handles.get(value.parentUuid) : value.parent }
    },
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
      scopeType: unknown,
      _handle?: unknown,
      _attributes?: number | null,
      _data?: unknown,
      metadata?: unknown,
      payload?: unknown,
    ) {
      const value = start("scope", name, payload, metadata, scopeType)
      if (active) active.current = value
      return value
    },
    popScope(value: unknown, payload?: unknown, _timestamp?: number | null, metadata?: unknown) {
      const item = value as Handle
      if (item.handleKind !== "scope") throw new Error("popScope requires a ScopeHandle")
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
      if (item.handleKind !== "llm") throw new Error("llmCallEnd requires an LlmHandle")
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
      if (item.handleKind !== "tool") throw new Error("toolCallEnd requires a ToolHandle")
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
      if (item && item.handleKind !== "scope") throw new Error("event parent must be a ScopeHandle")
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
    expect(providerFamily("groq-production")).toBe("groq")
    expect(providerFamily("xai")).toBe("xai")
    expect(providerFamily("customer-provider-with-private-name")).toBe("custom")
    expect(modelFamily("tenant/private-model-name")).toBe("custom")
    expect(modelFamily("anthropic/claude-sonnet-4")).toBe("claude")
    expect(modelFamily("openai/o3-mini")).toBe("o3")
    expect(modelFamily("meta/muse-spark")).toBe("muse")
    expect(modelFamily("minimax/minimax-m3")).toBe("minimax")
    expect(llmOperation("openai-responses")).toBe("openai.responses")
    expect(llmOperation("anthropic-messages")).toBe("anthropic.messages")
    expect(llmOperation("tenant-private-protocol")).toBe("unknown")
    expect(permissionFamily("external_directory")).toBe("filesystem")
    expect(permissionFamily("doom_loop")).toBe("safety")
    expect(toolCategory("read")).toBe("file_read")
    expect(toolCategory("question")).toBe("human_input")
    expect(toolCategory("private_customer_tool")).toBe("extension")
    expect(terminalResultFamily({ exit: 0 })).toBe("zero_exit")
    expect(terminalResultFamily({ exit: 7421 })).toBe("nonzero_exit")
    expect(terminalResultFamily({ exit: null, timeout: true })).toBe("timeout")
    expect(terminalResultFamily({ exit: null, aborted: true })).toBe("aborted")
    expect(terminalResultFamily({ exit: null, signal: true })).toBe("signal")
    expect(terminalResultFamily({ exit: null, signal: "PRIVATE_SIGNAL_DETAIL" })).toBe("signal")
    expect(terminalResultFamily({ exit: null })).toBe("unknown")
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
        contextLimit: 20,
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
        inputContextUtilizations: [1.05, -1, Number.NaN],
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
      "opencode.llm.input_context_utilization",
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
      "opencode.metric.schema_version": "3",
    })
    expect(emissions[0]?.measurements.at(-1)).toMatchObject({
      name: "opencode.llm.input_context_utilization",
      kind: "histogram",
      valueType: "f64",
      value: 1.05,
      unit: "1",
      boundaries: [0.25, 0.5, 0.75, 0.85, 0.9, 0.95, 1, 1.1],
    })
    expect(JSON.stringify(emissions)).not.toContain("customer-provider-with-private-name")
    expect(JSON.stringify(emissions)).not.toContain("tenant/private-model-name")
  })

  test("emits unary context utilization with an explicit unary scope", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.llmUnaryCompleted({
        role: "compaction",
        agentRuntime: "v2",
        runtime: "native",
        provider: "openai",
        model: "gpt-5",
        contextLimit: 20,
        outcome: "success",
        durationMs: 100,
        tokens: { inputTotal: 15 },
      }),
    )

    expect(emissions[0]?.measurements.at(-1)).toMatchObject({
      name: "opencode.llm.input_context_utilization",
      value: 0.75,
      unit: "1",
      attributes: expect.objectContaining({ scope: "unary_call" }),
    })
  })

  test("emits finite nonnegative USD cost with explicit provenance and bounded route attributes", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.llmCostRecorded({
        role: "primary",
        agentRuntime: "v1",
        provider: "customer-provider-with-private-name",
        model: "tenant/private-model-name",
        costUsd: 0.0125,
        source: "price_table_estimate",
      }),
    )
    await Effect.runPromise(
      relay.llmCostRecorded({
        role: "primary",
        agentRuntime: "v1",
        provider: "openai",
        model: "gpt-5",
        costUsd: Number.NaN,
        source: "provider_reported",
      }),
    )

    expect(emissions).toHaveLength(1)
    expect(emissions[0]?.name).toBe("opencode.llm.cost.recorded")
    expect(emissions[0]?.measurements).toEqual([
      expect.objectContaining({
        name: "opencode.llm.cost_usd",
        kind: "counter",
        valueType: "f64",
        value: 0.0125,
        unit: "USD",
        attributes: expect.objectContaining({
          call_role: "primary",
          agent_runtime: "v1",
          llm_mode: "stream",
          provider_family: "custom",
          model_family: "custom",
          source: "price_table_estimate",
          scope: "provider_step",
          "opencode.metric.schema_version": "3",
        }),
      }),
    ])
    expect(JSON.stringify(emissions)).not.toContain("customer-provider-with-private-name")
    expect(JSON.stringify(emissions)).not.toContain("tenant/private-model-name")
  })

  test("emits compaction lifecycle outcomes separately from successful V2 effectiveness estimates", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.compactionAttemptCompleted({
        runtime: "v2",
        trigger: "overflow_recovery",
        outcome: "success",
        durationMs: 1_250,
      }),
    )
    await Effect.runPromise(
      relay.compactionCompleted({
        runtime: "v2",
        sourceEstimatedTokens: 12_000,
        summaryEstimatedTokens: 1_500,
        retainedRecentEstimatedTokens: 8_000,
      }),
    )

    expect(emissions).toHaveLength(2)
    expect(emissions[0]).toMatchObject({
      name: "opencode.compaction.attempt.completed",
      measurements: [
        {
          name: "opencode.compaction.attempt.count",
          value: 1,
          attributes: expect.objectContaining({
            runtime: "v2",
            trigger: "overflow_recovery",
            outcome: "success",
          }),
        },
        expect.objectContaining({ name: "opencode.compaction.duration", value: 1_250 }),
      ],
    })
    expect(emissions[1]?.name).toBe("opencode.compaction.completed")
    expect(emissions[1]?.measurements).toEqual([
      expect.objectContaining({ name: "opencode.compaction.completed.count", value: 1 }),
      expect.objectContaining({
        name: "opencode.compaction.estimated_tokens",
        value: 12_000,
        attributes: expect.objectContaining({ runtime: "v2", kind: "source" }),
      }),
      expect.objectContaining({
        name: "opencode.compaction.estimated_tokens",
        value: 1_500,
        attributes: expect.objectContaining({ runtime: "v2", kind: "summary" }),
      }),
      expect.objectContaining({
        name: "opencode.compaction.estimated_tokens",
        value: 8_000,
        attributes: expect.objectContaining({ runtime: "v2", kind: "retained_recent" }),
      }),
    ])
  })

  test("prefers aggregate finish usage but emits per-step context utilization", async () => {
    const completed: Array<{
      readonly outcome: string
      readonly tokens?: { readonly inputTotal?: number }
      readonly inputContextUtilizations?: ReadonlyArray<number>
    }> = []
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
          {
            role: "primary",
            agentRuntime: "v1",
            runtime: "native",
            provider: "openai",
            model: "gpt",
            contextLimit: 10,
          },
          Stream.fromIterable(events),
        )
        .pipe(Stream.runDrain),
    )
    await Effect.runPromise(
      relay
        .observeLlmStream(
          {
            role: "primary",
            agentRuntime: "v1",
            runtime: "native",
            provider: "openai",
            model: "gpt",
            contextLimit: 10,
          },
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
          {
            role: "primary",
            agentRuntime: "v1",
            runtime: "native",
            provider: "openai",
            model: "gpt",
            contextLimit: 10,
          },
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
          {
            role: "primary",
            agentRuntime: "v1",
            runtime: "native",
            provider: "openai",
            model: "gpt",
            contextLimit: 10,
          },
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
      expect.objectContaining({
        outcome: "success",
        tokens: expect.objectContaining({ inputTotal: 11 }),
        inputContextUtilizations: [0.3],
      }),
      expect.objectContaining({
        outcome: "incomplete",
        tokens: expect.objectContaining({ inputTotal: 7 }),
        inputContextUtilizations: [0.2, 0.5],
      }),
      expect.objectContaining({
        outcome: "success",
        tokens: expect.objectContaining({ inputTotal: 13 }),
        inputContextUtilizations: [1.3],
      }),
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
        inputContextUtilizations: [0.5],
      }),
    ])
  })

  test("does not reinterpret aggregate finish usage when step usage is unavailable", async () => {
    const completed: LlmStreamCompleted[] = []
    const relay = makeForTesting(driver(), {
      llmStreamCompleted: (input) => Effect.sync(() => completed.push(input)),
    })

    await Effect.runPromise(
      relay
        .observeLlmStream(
          {
            role: "primary",
            agentRuntime: "v1",
            runtime: "ai_sdk",
            provider: "openai",
            model: "gpt",
            contextLimit: 10,
          },
          Stream.make({ type: "finish", reason: "stop", usage: { inputTokens: 11 } }),
        )
        .pipe(Stream.runDrain),
    )

    expect(completed[0]).toMatchObject({
      tokens: { inputTotal: 11 },
      inputContextUtilizations: [],
    })
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

  test("measures the first concrete output, classifies provider errors, and treats content filtering as failure", async () => {
    const emissions: Emission[] = []
    const completed: LlmStreamCompleted[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting(
      { ...driver(emissions), ...traceDriver(records) },
      {
        llmStreamCompleted: (input) =>
          Effect.gen(function* () {
            completed.push(input)
            yield* makeForTesting(driver(emissions)).llmStreamCompleted(input)
          }),
      },
    )

    await Effect.runPromise(
      relay
        .observeLlmStream(
          {
            role: "primary",
            agentRuntime: "v2",
            runtime: "native",
            provider: "openai",
            model: "gpt-5",
            protocol: "openai-responses",
          },
          Stream.fromIterable([
            { type: "text-start" },
            { type: "text-delta", text: "" },
            { type: "reasoning-delta", text: "thinking" },
            { type: "provider-error", classification: "context-overflow", retryable: true },
          ]),
        )
        .pipe(Stream.runDrain),
    )
    await Effect.runPromise(
      relay
        .observeLlmStream(
          {
            role: "primary",
            agentRuntime: "v2",
            runtime: "native",
            provider: "openai",
            model: "gpt-5",
            protocol: "openai-responses",
          },
          Stream.make({ type: "finish", reason: "content-filter" }),
        )
        .pipe(Stream.runDrain),
    )

    expect(completed[0]).toMatchObject({
      outcome: "provider_error",
      firstOutput: { kind: "reasoning" },
      providerError: { classification: "context-overflow", retryable: true },
    })
    expect(completed[1]).toMatchObject({ outcome: "failed", finish: "content-filter" })
    const providerError = emissions[0]?.measurements.find(
      (measurement) => measurement.name === "opencode.llm.provider_error.count",
    )
    expect(providerError?.attributes).toMatchObject({
      operation: "openai.responses",
      classification: "context_overflow",
      retryable: "true",
    })
    expect(
      emissions[0]?.measurements.find((measurement) => measurement.name === "opencode.llm.time_to_first_output")
        ?.attributes,
    ).toMatchObject({ output_kind: "reasoning" })
    expect(records.some((record) => record.name === "opencode.llm.first_output")).toBe(false)
    expect(records.find((record) => record.phase === "end" && record.kind === "llm")?.metadata).toMatchObject({
      "opencode.first_output_kind": "reasoning",
    })
  })

  test("observes unary LLM generation without changing success, failure, or interruption", async () => {
    const completed: LlmUnaryCompleted[] = []
    const relay = makeForTesting(driver(), {
      llmUnaryCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const input = {
      role: "agent_generation" as const,
      agentRuntime: "v1" as const,
      runtime: "ai_sdk" as const,
      provider: "openai",
      model: "gpt-5",
    }
    const value = { object: { name: "generated" }, finish: "stop", input: 7, output: 3 }

    await expect(
      Effect.runPromise(
        relay.observeLlmUnary(input, Effect.succeed(value), (result) => ({
          finish: result.finish,
          tokens: { inputTotal: result.input, outputTotal: result.output },
        })),
      ),
    ).resolves.toBe(value)
    const failure = new Error("host failure")
    const failed = await Effect.runPromiseExit(relay.observeLlmUnary(input, Effect.fail(failure)))
    const cancelled = await Effect.runPromiseExit(relay.observeLlmUnary(input, Effect.interrupt))

    expect(Exit.isFailure(failed)).toBe(true)
    expect(Exit.isFailure(cancelled)).toBe(true)
    expect(completed).toEqual([
      expect.objectContaining({ outcome: "success", finish: "stop", tokens: { inputTotal: 7, outputTotal: 3 } }),
      expect.objectContaining({ outcome: "failed" }),
      expect.objectContaining({ outcome: "cancelled" }),
    ])
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
    await Effect.runPromise(observation.complete("success", { terminalResult: "nonzero_exit" }))

    expect(emissions[0]?.measurements).toHaveLength(1)
    expect(emissions[0]?.measurements[0]?.attributes).toMatchObject({
      category: "extension",
      execution: "provider",
      outcome: "success",
    })
    expect(JSON.stringify(emissions)).not.toContain("private_provider_tool")
  })

  test("records a bounded terminal result without changing successful tool status", async () => {
    const emissions: Emission[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(emissions), ...traceDriver(records) })
    const observation = await Effect.runPromise(relay.beginTool({ name: "bash", execution: "local" }))
    const terminalResult = terminalResultFamily({ exit: 7421, command: "PRIVATE_COMMAND_CANARY" })

    await Effect.runPromise(observation.complete("success", { terminalResult }))

    expect(emissions).toHaveLength(1)
    expect(emissions[0]?.measurements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "opencode.tool_call.count",
          attributes: expect.objectContaining({ category: "terminal", execution: "local", outcome: "success" }),
        }),
        expect.objectContaining({
          name: "opencode.tool.terminal_result.count",
          attributes: expect.objectContaining({ result_family: "nonzero_exit", outcome: "success" }),
        }),
      ]),
    )
    const ended = records.find((item) => item.phase === "end" && item.kind === "tool")
    expect(ended).toMatchObject({
      payload: { result: { outcome: "success", terminal_result_family: "nonzero_exit" } },
      metadata: {
        "opencode.outcome": "success",
        "opencode.terminal_result_family": "nonzero_exit",
        "otel.status_code": "OK",
      },
    })
    expect(JSON.stringify({ emissions, records })).not.toContain("7421")
    expect(JSON.stringify({ emissions, records })).not.toContain("PRIVATE_COMMAND_CANARY")
  })

  test("claims tool completion when the completion effect runs and emits once", async () => {
    const completed: ToolCompleted[] = []
    const relay = makeForTesting(driver(), {
      toolCompleted: (input) => Effect.sync(() => completed.push(input)),
    })
    const observation = await Effect.runPromise(relay.beginTool({ name: "bash", execution: "local" }))
    const success = observation.complete("success", { terminalResult: "zero_exit" })
    const failed = observation.complete("failed", { terminalResult: "nonzero_exit" })

    await Effect.runPromise(failed)
    await Effect.runPromise(success)

    expect(completed).toHaveLength(1)
    expect(completed[0]).toMatchObject({
      name: "bash",
      execution: "local",
      outcome: "failed",
      terminalResult: "nonzero_exit",
    })
  })

  test("ignores terminal result details for non-terminal local tools", async () => {
    const emissions: Emission[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(emissions), ...traceDriver(records) })
    const observation = await Effect.runPromise(relay.beginTool({ name: "read", execution: "local" }))

    await Effect.runPromise(observation.complete("success", { terminalResult: "nonzero_exit" }))

    expect(JSON.stringify({ emissions, records })).not.toContain("terminal_result")
    expect(emissions[0]?.measurements.map((measurement) => measurement.name)).toEqual([
      "opencode.tool_call.count",
      "opencode.tool_call.duration",
    ])
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

  test("separates native physical attempts and retries from the logical host stream", async () => {
    const emissions: Emission[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(emissions), ...traceDriver(records) })
    const source = Stream.unwrap(
      Effect.gen(function* () {
        const observer = yield* RequestExecutor.CurrentAttemptObserver
        if (!observer) return Stream.fail(new Error("attempt observer missing"))
        yield* observer({ type: "started", attempt: "first" })
        yield* observer({
          type: "completed",
          attempt: "first",
          outcome: "failed",
          durationMs: 12,
          statusFamily: "5xx",
          errorKind: "provider_internal",
          retryable: true,
          willRetry: true,
        })
        yield* observer({
          type: "retry-scheduled",
          attempt: "first",
          nextAttempt: "second",
          delayMs: 500,
          delaySource: "backoff",
        })
        yield* observer({ type: "started", attempt: "second" })
        yield* observer({
          type: "completed",
          attempt: "second",
          outcome: "success",
          durationMs: 20,
          statusFamily: "2xx",
          willRetry: false,
        })
        return Stream.make({ type: "finish", reason: "stop" })
      }),
    )

    await Effect.runPromise(
      relay.observeRun(
        { runtime: "v2" },
        relay
          .observeLlmStream(
            {
              role: "primary",
              agentRuntime: "v2",
              runtime: "native",
              provider: "nvidia",
              model: "openai/gpt-oss-20b",
              protocol: "openai-chat",
            },
            source,
          )
          .pipe(Stream.runDrain),
      ),
    )

    expect(emissions.filter((emission) => emission.name === "opencode.llm.provider_attempt.started")).toHaveLength(2)
    const failed = emissions.find(
      (emission) =>
        emission.name === "opencode.llm.provider_attempt.completed" &&
        emission.measurements[0]?.attributes?.outcome === "failed",
    )
    expect(failed?.measurements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "opencode.llm.provider_attempt.completed.count",
          attributes: expect.objectContaining({
            attempt: "first",
            status_family: "5xx",
            error_kind: "provider_internal",
            retryable: "true",
            will_retry: "true",
            provider_family: "nvidia",
            operation: "openai.chat_completions",
          }),
        }),
        expect.objectContaining({ name: "opencode.llm.provider_attempt.time_to_headers", value: 12 }),
      ]),
    )
    expect(
      emissions.find((emission) => emission.name === "opencode.llm.provider_retry.scheduled")?.measurements,
    ).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ name: "opencode.llm.provider_retry.scheduled.count", value: 1 }),
        expect.objectContaining({ name: "opencode.llm.provider_retry.delay", value: 500 }),
      ]),
    )
    const operations = Object.fromEntries(
      (emissions.find((emission) => emission.name === "opencode.agent.run.completed")?.measurements ?? [])
        .filter((measurement) => measurement.name === "opencode.agent.run.operation_count")
        .map((measurement) => [measurement.attributes?.kind, measurement.value]),
    )
    expect(operations).toMatchObject({
      llm_operation: 1,
      provider_attempt: 2,
      provider_retry: 1,
      host_retry: 0,
    })
    const llm = records.find((record) => record.phase === "start" && record.kind === "llm")
    const attempts = records.filter(
      (record) => record.phase === "start" && record.name === "opencode.llm.provider_attempt",
    )
    expect(attempts).toHaveLength(2)
    expect(attempts.every((record) => record.parent === llm?.id)).toBe(true)
    expect(
      records.filter((record) => record.phase === "end" && record.name === "opencode.llm.provider_attempt"),
    ).toHaveLength(2)
    expect(
      records.find((record) => record.phase === "event" && record.name === "opencode.llm.provider_retry.scheduled")
        ?.parent,
    ).toBe(attempts[0]?.id)
    const retryIndex = records.findIndex(
      (record) => record.phase === "event" && record.name === "opencode.llm.provider_retry.scheduled",
    )
    const firstAttemptEnd = records.findIndex(
      (record) => record.phase === "end" && record.name === "opencode.llm.provider_attempt",
    )
    expect(retryIndex).toBeGreaterThan(-1)
    expect(firstAttemptEnd).toBeGreaterThan(retryIndex)
  })

  test("closes duplicate and malformed unmatched physical attempts before their logical LLM", async () => {
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(), ...traceDriver(records) })
    const source = Stream.unwrap(
      Effect.gen(function* () {
        const observer = yield* RequestExecutor.CurrentAttemptObserver
        if (!observer) return Stream.fail(new Error("attempt observer missing"))
        yield* observer({ type: "started", attempt: "first" })
        yield* observer({ type: "started", attempt: "first" })
        yield* observer({
          type: "retry-scheduled",
          attempt: "first",
          nextAttempt: "second",
          delayMs: 500,
          delaySource: "backoff",
        })
        return Stream.make({ type: "finish", reason: "stop" })
      }),
    )

    await Effect.runPromise(
      relay
        .observeLlmStream(
          {
            role: "primary",
            agentRuntime: "v2",
            runtime: "native",
            provider: "nvidia",
            model: "openai/gpt-oss-20b",
            protocol: "openai-chat",
          },
          source,
        )
        .pipe(Stream.runDrain),
    )

    const attemptEnds = records
      .map((record, index) => ({ record, index }))
      .filter(({ record }) => record.phase === "end" && record.name === "opencode.llm.provider_attempt")
    const llmEnd = records.findIndex((record) => record.phase === "end" && record.kind === "llm")
    expect(
      records.filter((record) => record.phase === "start" && record.name === "opencode.llm.provider_attempt"),
    ).toHaveLength(2)
    expect(attemptEnds).toHaveLength(2)
    expect(attemptEnds.every(({ index }) => llmEnd > index)).toBe(true)
    expect(attemptEnds.every(({ record }) => record.payload && typeof record.payload === "object")).toBe(true)
    expect(attemptEnds.map(({ record }) => record.payload)).toEqual([
      expect.objectContaining({ outcome: "failed", error_kind: "unknown" }),
      expect.objectContaining({ outcome: "failed", error_kind: "unknown" }),
    ])
    expect(records.some((record) => record.name === "opencode.llm.provider_retry.scheduled")).toBe(false)
  })

  test("observes privacy-bounded human question waits across every terminal resolution", async () => {
    const emissions: Emission[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(emissions), ...traceDriver(records) })

    await Effect.runPromise(
      relay.observeQuestionWait({ runtime: "v1" }, Effect.succeed("not-an-answer"), () => "answered"),
    )
    await Effect.runPromise(
      relay.observeQuestionWait({ runtime: "v2" }, Effect.fail("not-a-prompt"), () => "rejected").pipe(Effect.exit),
    )
    await Effect.runPromise(relay.observeQuestionWait({ runtime: "v2" }, Effect.fail("opaque")).pipe(Effect.exit))
    await Effect.runPromise(
      Effect.gen(function* () {
        const fiber = yield* relay.observeQuestionWait({ runtime: "v1" }, Effect.never).pipe(Effect.forkScoped)
        yield* Effect.yieldNow
        yield* Fiber.interrupt(fiber)
      }).pipe(Effect.scoped),
    )

    const questionEmissions = emissions.filter((emission) => emission.name === "opencode.question.wait.completed")
    expect(questionEmissions).toHaveLength(4)
    expect(
      questionEmissions.map(
        (emission) =>
          emission.measurements.find((measurement) => measurement.name === "opencode.question.wait.count")?.attributes
            ?.resolution,
      ),
    ).toEqual(["answered", "rejected", "unknown", "cancelled"])
    for (const emission of questionEmissions) {
      expect(emission.measurements).toEqual(
        expect.arrayContaining([
          expect.objectContaining({ name: "opencode.question.wait.count", value: 1 }),
          expect.objectContaining({ name: "opencode.question.wait.duration", kind: "histogram" }),
        ]),
      )
      expect(emission.measurements.every((measurement) => measurement.attributes?.runtime !== undefined)).toBe(true)
    }

    const starts = records.filter((record) => record.phase === "start" && record.name === "opencode.question.wait")
    const ends = records.filter((record) => record.phase === "end" && record.name === "opencode.question.wait")
    expect(starts).toHaveLength(4)
    expect(starts.every((record) => record.scopeType === "function")).toBe(true)
    expect(ends.map((record) => (record.payload as { resolution: string }).resolution)).toEqual([
      "answered",
      "rejected",
      "unknown",
      "cancelled",
    ])
    expect(JSON.stringify({ emissions: questionEmissions, starts, ends })).not.toContain("not-an-answer")
    expect(JSON.stringify({ emissions: questionEmissions, starts, ends })).not.toContain("not-a-prompt")
    expect(JSON.stringify({ emissions: questionEmissions, starts, ends })).not.toContain("opaque")
  })

  test("creates a run envelope, propagates tool scope, detaches background work, and keeps a blocked terminal outcome", async () => {
    const emissions: Emission[] = []
    const records: TraceRecord[] = []
    const relay = makeForTesting({ ...driver(emissions), ...traceDriver(records) })

    await Effect.runPromise(
      relay.observeRun(
        { runtime: "v2" },
        Effect.gen(function* () {
          yield* relay.observeTurn(
            { role: "primary", runtime: "v2" },
            Effect.gen(function* () {
              const tool = yield* relay.beginTool({ name: "task", category: "delegation", execution: "local" })
              yield* tool.run(
                Effect.gen(function* () {
                  yield* relay
                    .observeLlmStream(
                      {
                        role: "primary",
                        agentRuntime: "v2",
                        runtime: "native",
                        provider: "openai",
                        model: "gpt-5",
                        protocol: "openai-responses",
                      },
                      Stream.make({ type: "finish", reason: "stop" }),
                    )
                    .pipe(Stream.runDrain)
                  yield* relay.retryScheduled({
                    runtime: "v2",
                    attempt: 2,
                    delayMs: 2_500,
                    delaySource: "retry_after",
                    errorKind: "rate_limit",
                  })
                  yield* relay.permissionEvaluated({ runtime: "v2", family: "filesystem", effect: "ask" })
                  yield* relay.observePermissionWait({ runtime: "v2", family: "filesystem" }, Effect.void, () => "once")
                  yield* relay.observeQuestionWait({ runtime: "v2" }, Effect.void, () => "answered")
                }),
              )
              yield* tool.complete("success")
            }),
            () => "blocked",
          )
          yield* relay.detached(relay.observeTurn({ role: "title", runtime: "v2" }, Effect.void))
        }),
      ),
    )

    const run = records.find((record) => record.phase === "start" && record.name === "opencode.agent.run")
    const turns = records.filter((record) => record.phase === "start" && record.name === "opencode.agent.turn")
    const tool = records.find((record) => record.phase === "start" && record.kind === "tool")
    const llm = records.find((record) => record.phase === "start" && record.kind === "llm")
    const evaluation = records.find(
      (record) => record.phase === "start" && record.name === "opencode.permission.evaluated",
    )
    const evaluationEnd = records.find(
      (record) => record.phase === "end" && record.name === "opencode.permission.evaluated",
    )
    const permission = records.find((record) => record.phase === "start" && record.name === "opencode.permission.wait")
    const question = records.find((record) => record.phase === "start" && record.name === "opencode.question.wait")
    expect(turns).toHaveLength(2)
    expect(turns[0]?.parent).toBe(run?.id)
    expect(turns[1]?.parent).not.toBe(run?.id)
    expect(tool?.parent).toBe(turns[0]?.id)
    expect(llm?.parent).toBe(tool?.id)
    expect(evaluation?.parent).toBe(tool?.id)
    expect(evaluationEnd).toMatchObject({
      parent: tool?.id,
      payload: { effect: "ask" },
      metadata: {
        "opencode.permission_effect": "ask",
        "opencode.trace.schema_version": "3",
        "otel.status_code": "UNSET",
      },
    })
    expect(
      records.filter((record) => record.phase === "end" && record.name === "opencode.permission.evaluated"),
    ).toHaveLength(1)
    expect(permission?.parent).toBe(tool?.id)
    expect(question?.parent).toBe(tool?.id)
    expect(records.indexOf(evaluationEnd!)).toBeLessThan(
      records.findIndex((record) => record.phase === "end" && record.kind === "tool"),
    )

    expect(emissions.some((emission) => emission.name === "opencode.agent.run.started")).toBe(true)
    const completed = emissions.find((emission) => emission.name === "opencode.agent.run.completed")
    expect(
      completed?.measurements.find((measurement) => measurement.name === "opencode.agent.run.completed.count"),
    ).toMatchObject({ attributes: expect.objectContaining({ runtime: "v2", outcome: "blocked" }) })
    const operations = Object.fromEntries(
      (completed?.measurements ?? [])
        .filter((measurement) => measurement.name === "opencode.agent.run.operation_count")
        .map((measurement) => [measurement.attributes?.kind, measurement.value]),
    )
    expect(operations).toEqual({
      turn: 1,
      llm_operation: 1,
      tool_call: 1,
      host_retry: 1,
      provider_attempt: 0,
      provider_retry: 0,
      permission_wait: 1,
      question_wait: 1,
    })
    expect(
      emissions.find((emission) => emission.name === "opencode.permission.evaluated")?.measurements[0],
    ).toMatchObject({
      name: "opencode.permission.evaluation.count",
      attributes: expect.objectContaining({ runtime: "v2", permission_family: "filesystem", effect: "ask" }),
    })
    expect(emissions.find((emission) => emission.name === "opencode.permission.wait.completed")?.measurements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "opencode.permission.wait.count",
          attributes: expect.objectContaining({ resolution: "once" }),
        }),
        expect.objectContaining({ name: "opencode.permission.wait.duration" }),
      ]),
    )
    expect(emissions.find((emission) => emission.name === "opencode.llm.host_retry.scheduled")?.measurements).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          name: "opencode.llm.host_retry_scheduled.count",
          attributes: expect.objectContaining({
            attempt: "second",
            delay_source: "retry_after",
            error_kind: "rate_limit",
          }),
        }),
        expect.objectContaining({ name: "opencode.llm.host_retry.delay", value: 2_500 }),
      ]),
    )
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
