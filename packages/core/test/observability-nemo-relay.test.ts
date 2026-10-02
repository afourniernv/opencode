import { describe, expect, test } from "bun:test"
import { Effect } from "effect"
import {
  durationBucket,
  finishReason,
  makeForTesting,
  modelFamily,
  providerFamily,
  toolCategory,
  toolDurationBucket,
} from "@opencode-ai/core/observability/nemo-relay"

type Emission = {
  readonly name: string
  readonly measurements: ReadonlyArray<{
    readonly name: string
    readonly value: number
    readonly attributes?: Record<string, string | number | boolean>
  }>
  readonly metadata?: Record<string, string | number | boolean> | null
}

function driver(emissions: Emission[]) {
  return {
    MetricKind: { Counter: "counter" },
    MetricValueType: { U64: "u64" },
    metric(name: string, measurements: Emission["measurements"], _handle?: null, metadata?: Emission["metadata"]) {
      emissions.push({ name, measurements, metadata })
    },
    flushSubscribers: async () => {},
  }
}

describe("NeMo Relay observability", () => {
  test("maps arbitrary identifiers into closed families and categories", () => {
    expect(providerFamily("openrouter-team-secret")).toBe("openrouter")
    expect(providerFamily("customer-provider-with-private-name")).toBe("custom")
    expect(modelFamily("tenant/private-model-name")).toBe("custom")
    expect(modelFamily("anthropic/claude-sonnet-4")).toBe("claude")
    expect(toolCategory("read")).toBe("file_read")
    expect(toolCategory("private_customer_tool")).toBe("extension")
    expect(finishReason("tool-calls")).toBe("tool_calls")
    expect(finishReason("provider-private-reason")).toBe("unknown")
  })

  test("uses bounded duration buckets", () => {
    expect(durationBucket(999)).toBe("lt_1s")
    expect(durationBucket(1_000)).toBe("1s_to_5s")
    expect(durationBucket(600_000)).toBe("gte_10m")
    expect(durationBucket(Number.NaN)).toBe("unknown")
    expect(toolDurationBucket(99)).toBe("lt_100ms")
    expect(toolDurationBucket(30_000)).toBe("gte_30s")
  })

  test("emits an allowlisted logical-call projection without raw identifiers", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.llmCompleted({
        role: "primary",
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
    expect(emissions[0]?.name).toBe("opencode.llm.completed")
    expect(emissions[0]?.measurements.map((item) => item.name)).toEqual([
      "opencode.llm.logical_call.count",
      "opencode.llm.duration_bucket.count",
      "opencode.model_route.count",
      "opencode.llm.finish_reason.count",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
      "opencode.llm.tokens",
    ])
    expect(emissions[0]?.measurements[2]?.attributes).toEqual({
      provider_family: "custom",
      model_family: "custom",
    })
    expect(JSON.stringify(emissions)).not.toContain("customer-provider-with-private-name")
    expect(JSON.stringify(emissions)).not.toContain("tenant/private-model-name")
  })

  test("records provider tools without claiming local execution latency", async () => {
    const emissions: Emission[] = []
    const relay = makeForTesting(driver(emissions))

    await Effect.runPromise(
      relay.toolCompleted({
        name: "private_provider_tool",
        execution: "provider",
        outcome: "success",
        durationMs: 42,
      }),
    )

    expect(emissions[0]?.measurements).toHaveLength(1)
    expect(emissions[0]?.measurements[0]?.attributes).toEqual({
      category: "provider",
      execution: "provider",
      outcome: "success",
    })
    expect(JSON.stringify(emissions)).not.toContain("private_provider_tool")
  })

  test("keeps metric failures fail-open", async () => {
    const relay = makeForTesting({
      ...driver([]),
      metric() {
        throw new Error("collector unavailable")
      },
    })

    await expect(
      Effect.runPromise(
        relay.toolCompleted({
          name: "read",
          execution: "local",
          outcome: "success",
          durationMs: 10,
        }),
      ),
    ).resolves.toBeUndefined()
  })
})
