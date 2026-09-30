import { SessionV1 } from "@opencode-ai/core/v1/session"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Database } from "@opencode-ai/core/database/database"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2Bridge } from "@/event-v2-bridge"
import { expect } from "bun:test"
import { APICallError, tool } from "ai"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Stream } from "effect"
import path from "path"
import z from "zod"
import type { Agent } from "../../src/agent/agent"
import { Provider } from "@/provider/provider"

import { Session } from "@/session/session"
import { LLM } from "../../src/session/llm"
import { MessageV2 } from "../../src/session/message-v2"
import { SessionProcessor } from "../../src/session/processor"
import { MessageID, PartID, SessionID } from "../../src/session/schema"
import { SessionStatus } from "../../src/session/status"
import { SessionSummary } from "../../src/session/summary"
import { CrossSpawnSpawner } from "@opencode-ai/core/cross-spawn-spawner"
import { provideTmpdirInstance, provideTmpdirServer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { raw, reply, TestLLMServer } from "../lib/llm-server"
import { RuntimeFlags } from "@/effect/runtime-flags"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { ModelV2 } from "@opencode-ai/core/model"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { LLMEvent } from "@opencode-ai/llm"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"
import { markDynamicMcpTool } from "@/session/tool-semantics"

const summary = Layer.succeed(
  SessionSummary.Service,
  SessionSummary.Service.of({
    summarize: () => Effect.void,
    diff: () => Effect.succeed([]),
    computeDiff: () => Effect.succeed([]),
  }),
)

const ref = {
  providerID: ProviderV2.ID.make("test"),
  modelID: ModelV2.ID.make("test-model"),
}

const cfg = {
  provider: {
    test: {
      name: "Test",
      id: "test",
      env: [],
      npm: "@ai-sdk/openai-compatible",
      models: {
        "test-model": {
          id: "test-model",
          name: "Test Model",
          attachment: false,
          reasoning: false,
          temperature: false,
          tool_call: true,
          release_date: "2025-01-01",
          limit: { context: 100000, output: 10000 },
          cost: { input: 0, output: 0 },
          options: {},
        },
      },
      options: {
        apiKey: "test-key",
        baseURL: "http://localhost:1/v1",
      },
    },
  },
}

function providerCfg(url: string) {
  return {
    ...cfg,
    provider: {
      ...cfg.provider,
      test: {
        ...cfg.provider.test,
        options: {
          ...cfg.provider.test.options,
          baseURL: url,
        },
      },
    },
  }
}

function agent(): Agent.Info {
  return {
    name: "build",
    mode: "primary",
    options: {},
    permission: [{ permission: "*", pattern: "*", action: "allow" }],
  }
}

function defer<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const waitFor = <A, E, R>(check: Effect.Effect<A | undefined, E, R>, message: string) =>
  Effect.gen(function* () {
    const stop = Date.now() + 500
    while (Date.now() < stop) {
      const value = yield* check
      if (value !== undefined) return value
      yield* Effect.sleep("10 millis")
    }
    return yield* Effect.fail(new Error(message))
  })

const user = Effect.fn("TestSession.user")(function* (sessionID: SessionID, text: string) {
  const session = yield* Session.Service
  const msg = yield* session.updateMessage({
    id: MessageID.ascending(),
    role: "user",
    sessionID,
    agent: "build",
    model: ref,
    time: { created: Date.now() },
  })
  yield* session.updatePart({
    id: PartID.ascending(),
    messageID: msg.id,
    sessionID,
    type: "text",
    text,
  })
  return msg
})

const assistant = Effect.fn("TestSession.assistant")(function* (
  sessionID: SessionID,
  parentID: MessageID,
  root: string,
) {
  const session = yield* Session.Service
  const msg: SessionV1.Assistant = {
    id: MessageID.ascending(),
    role: "assistant",
    sessionID,
    mode: "build",
    agent: "build",
    path: { cwd: root, root },
    cost: 0,
    tokens: {
      total: 0,
      input: 0,
      output: 0,
      reasoning: 0,
      cache: { read: 0, write: 0 },
    },
    modelID: ref.modelID,
    providerID: ref.providerID,
    parentID,
    time: { created: Date.now() },
    finish: "end_turn",
  }
  yield* session.updateMessage(msg)
  return msg
})

const root = LayerNode.group([
  SessionProcessor.node,
  Session.node,
  SessionProjector.node,
  Provider.node,
  Database.node,
  EventV2Bridge.node,
  SessionStatus.node,
  CrossSpawnSpawner.node,
])
const replacements = [
  [SessionSummary.node, summary],
  [RuntimeFlags.node, RuntimeFlags.layer({ experimentalEventSystem: true })],
] as const
const env = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  replacements,
)

const it = testEffect(env)

const providerErrorLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: {}, providerExecuted: true }),
        LLMEvent.toolResult({
          id: "call-1",
          name: "lookup",
          result: { type: "error", value: "provider boom" },
          providerExecuted: true,
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const providerToolObservations: NemoRelay.ToolCompleted[] = []
const relay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => providerToolObservations.push(input)) },
    ),
  ),
)
const providerErrorEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, providerErrorLLM],
  [NemoRelay.node, relay],
])
const itProviderError = testEffect(providerErrorEnv)

const localSuccessLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-1", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-1", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-1", name: "lookup", input: {} }),
        LLMEvent.toolResult({
          id: "call-1",
          name: "lookup",
          result: { type: "json", value: { title: "Lookup", output: "ok", metadata: {} } },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const localToolObservations: NemoRelay.ToolCompleted[] = []
const localRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => localToolObservations.push(input)) },
    ),
  ),
)
const localSuccessEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, localSuccessLLM],
  [NemoRelay.node, localRelay],
])
const itLocalSuccess = testEffect(localSuccessEnv)

const terminalResultLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-nonzero", name: "bash" }),
        LLMEvent.toolInputEnd({ id: "call-nonzero", name: "bash" }),
        LLMEvent.toolCall({ id: "call-nonzero", name: "bash", input: {} }),
        LLMEvent.toolResult({
          id: "call-nonzero",
          name: "bash",
          result: {
            type: "json",
            value: { title: "Private command", output: "private output", metadata: { exit: 7421 } },
          },
        }),
        LLMEvent.toolInputStart({ id: "call-zero", name: "shell" }),
        LLMEvent.toolInputEnd({ id: "call-zero", name: "shell" }),
        LLMEvent.toolCall({ id: "call-zero", name: "shell", input: {} }),
        LLMEvent.toolResult({
          id: "call-zero",
          name: "shell",
          result: { type: "json", value: { title: "Shell", output: "ok", metadata: { exit: 0 } } },
        }),
        LLMEvent.toolInputStart({ id: "call-timeout", name: "bash" }),
        LLMEvent.toolInputEnd({ id: "call-timeout", name: "bash" }),
        LLMEvent.toolCall({ id: "call-timeout", name: "bash", input: {} }),
        LLMEvent.toolResult({
          id: "call-timeout",
          name: "bash",
          result: {
            type: "json",
            value: { title: "Bash", output: "timed out", metadata: { exit: null, timeout: true } },
          },
        }),
        LLMEvent.toolInputStart({ id: "call-extension", name: "customer_extension" }),
        LLMEvent.toolInputEnd({ id: "call-extension", name: "customer_extension" }),
        LLMEvent.toolCall({ id: "call-extension", name: "customer_extension", input: {} }),
        LLMEvent.toolResult({
          id: "call-extension",
          name: "customer_extension",
          result: {
            type: "json",
            value: { title: "Extension", output: "ok", metadata: { exit: 19, timeout: true } },
          },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const terminalResultObservations: NemoRelay.ToolCompleted[] = []
const terminalResultRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => terminalResultObservations.push(input)) },
    ),
  ),
)
const terminalResultEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, terminalResultLLM],
  [NemoRelay.node, terminalResultRelay],
])
const itTerminalResult = testEffect(terminalResultEnv)

const dynamicMcpToolName = "private-server_finance_lookup"
const dynamicMcpLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-mcp", name: dynamicMcpToolName }),
        LLMEvent.toolInputEnd({ id: "call-mcp", name: dynamicMcpToolName }),
        LLMEvent.toolCall({ id: "call-mcp", name: dynamicMcpToolName, input: {} }),
        LLMEvent.toolResult({
          id: "call-mcp",
          name: dynamicMcpToolName,
          result: { type: "json", value: { title: "MCP", output: "ok", metadata: {} } },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const dynamicMcpToolObservations: NemoRelay.ToolCompleted[] = []
const dynamicMcpRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => dynamicMcpToolObservations.push(input)) },
    ),
  ),
)
const dynamicMcpEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, dynamicMcpLLM],
  [NemoRelay.node, dynamicMcpRelay],
])
const itDynamicMcp = testEffect(dynamicMcpEnv)

const interruptedToolObservations: NemoRelay.ToolCompleted[] = []
const interruptedRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => interruptedToolObservations.push(input)) },
    ),
  ),
)
const interruptedEnv = LayerNode.compile(
  LayerNode.group([root, LayerNode.make({ service: TestLLMServer, layer: TestLLMServer.layer, deps: [] })]),
  [...replacements, [NemoRelay.node, interruptedRelay]],
)
const itInterrupted = testEffect(interruptedEnv)

let missingPartGate = defer<void>()
const missingPartLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-missing", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-missing", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-missing", name: "lookup", input: {} }),
      ).pipe(
        Stream.concat(
          Stream.unwrap(
            Effect.promise(() => missingPartGate.promise).pipe(
              Effect.as(
                Stream.make(
                  LLMEvent.toolResult({
                    id: "call-missing",
                    name: "lookup",
                    result: { type: "json", value: { title: "Lookup", output: "ok", metadata: {} } },
                  }),
                  LLMEvent.stepFinish({ index: 0, reason: "stop" }),
                  LLMEvent.finish({ reason: "stop" }),
                ),
              ),
            ),
          ),
        ),
      ),
  }),
)
const missingPartObservations: NemoRelay.ToolCompleted[] = []
const missingPartRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(
      {
        MetricKind: { Counter: "counter", Histogram: "histogram" },
        MetricValueType: { U64: "u64", F64: "f64" },
        metric() {},
        flushSubscribers: async () => {},
      },
      { toolCompleted: (input) => Effect.sync(() => missingPartObservations.push(input)) },
    ),
  ),
)
const missingPartEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, missingPartLLM],
  [NemoRelay.node, missingPartRelay],
])
const itMissingPart = testEffect(missingPartEnv)

const reusedCallIDLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-reused", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-reused", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-reused", name: "lookup", input: { generation: 1 } }),
        LLMEvent.toolResult({
          id: "call-reused",
          name: "lookup",
          result: { type: "json", value: { title: "First", output: "one", metadata: {} } },
        }),
        LLMEvent.toolInputStart({ id: "call-reused", name: "lookup" }),
        LLMEvent.toolInputEnd({ id: "call-reused", name: "lookup" }),
        LLMEvent.toolCall({ id: "call-reused", name: "lookup", input: { generation: 2 } }),
        LLMEvent.toolResult({
          id: "call-reused",
          name: "lookup",
          result: { type: "json", value: { title: "Second", output: "two", metadata: {} } },
        }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      ),
  }),
)
const reusedCallIDEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, reusedCallIDLLM],
  [NemoRelay.node, localRelay],
])
const itReusedCallID = testEffect(reusedCallIDEnv)

const fragmentFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.reasoningStart({ id: "reasoning-1" }),
        LLMEvent.reasoningDelta({ id: "reasoning-1", text: "thinking" }),
        LLMEvent.textStart({ id: "text-1" }),
        LLMEvent.textDelta({ id: "text-1", text: "partial" }),
        LLMEvent.providerError({ message: "provider boom" }),
      ),
  }),
)
const fragmentFailureEnv = LayerNode.compile(root, [...replacements, [LLM.node, fragmentFailureLLM]])
const itFragmentFailure = testEffect(fragmentFailureEnv)

const relayMetrics = {
  MetricKind: { Counter: "counter", Histogram: "histogram" },
  MetricValueType: { U64: "u64", F64: "f64" },
  metric() {},
  flushSubscribers: async () => {},
}

let retryMetadataStreamCalls = 0
const retryMetadataLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () => {
      retryMetadataStreamCalls++
      if (retryMetadataStreamCalls === 1) {
        return Stream.fail(
          new APICallError({
            message: "Too many requests",
            url: "https://provider.invalid/v1/chat/completions",
            requestBodyValues: {},
            statusCode: 429,
            responseHeaders: { "retry-after-ms": "0" },
            responseBody: JSON.stringify({ error: { type: "rate_limit_error" } }),
          }),
        )
      }
      return Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.stepFinish({ index: 0, reason: "stop" }),
        LLMEvent.finish({ reason: "stop" }),
      )
    },
  }),
)
const retryMetadataObservations: Parameters<NemoRelay.Interface["retryScheduled"]>[0][] = []
const retryMetadataService = NemoRelay.makeForTesting(relayMetrics)
const retryMetadataRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of({
    ...retryMetadataService,
    retryScheduled: (input) => Effect.sync(() => retryMetadataObservations.push(input)),
  }),
)
const retryMetadataEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, retryMetadataLLM],
  [NemoRelay.node, retryMetadataRelay],
])
const itRetryMetadata = testEffect(retryMetadataEnv)

function policyFailureLLM(error: PermissionV1.Error) {
  return Layer.succeed(
    LLM.Service,
    LLM.Service.of({
      stream: () =>
        Stream.make(
          LLMEvent.stepStart({ index: 0 }),
          LLMEvent.toolInputStart({ id: "call-policy", name: "lookup" }),
          LLMEvent.toolInputEnd({ id: "call-policy", name: "lookup" }),
          LLMEvent.toolCall({ id: "call-policy", name: "lookup", input: {} }),
          LLMEvent.toolError({ id: "call-policy", name: "lookup", message: error.message, error }),
          LLMEvent.stepFinish({ index: 0, reason: "stop" }),
          LLMEvent.finish({ reason: "stop" }),
        ),
    }),
  )
}

function policyFailureEnv(error: PermissionV1.Error, observations: NemoRelay.ToolCompleted[]) {
  const policyRelay = Layer.succeed(
    NemoRelay.Service,
    NemoRelay.Service.of(
      NemoRelay.makeForTesting(relayMetrics, {
        toolCompleted: (input) => Effect.sync(() => observations.push(input)),
      }),
    ),
  )
  return LayerNode.compile(root, [...replacements, [LLM.node, policyFailureLLM(error)], [NemoRelay.node, policyRelay]])
}

const deniedToolObservations: NemoRelay.ToolCompleted[] = []
const correctedToolObservations: NemoRelay.ToolCompleted[] = []
const rejectedToolObservations: NemoRelay.ToolCompleted[] = []
const itDeniedTool = testEffect(policyFailureEnv(new PermissionV1.DeniedError({ ruleset: [] }), deniedToolObservations))
const itCorrectedTool = testEffect(
  policyFailureEnv(new PermissionV1.CorrectedError({ feedback: "Use another tool" }), correctedToolObservations),
)
const itRejectedTool = testEffect(policyFailureEnv(new PermissionV1.RejectedError(), rejectedToolObservations))

let cleanupFailureGate = defer<void>()
const cleanupFailureLLM = Layer.succeed(
  LLM.Service,
  LLM.Service.of({
    stream: () =>
      Stream.make(
        LLMEvent.stepStart({ index: 0 }),
        LLMEvent.toolInputStart({ id: "call-first", name: "first" }),
        LLMEvent.toolInputEnd({ id: "call-first", name: "first" }),
        LLMEvent.toolCall({ id: "call-first", name: "first", input: {} }),
        LLMEvent.toolInputStart({ id: "call-second", name: "second" }),
        LLMEvent.toolInputEnd({ id: "call-second", name: "second" }),
        LLMEvent.toolCall({ id: "call-second", name: "second", input: {} }),
      ).pipe(
        Stream.concat(
          Stream.unwrap(
            Effect.promise(() => cleanupFailureGate.promise).pipe(
              Effect.as(Stream.make(LLMEvent.providerError({ message: "provider boom" }))),
            ),
          ),
        ),
      ),
  }),
)
const cleanupFailureObservations: NemoRelay.ToolCompleted[] = []
const cleanupFailureRelay = Layer.succeed(
  NemoRelay.Service,
  NemoRelay.Service.of(
    NemoRelay.makeForTesting(relayMetrics, {
      toolCompleted: (input) =>
        Effect.sync(() => {
          cleanupFailureObservations.push(input)
          if (input.name === "first") throw new Error("first Relay completion failed")
        }),
    }),
  ),
)
const cleanupFailureEnv = LayerNode.compile(root, [
  ...replacements,
  [LLM.node, cleanupFailureLLM],
  [NemoRelay.node, cleanupFailureRelay],
])
const itCleanupFailure = testEffect(cleanupFailureEnv)

const boot = Effect.fn("test.boot")(function* () {
  const processors = yield* SessionProcessor.Service
  const session = yield* Session.Service
  const provider = yield* Provider.Service
  return { processors, session, provider }
})

const runPolicyFailure = Effect.fn("test.runPolicyFailure")(function* (dir: string, prompt: string) {
  const { processors, session, provider } = yield* boot()
  const chat = yield* session.create({})
  const parent = yield* user(chat.id, prompt)
  const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
  const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
  const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
  const result = yield* handle.process({
    user: {
      id: parent.id,
      sessionID: chat.id,
      role: "user",
      time: parent.time,
      agent: parent.agent,
      model: { providerID: ref.providerID, modelID: ref.modelID },
    } satisfies SessionV1.User,
    sessionID: chat.id,
    model: mdl,
    agent: agent(),
    system: [],
    messages: [{ role: "user", content: prompt }],
    tools: {},
  })
  const parts = yield* MessageV2.parts(msg.id)
  return {
    result,
    call: parts.find((part): part is SessionV1.ToolPart => part.type === "tool"),
  }
})

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

it.live("session.processor effect tests capture llm input cleanly", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("hello")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const input = {
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "hi" }],
          tools: {},
        } satisfies LLM.StreamInput

        const value = yield* handle.process(input)
        const parts = yield* MessageV2.parts(msg.id)
        const calls = yield* llm.calls

        expect(value).toBe("continue")
        expect(calls).toBe(1)
        expect(parts.some((part) => part.type === "text" && part.text === "hello")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests preserve text start time", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const gate = defer<void>()
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            head: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { role: "assistant" } }],
              },
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: { content: "hello" } }],
              },
            ],
            wait: gate.promise,
            tail: [
              {
                id: "chatcmpl-test",
                object: "chat.completion.chunk",
                choices: [{ delta: {}, finish_reason: "stop" }],
              },
            ],
          }),
        )

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "hi")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "hi" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.TextPart => part.type === "text")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for text part",
        )
        yield* Effect.sleep("20 millis")
        gate.resolve()

        const exit = yield* Fiber.await(run)
        const text = (yield* MessageV2.parts(msg.id)).find((part): part is SessionV1.TextPart => part.type === "text")

        expect(Exit.isSuccess(exit)).toBe(true)
        expect(text?.text).toBe("hello")
        expect(text?.time?.start).toBeDefined()
        expect(text?.time?.end).toBeDefined()
        if (!text?.time?.start || !text.time.end) return
        expect(text.time.start).toBeLessThan(text.time.end)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests stop after token overflow requests compaction", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.text("after", { usage: { input: 100, output: 0 } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const base = yield* provider.getModel(ref.providerID, ref.modelID)
        const mdl = { ...base, limit: { context: 20, output: 10 } }
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("compact")
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(parts.some((part) => part.type === "step-finish")).toBe(true)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests capture reasoning from http mock", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("think").text("done").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.find((part): part is SessionV1.ReasoningPart => part.type === "reasoning")
        const text = parts.find((part): part is SessionV1.TextPart => part.type === "text")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(reasoning?.text).toBe("think")
        expect(text?.text).toBe("done")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests reset reasoning state across retries", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        providerToolObservations.length = 0
        const { processors, session, provider } = yield* boot()

        yield* llm.push(reply().reason("one").reset(), reply().reason("two").stop())

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reason")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reason" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const reasoning = parts.filter((part): part is SessionV1.ReasoningPart => part.type === "reasoning")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(reasoning.some((part) => part.text === "two")).toBe(true)
        expect(reasoning.some((part) => part.text === "onetwo")).toBe(false)
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests do not retry unknown json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { error: { message: "no_kv_space" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "json" }],
          tools: {},
        })

        expect(value).toBe("stop")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error?.name).toBe("APIError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry recognized structured json errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(429, { type: "error", error: { type: "too_many_requests" } })
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry json" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry OpenAI-compatible midstream server errors", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(raw({ chunks: [{ error: { type: "server_error", code: "server_error", message: "xxx" } }] }))
        yield* llm.text("after")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry midstream server error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry midstream server error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests retry network_error finish reasons", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.push(
          raw({
            chunks: [
              {
                id: "chatcmpl-network-error",
                object: "chat.completion.chunk",
                choices: [{ index: 0, delta: { role: "assistant", content: "" }, finish_reason: "network_error" }],
              },
            ],
          }),
        )
        yield* llm.text("after retry")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry network error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry network error" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(parts.some((part) => part.type === "text" && part.text === "after retry")).toBe(true)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests publish retry status updates", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        yield* llm.error(503, { error: "boom" })
        yield* llm.text("")

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const states: number[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== SessionStatus.Event.Status.type) return Effect.void
          const data = evt.data as typeof SessionStatus.Event.Status.data.Type
          if (data.sessionID === chat.id && data.status.type === "retry") states.push(data.status.attempt)
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry" }],
          tools: {},
        })

        yield* off

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(2)
        expect(states).toStrictEqual([1])
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itRetryMetadata.live("session.processor forwards enriched retry metadata to Relay", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        retryMetadataStreamCalls = 0
        retryMetadataObservations.length = 0
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "retry metadata")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "retry metadata" }],
          tools: {},
        })

        expect(value).toBe("continue")
        expect(retryMetadataStreamCalls).toBe(2)
        expect(retryMetadataObservations).toEqual([
          {
            runtime: "v1",
            attempt: 1,
            delayMs: 0,
            delaySource: "retry_after",
            errorKind: "rate_limit",
          },
        ])
      }),
    { config: cfg },
  ),
)

it.live("session.processor effect tests compact on structured context overflow", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.error(400, { type: "error", error: { code: "context_length_exceeded" } })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "compact json")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "compact json" }],
          tools: {},
        })

        expect(value).toBe("compact")
        expect(yield* llm.calls).toBe(1)
        expect(handle.message.error).toBeUndefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests complete AI SDK tool calls when native flag is off", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()

        yield* llm.tool("lookup", { query: "weather" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const value = yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "tool" }],
          tools: {
            lookup: tool({
              description: "Look up information",
              inputSchema: z.object({ query: z.string() }),
              execute: async (input) => ({
                title: "Weather lookup",
                output: `result:${input.query}`,
                metadata: { source: "test" },
              }),
            }),
          },
        })

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(value).toBe("continue")
        expect(yield* llm.calls).toBe(1)
        expect(call?.callID).toBe("call_1")
        expect(call?.tool).toBe("lookup")
        expect(call?.state.status).toBe("completed")
        if (call?.state.status !== "completed") return
        expect(call.state.input).toEqual({ query: "weather" })
        expect(call.state.output).toBe("result:weather")
        expect(call.state.title).toBe("Weather lookup")
        expect(call.state.metadata).toEqual({ source: "test" })
        expect(call.state.time.start).toBeDefined()
        expect(call.state.time.end).toBeDefined()
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark pending tools as aborted on cleanup", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.toolHang("bash", { cmd: "pwd" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "tool abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "tool abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => parts.find((part): part is SessionV1.ToolPart => part.type === "tool")),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for tool part",
        )
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(yield* llm.calls).toBe(1)
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") {
          expect(call.state.error).toBe("Tool execution aborted")
          expect(call.state.metadata?.interrupted).toBe(true)
          expect(call.state.time.end).toBeDefined()
        }
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itInterrupted.live("session.processor effect tests report interrupted running tools as cancelled", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        interruptedToolObservations.length = 0
        const started = defer<void>()
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()

        yield* llm.tool("lookup", { query: "weather" })

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt running tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt running tool" }],
            tools: {
              lookup: tool({
                description: "Wait until interrupted",
                inputSchema: z.object({ query: z.string() }),
                execute: async (_input, options) => {
                  started.resolve()
                  return await new Promise<{ title: string; output: string; metadata: Record<string, never> }>(
                    (_resolve, reject) => {
                      const abort = () => reject(new DOMException("Aborted", "AbortError"))
                      if (options.abortSignal?.aborted) abort()
                      else options.abortSignal?.addEventListener("abort", abort, { once: true })
                    },
                  )
                },
              }),
            },
          })
          .pipe(Effect.forkChild)

        yield* Effect.promise(() => started.promise)
        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) =>
              parts.find((part): part is SessionV1.ToolPart => part.type === "tool" && part.state.status === "running"),
            ),
            Effect.provideService(Database.Service, database),
          ),
          "timed out waiting for running tool part",
        )
        yield* Fiber.interrupt(run)

        expect(interruptedToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "cancelled" }),
        ])
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests record aborted errors and idle state", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const seen = defer<void>()
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "abort")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const errs: string[] = []
        const off = yield* events.listen((evt) => {
          if (evt.type !== Session.Event.Error.type) return Effect.void
          const data = evt.data as typeof Session.Event.Error.data.Type
          if (data.sessionID !== chat.id || !data.error) return Effect.void
          errs.push(data.error.name)
          seen.resolve()
          return Effect.void
        })
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "abort" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        yield* Effect.promise(() => seen.promise)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)
        yield* off

        expect(Exit.isFailure(exit)).toBe(true)
        if (Exit.isFailure(exit)) {
          expect(Cause.hasInterruptsOnly(exit.cause)).toBe(true)
        }
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
        expect(errs).toContain("MessageAbortedError")
      }),
    { config: (url) => providerCfg(url) },
  ),
)

it.live("session.processor effect tests mark interruptions aborted without manual abort", () =>
  provideTmpdirServer(
    ({ dir, llm }) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const sts = yield* SessionStatus.Service

        yield* llm.hang

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({
          assistantMessage: msg,
          sessionID: chat.id,
          model: mdl,
        })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* llm.wait(1)
        yield* Fiber.interrupt(run)

        const exit = yield* Fiber.await(run)
        const stored = yield* MessageV2.get({ sessionID: chat.id, messageID: msg.id })
        const state = yield* sts.get(chat.id)

        expect(Exit.isFailure(exit)).toBe(true)
        expect(handle.message.error?.name).toBe("MessageAbortedError")
        expect(stored.info.role).toBe("assistant")
        if (stored.info.role === "assistant") {
          expect(stored.info.error?.name).toBe("MessageAbortedError")
        }
        expect(state).toMatchObject({ type: "idle" })
      }),
    { config: (url) => providerCfg(url) },
  ),
)

itProviderError.live("session.processor effect tests fail provider-executed error results", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider tool error")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "provider tool error" }],
          tools: {},
        })
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(call?.state.status).toBe("error")
        if (call?.state.status === "error") expect(call.state.error).toBe("provider boom")
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(MessageV2.Event.Updated.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
        expect(providerToolObservations).toEqual([
          {
            name: "lookup",
            outcome: "failed",
            execution: "provider",
            durationMs: undefined,
          },
        ])
      }),
    { config: cfg },
  ),
)

itDeniedTool.live("session.processor effect tests continue after a denied tool while reporting blocked", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        deniedToolObservations.length = 0
        const result = yield* runPolicyFailure(dir, "denied tool")

        expect(result.result).toBe("continue")
        expect(result.call?.state.status).toBe("error")
        expect(deniedToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "blocked" }),
        ])
      }),
    { config: cfg },
  ),
)

itCorrectedTool.live("session.processor effect tests continue after a corrected tool while reporting blocked", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        correctedToolObservations.length = 0
        const result = yield* runPolicyFailure(dir, "corrected tool")

        expect(result.result).toBe("continue")
        expect(result.call?.state.status).toBe("error")
        expect(correctedToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "blocked" }),
        ])
      }),
    { config: cfg },
  ),
)

itRejectedTool.live("session.processor effect tests stop after a rejected tool while reporting blocked", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        rejectedToolObservations.length = 0
        const result = yield* runPolicyFailure(dir, "rejected tool")

        expect(result.result).toBe("stop")
        expect(result.call?.state.status).toBe("error")
        expect(rejectedToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "blocked" }),
        ])
      }),
    { config: cfg },
  ),
)

itCleanupFailure.live("session.processor effect tests settle every Relay tool after cleanup I/O failure", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        cleanupFailureGate = defer<void>()
        cleanupFailureObservations.length = 0
        const database = yield* Database.Service
        const { processors, session, provider } = yield* boot()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "cleanup failure")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "cleanup failure" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) => {
              const calls = parts.filter((part): part is SessionV1.ToolPart => part.type === "tool")
              return calls.length === 2 && calls.every((call) => call.state.status === "running") ? calls : undefined
            }),
          ),
          "timed out waiting for running cleanup tool parts",
        )

        // Rename the table only after both tools are durably admitted. This
        // makes cleanup getPart fail for every call without permanently
        // damaging the test database.
        yield* database.db.run("ALTER TABLE part RENAME TO part_cleanup_failure")
        cleanupFailureGate.resolve()
        const exit = yield* Fiber.await(run).pipe(
          Effect.ensuring(database.db.run("ALTER TABLE part_cleanup_failure RENAME TO part").pipe(Effect.ignore)),
        )

        expect(Exit.isFailure(exit)).toBe(true)
        expect(cleanupFailureObservations).toEqual([
          expect.objectContaining({ name: "first", execution: "local", outcome: "failed" }),
          expect.objectContaining({ name: "second", execution: "local", outcome: "failed" }),
        ])
      }).pipe(Effect.ensuring(Effect.sync(() => cleanupFailureGate.resolve()))),
    { config: cfg },
  ),
)

itLocalSuccess.live("session.processor effect tests observe completed local tools", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        localToolObservations.length = 0
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "local tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "local tool" }],
          tools: {},
        })

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(call?.state.status).toBe("completed")
        expect(localToolObservations).toEqual([
          {
            name: "lookup",
            outcome: "success",
            execution: "local",
            durationMs: expect.any(Number),
          },
        ])
      }),
    { config: cfg },
  ),
)

itTerminalResult.live("session.processor records bounded terminal results without failing successful tools", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        terminalResultObservations.length = 0
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "terminal results")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "terminal results" }],
          tools: {},
        })

        expect(terminalResultObservations).toEqual([
          expect.objectContaining({
            name: "bash",
            execution: "local",
            outcome: "success",
            terminalResult: "nonzero_exit",
          }),
          expect.objectContaining({
            name: "shell",
            execution: "local",
            outcome: "success",
            terminalResult: "zero_exit",
          }),
          expect.objectContaining({
            name: "bash",
            execution: "local",
            outcome: "success",
            terminalResult: "timeout",
          }),
          expect.objectContaining({
            name: "customer_extension",
            execution: "local",
            outcome: "success",
          }),
        ])
        expect(terminalResultObservations[3]).not.toHaveProperty("terminalResult")
      }),
    { config: cfg },
  ),
)

itDynamicMcp.live("session.processor classifies marked dynamic MCP tools without parsing their names", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        dynamicMcpToolObservations.length = 0
        const { processors, session, provider } = yield* boot()

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "dynamic MCP tool")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "dynamic MCP tool" }],
          tools: {
            [dynamicMcpToolName]: markDynamicMcpTool(
              tool({
                description: "Private MCP tool",
                inputSchema: z.object({}),
                execute: async () => ({ title: "MCP", output: "ok", metadata: {} }),
              }),
            ),
          },
        })

        expect(dynamicMcpToolObservations).toEqual([
          {
            name: dynamicMcpToolName,
            category: "mcp",
            outcome: "success",
            execution: "local",
            durationMs: expect.any(Number),
          },
        ])
      }),
    { config: cfg },
  ),
)

itLocalSuccess.live("session.processor effect tests do not overwrite a tool committed during interruption", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        localToolObservations.length = 0
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service
        const committed = yield* Deferred.make<void>()
        const releaseNotification = yield* Deferred.make<void>()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "interrupt after tool commit")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const off = yield* events.listen((event) => {
          if (event.type !== MessageV2.Event.PartUpdated.type) return Effect.void
          const data = event.data as typeof MessageV2.Event.PartUpdated.data.Type
          const part = data.part
          if (part.type !== "tool" || part.callID !== "call-1" || part.state.status !== "completed") return Effect.void
          return Deferred.succeed(committed, undefined).pipe(Effect.andThen(Deferred.await(releaseNotification)))
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "interrupt after tool commit" }],
            tools: {},
          })
          .pipe(Effect.forkChild)

        yield* Deferred.await(committed)
        const interrupt = yield* Fiber.interrupt(run).pipe(Effect.forkChild)
        yield* Effect.sleep("10 millis")
        yield* Deferred.succeed(releaseNotification, undefined)
        yield* Fiber.join(interrupt)
        const exit = yield* Fiber.await(run)
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        const call = parts.find((part): part is SessionV1.ToolPart => part.type === "tool")
        expect(Exit.isFailure(exit)).toBe(true)
        expect(call?.state.status).toBe("completed")
        expect(localToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "success" }),
        ])
      }),
    { config: cfg },
  ),
)

itMissingPart.live("session.processor effect tests settle Relay when an active tool part is deleted", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        missingPartGate = defer<void>()
        missingPartObservations.length = 0
        const { processors, session, provider } = yield* boot()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "deleted tool part")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })
        const run = yield* handle
          .process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "deleted tool part" }],
            tools: {},
          })
          .pipe(Effect.forkChild)
        const call = yield* waitFor(
          MessageV2.parts(msg.id).pipe(
            Effect.map((parts) =>
              parts.find(
                (part): part is SessionV1.ToolPart =>
                  part.type === "tool" && part.callID === "call-missing" && part.state.status === "running",
              ),
            ),
          ),
          "timed out waiting for running tool part",
        )

        yield* session.removePart({ sessionID: chat.id, messageID: msg.id, partID: call.id })
        missingPartGate.resolve()
        yield* Fiber.join(run)

        expect(missingPartObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "failed" }),
        ])
      }).pipe(Effect.ensuring(Effect.sync(() => missingPartGate.resolve()))),
    { config: cfg },
  ),
)

itReusedCallID.live("session.processor effect tests observe each reused provider call-id generation", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        localToolObservations.length = 0
        const { processors, session, provider } = yield* boot()
        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "reused tool id")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        yield* handle.process({
          user: {
            id: parent.id,
            sessionID: chat.id,
            role: "user",
            time: parent.time,
            agent: parent.agent,
            model: { providerID: ref.providerID, modelID: ref.modelID },
          } satisfies SessionV1.User,
          sessionID: chat.id,
          model: mdl,
          agent: agent(),
          system: [],
          messages: [{ role: "user", content: "reused tool id" }],
          tools: {},
        })

        expect(localToolObservations).toEqual([
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "success" }),
          expect.objectContaining({ name: "lookup", execution: "local", outcome: "success" }),
        ])
      }),
    { config: cfg },
  ),
)

itFragmentFailure.live("session.processor effect tests retain partial legacy parts without v2 events", () =>
  provideTmpdirInstance(
    (dir) =>
      Effect.gen(function* () {
        const { processors, session, provider } = yield* boot()
        const events = yield* EventV2Bridge.Service

        const chat = yield* session.create({})
        const parent = yield* user(chat.id, "provider failure")
        const msg = yield* assistant(chat.id, parent.id, path.resolve(dir))
        const mdl = yield* provider.getModel(ref.providerID, ref.modelID)
        const seen: string[] = []
        const off = yield* events.listen((event) => {
          seen.push(event.type)
          return Effect.void
        })
        const handle = yield* processors.create({ assistantMessage: msg, sessionID: chat.id, model: mdl })

        expect(
          yield* handle.process({
            user: {
              id: parent.id,
              sessionID: chat.id,
              role: "user",
              time: parent.time,
              agent: parent.agent,
              model: { providerID: ref.providerID, modelID: ref.modelID },
            } satisfies SessionV1.User,
            sessionID: chat.id,
            model: mdl,
            agent: agent(),
            system: [],
            messages: [{ role: "user", content: "provider failure" }],
            tools: {},
          }),
        ).toBe("stop")
        yield* off

        const parts = yield* MessageV2.parts(msg.id)
        expect(parts).toEqual(
          expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "partial" }),
            expect.objectContaining({ type: "reasoning", text: "thinking" }),
          ]),
        )
        expect(seen).toContain(MessageV2.Event.PartUpdated.type)
        expect(seen).toContain(Session.Event.Error.type)
        expect(seen.filter((type) => type.startsWith("session.next."))).toEqual([])
      }),
    { config: cfg },
  ),
)
