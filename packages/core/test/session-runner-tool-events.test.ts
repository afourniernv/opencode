import { expect, test } from "bun:test"
import { Effect, Schema, Stream } from "effect"
import { LLMEvent } from "@opencode-ai/llm"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionV2 } from "@opencode-ai/core/session"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { createLLMEventPublisher } from "@opencode-ai/core/session/runner/publish-llm-event"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"

const sessionID = SessionV2.ID.make("ses_tool_event_test")
const base64 = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAAB"
const capture = (failType?: string) => {
  const published: Array<{ readonly type: string; readonly data: unknown }> = []
  const attempted: string[] = []
  const observed: NemoRelay.ToolCompleted[] = []
  const relay = NemoRelay.makeForTesting(
    {
      MetricKind: { Counter: "counter", Histogram: "histogram" },
      MetricValueType: { U64: "u64", F64: "f64" },
      metric: () => {},
      flushSubscribers: async () => {},
    },
    { toolCompleted: (input) => Effect.sync(() => observed.push(input)) },
  )
  const events = EventV2.Service.of({
    publish: (definition, data) => {
      const type = definition.durable
        ? EventV2.versionedType(definition.type, definition.durable.version)
        : definition.type
      attempted.push(type)
      if (type === failType) return Effect.die(`failed to persist ${type}`)
      return Effect.sync(() => {
        const event = { id: EventV2.ID.create(), type: definition.type, data } as EventV2.Payload<typeof definition>
        published.push({
          type,
          data,
        })
        return event
      })
    },
    subscribe: () => Stream.empty,
    all: () => Stream.empty,
    durable: () => Stream.empty,
    listen: () => Effect.succeed(Effect.void),
    project: () => Effect.void,
    replay: () => Effect.void,
    replayAll: () => Effect.succeed(undefined),
    remove: () => Effect.void,
    claim: () => Effect.void,
  })
  return {
    attempted,
    published,
    observed,
    publisher: createLLMEventPublisher(events, {
      sessionID,
      agent: "build",
      model: {
        id: ModelV2.ID.make("model"),
        providerID: ProviderV2.ID.make("provider"),
      },
      relay,
    }),
  }
}

const call = LLMEvent.toolCall({ id: "call-image", name: "read", input: { path: "pixel.png" } })
const result = LLMEvent.toolResult({
  id: "call-image",
  name: "read",
  result: {
    type: "content",
    value: [
      { type: "text", text: "Image read successfully" },
      { type: "file", uri: `data:image/png;base64,${base64}`, mime: "image/png", name: "pixel.png" },
    ],
  },
  output: {
    structured: { type: "media", mime: "image/png" },
    content: [
      { type: "text", text: "Image read successfully" },
      { type: "file", uri: `data:image/png;base64,${base64}`, mime: "image/png", name: "pixel.png" },
    ],
  },
})

test("local tool success serializes media base64 once and reconstructs from structured content", async () => {
  const { published, publisher } = capture()
  await Effect.runPromise(publisher.publish(call))
  await Effect.runPromise(publisher.publish(result))

  const success = published.find((event) => event.type === "session.next.tool.success.1")
  expect(success).toBeDefined()
  const serialized = JSON.stringify(success)
  expect(serialized.split(base64)).toHaveLength(2)
  expect(success?.data).not.toHaveProperty("result")

  expect(success?.data).toMatchObject({
    content: [
      { type: "text", text: "Image read successfully" },
      { type: "file", uri: `data:image/png;base64,${base64}`, mime: "image/png" },
    ],
  })
})

test("provider-executed success retains its compatibility result", async () => {
  const { observed, published, publisher } = capture()
  await Effect.runPromise(publisher.publish(LLMEvent.toolCall({ ...call, providerExecuted: true })))
  await Effect.runPromise(publisher.publish(LLMEvent.toolResult({ ...result, providerExecuted: true })))
  const success = published.find((event) => event.type === "session.next.tool.success.1")
  expect(success?.data).toHaveProperty("result")
  expect(observed).toEqual([
    {
      name: "read",
      execution: "provider",
      outcome: "success",
    },
  ])
})

test("provider success is reported failed when durable success publication fails", async () => {
  const { observed, publisher } = capture("session.next.tool.success.1")
  await Effect.runPromise(publisher.publish(LLMEvent.toolCall({ ...call, providerExecuted: true })))
  const exit = await Effect.runPromiseExit(
    publisher.publish(LLMEvent.toolResult({ ...result, providerExecuted: true })),
  )

  expect(exit._tag).toBe("Failure")
  expect(observed).toEqual([
    {
      name: "read",
      execution: "provider",
      outcome: "failed",
    },
  ])
})

test("provider-executed failures settle Relay exactly once", async () => {
  const { observed, publisher } = capture()
  const providerCall = LLMEvent.toolCall({
    id: "call-provider-error",
    name: "search",
    input: {},
    providerExecuted: true,
  })
  const providerFailure = LLMEvent.toolResult({
    id: providerCall.id,
    name: providerCall.name,
    result: { type: "error", value: "provider failed" },
    providerExecuted: true,
  })
  await Effect.runPromise(publisher.publish(providerCall))
  await Effect.runPromise(publisher.publish(providerFailure))
  await Effect.runPromise(publisher.publish(providerFailure))

  expect(observed).toEqual([
    {
      name: "search",
      execution: "provider",
      outcome: "failed",
    },
  ])
})

test("unsettled provider tools are cancelled once during interruption cleanup", async () => {
  const { observed, publisher } = capture()
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.toolCall({ id: "call-provider-interrupted", name: "lookup", input: {}, providerExecuted: true }),
    ),
  )
  await Effect.runPromise(publisher.failUnsettledTools("interrupted", true, "cancelled"))
  await Effect.runPromise(publisher.failUnsettledTools("interrupted again", true, "cancelled"))

  expect(observed).toEqual([
    {
      name: "lookup",
      execution: "provider",
      outcome: "cancelled",
    },
  ])
})

test("durable tool failure remains fail-fast while all Relay observations close", async () => {
  const { attempted, observed, publisher } = capture("session.next.tool.failed.1")
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.toolCall({ id: "call-provider-first", name: "lookup", input: {}, providerExecuted: true }),
    ),
  )
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.toolCall({ id: "call-provider-second", name: "search", input: {}, providerExecuted: true }),
    ),
  )

  const exit = await Effect.runPromiseExit(publisher.failUnsettledTools("provider failed", true))

  expect(exit._tag).toBe("Failure")
  expect(attempted.filter((type) => type === "session.next.tool.failed.1")).toHaveLength(1)
  expect(observed).toEqual([
    { name: "lookup", execution: "provider", outcome: "failed" },
    { name: "search", execution: "provider", outcome: "failed" },
  ])
})

test("binary failure emits no success event", async () => {
  const { published, publisher } = capture()
  await Effect.runPromise(publisher.publish(call))
  await Effect.runPromise(
    publisher.publish(
      LLMEvent.toolResult({
        id: call.id,
        name: call.name,
        result: { type: "error", value: "Cannot read binary file" },
      }),
    ),
  )
  expect(published.some((event) => event.type === "session.next.tool.success.1")).toBe(false)
  expect(published.some((event) => event.type === "session.next.tool.failed.1")).toBe(true)
})

test("old success event data containing result still decodes", () => {
  const decoded = Schema.decodeUnknownSync(SessionEvent.Tool.Success.data)({
    sessionID,
    timestamp: Date.now(),
    assistantMessageID: SessionMessage.ID.create(),
    callID: "call-old",
    structured: { type: "media", mime: "image/png" },
    content: [{ type: "file", uri: `data:image/png;base64,${base64}`, mime: "image/png" }],
    result: { type: "content", value: [{ type: "file", uri: `data:image/png;base64,${base64}`, mime: "image/png" }] },
    provider: { executed: false },
  })
  expect(decoded.result).toMatchObject({ type: "content" })
})

test("step finish records settlement without publishing step ended", async () => {
  const { published, publisher } = capture()
  await Effect.runPromise(publisher.publish(LLMEvent.stepStart({ index: 0 })))
  await Effect.runPromise(publisher.publish(LLMEvent.stepFinish({ index: 0, reason: "stop" })))

  expect(published.some((event) => event.type === "session.next.step.ended.2")).toBe(false)
  expect(publisher.stepSettlement()).toMatchObject({ finish: "stop" })
})
