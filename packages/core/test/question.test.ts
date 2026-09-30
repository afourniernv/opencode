import { describe, expect } from "bun:test"
import { Context, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { EventV2 } from "@opencode-ai/core/event"
import { QuestionV2 } from "@opencode-ai/core/question"
import { SessionV2 } from "@opencode-ai/core/session"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"
import { testEffect } from "./lib/effect"

const questions = AppNodeBuilder.build(LayerNode.group([EventV2.node, QuestionV2.node]))
const it = testEffect(questions)

const observedQuestionWaits: NemoRelay.QuestionWaitCompleted[] = []
const observedQuestions = AppNodeBuilder.build(LayerNode.group([EventV2.node, QuestionV2.node]), [
  [
    NemoRelay.node,
    Layer.succeed(
      NemoRelay.Service,
      NemoRelay.Service.of(
        NemoRelay.makeForTesting(
          {
            MetricKind: { Counter: "counter", Histogram: "histogram" },
            MetricValueType: { U64: "u64", F64: "f64" },
            metric() {},
            flushSubscribers: async () => {},
          },
          { questionWaitCompleted: (input) => Effect.sync(() => observedQuestionWaits.push(input)) },
        ),
      ),
    ),
  ],
])
const observedIt = testEffect(observedQuestions)

const sessionID = SessionV2.ID.make("ses_question_test")
const question: QuestionV2.Info = {
  question: "Which option?",
  header: "Option",
  options: [{ label: "One", description: "First option" }],
}

const waitForAsk = Effect.fn("QuestionV2Test.waitForAsk")(function* (
  service: QuestionV2.Interface,
  input: QuestionV2.AskInput,
) {
  const events = yield* EventV2.Service
  const asked = yield* Deferred.make<QuestionV2.Request>()
  const unsubscribe = yield* events.listen((event) =>
    event.type === QuestionV2.Event.Asked.type
      ? Deferred.succeed(asked, event.data as QuestionV2.Request).pipe(Effect.asVoid)
      : Effect.void,
  )
  yield* Effect.addFinalizer(() => unsubscribe)
  const fiber = yield* service.ask(input).pipe(Effect.forkScoped)
  return { fiber, request: yield* Deferred.await(asked) }
})

describe("QuestionV2", () => {
  observedIt.effect("reports answered, rejected, and interrupted waits without question contents", () =>
    Effect.gen(function* () {
      observedQuestionWaits.length = 0
      const service = yield* QuestionV2.Service

      const answered = yield* waitForAsk(service, { sessionID, questions: [question] })
      yield* service.reply({ requestID: answered.request.id, answers: [["One"]] })
      yield* Fiber.join(answered.fiber)

      const rejected = yield* waitForAsk(service, { sessionID, questions: [question] })
      yield* service.reject(rejected.request.id)
      yield* Fiber.await(rejected.fiber)

      const cancelled = yield* waitForAsk(service, { sessionID, questions: [question] })
      yield* Fiber.interrupt(cancelled.fiber)

      expect(observedQuestionWaits.map(({ runtime, resolution }) => ({ runtime, resolution }))).toEqual([
        { runtime: "v2", resolution: "answered" },
        { runtime: "v2", resolution: "rejected" },
        { runtime: "v2", resolution: "cancelled" },
      ])
      expect(JSON.stringify(observedQuestionWaits)).not.toContain(question.question)
      expect(JSON.stringify(observedQuestionWaits)).not.toContain("One")
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("publishes lifecycle events and settles a pending reply", () =>
    Effect.gen(function* () {
      const service = yield* QuestionV2.Service
      const events = yield* EventV2.Service
      const published: EventV2.Payload[] = []
      const unsubscribe = yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type.startsWith("question.v2.")) published.push(event)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const { fiber, request } = yield* waitForAsk(service, { sessionID, questions: [question] })

      expect(request.id).toMatch(/^que_/)
      expect(yield* service.list()).toEqual([request])
      yield* service.reply({ requestID: request.id, answers: [["One"]] })

      expect(yield* Fiber.join(fiber)).toEqual([["One"]])
      expect(yield* service.list()).toEqual([])
      expect(published.map((event) => [event.type, event.data])).toEqual([
        [QuestionV2.Event.Asked.type, request],
        [QuestionV2.Event.Replied.type, { sessionID, requestID: request.id, answers: [["One"]] }],
      ])
    }),
  )

  it.effect("publishes rejection, fails the ask, and rejects unknown IDs", () =>
    Effect.gen(function* () {
      const service = yield* QuestionV2.Service
      const events = yield* EventV2.Service
      const published: EventV2.Payload[] = []
      const unsubscribe = yield* events.listen((event) =>
        Effect.sync(() => {
          if (event.type === QuestionV2.Event.Rejected.type) published.push(event)
        }),
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const { fiber, request } = yield* waitForAsk(service, { sessionID, questions: [question] })

      yield* service.reject(request.id)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(exit.cause.toString()).toContain("QuestionV2.RejectedError")
      expect(published.map((event) => event.data)).toEqual([{ sessionID, requestID: request.id }])

      const unknown = QuestionV2.ID.ascending("que_unknown")
      expect(yield* service.reply({ requestID: unknown, answers: [] }).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: unknown }),
      )
      expect(yield* service.reject(unknown).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: unknown }),
      )
    }),
  )

  observedIt.effect("reports the winner of a concurrent reply and reject exactly once", () =>
    Effect.gen(function* () {
      observedQuestionWaits.length = 0
      const service = yield* QuestionV2.Service
      const pending = yield* waitForAsk(service, { sessionID, questions: [question] })

      const [replyExit, rejectExit] = yield* Effect.all(
        [
          service.reply({ requestID: pending.request.id, answers: [["One"]] }).pipe(Effect.exit),
          service.reject(pending.request.id).pipe(Effect.exit),
        ],
        { concurrency: "unbounded" },
      )
      const askExit = yield* Fiber.await(pending.fiber)

      expect([replyExit, rejectExit].filter(Exit.isSuccess)).toHaveLength(1)
      if (Exit.isSuccess(replyExit)) {
        expect(Exit.isSuccess(askExit)).toBe(true)
        expect(observedQuestionWaits).toEqual([expect.objectContaining({ runtime: "v2", resolution: "answered" })])
      } else {
        expect(Exit.isFailure(askExit)).toBe(true)
        expect(observedQuestionWaits).toEqual([expect.objectContaining({ runtime: "v2", resolution: "rejected" })])
      }
      expect(yield* service.list()).toEqual([])
    }),
  )

  observedIt.effect("restores a claimed question when reply publication fails", () =>
    Effect.gen(function* () {
      observedQuestionWaits.length = 0
      const service = yield* QuestionV2.Service
      const events = yield* EventV2.Service
      let failReply = true
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== QuestionV2.Event.Replied.type || !failReply) return Effect.void
        failReply = false
        return Effect.die("injected reply listener failure")
      })
      yield* Effect.addFinalizer(() => unsubscribe)
      const pending = yield* waitForAsk(service, { sessionID, questions: [question] })

      const first = yield* service.reply({ requestID: pending.request.id, answers: [["One"]] }).pipe(Effect.exit)
      expect(Exit.isFailure(first)).toBe(true)
      expect(yield* service.list()).toEqual([pending.request])
      expect(observedQuestionWaits).toEqual([])

      yield* service.reply({ requestID: pending.request.id, answers: [["One"]] })
      expect(yield* Fiber.join(pending.fiber)).toEqual([["One"]])
      expect(observedQuestionWaits).toEqual([expect.objectContaining({ runtime: "v2", resolution: "answered" })])
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("isolates pending requests by location-layer instance and rejects them on finalization", () =>
    Effect.gen(function* () {
      observedQuestionWaits.length = 0
      const firstScope = yield* Scope.make()
      const secondScope = yield* Scope.make()
      const first = Context.get(
        yield* Layer.buildWithScope(Layer.fresh(observedQuestions), firstScope),
        QuestionV2.Service,
      )
      const second = Context.get(
        yield* Layer.buildWithScope(Layer.fresh(observedQuestions), secondScope),
        QuestionV2.Service,
      )
      const fiber = yield* first.ask({ sessionID, questions: [question] }).pipe(Effect.forkScoped)
      yield* Effect.yieldNow
      const request = (yield* first.list())[0]!

      expect(yield* second.list()).toEqual([])
      expect(yield* second.reply({ requestID: request.id, answers: [["One"]] }).pipe(Effect.flip)).toEqual(
        new QuestionV2.NotFoundError({ requestID: request.id }),
      )

      yield* Scope.close(firstScope, Exit.void)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit)) expect(exit.cause.toString()).toContain("QuestionV2.RejectedError")
      expect(observedQuestionWaits).toEqual([expect.objectContaining({ runtime: "v2", resolution: "cancelled" })])
      yield* Scope.close(secondScope, Exit.void)
    }),
  )
})
