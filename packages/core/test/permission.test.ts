import { describe, expect } from "bun:test"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { AgentV2 } from "@opencode-ai/core/agent"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { PermissionV2 } from "@opencode-ai/core/permission"
import { PermissionTable } from "@opencode-ai/core/permission/sql"
import { PermissionSaved } from "@opencode-ai/core/permission/saved"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { eq } from "drizzle-orm"
import { location } from "./fixture/location"
import { testEffect } from "./lib/effect"

const current = Layer.succeed(
  Location.Service,
  Location.Service.of(location({ directory: AbsolutePath.make("/project") })),
)
const observedPermissionWaits: NemoRelay.PermissionWaitCompleted[] = []
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
      { permissionWaitCompleted: (input) => Effect.sync(() => observedPermissionWaits.push(input)) },
    ),
  ),
)
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([
      Database.node,
      EventV2.node,
      SessionStore.node,
      PermissionSaved.node,
      AgentV2.node,
      PermissionV2.node,
    ]),
    [
      [Location.node, current],
      [NemoRelay.node, relay],
    ],
  ),
)

function setup(rules: PermissionV2.Ruleset = []) {
  return Effect.gen(function* () {
    const { db } = yield* Database.Service
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: SessionV2.ID.make("ses_test"),
        project_id: Project.ID.global,
        slug: "test",
        directory: "/project",
        title: "test",
        version: "test",
        agent: "test",
      })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* setRules(rules)
  })
}

function setRules(rules: PermissionV2.Ruleset) {
  return Effect.gen(function* () {
    const agents = yield* AgentV2.Service
    yield* agents.transform((editor) =>
      editor.update(AgentV2.ID.make("test"), (agent) => {
        agent.permissions = [...rules]
      }),
    )
  })
}

function assertion(input: Partial<PermissionV2.AssertInput> = {}) {
  return {
    id: PermissionV2.ID.create("per_test"),
    sessionID: SessionV2.ID.make("ses_test"),
    action: "read",
    resources: ["src/index.ts"],
    ...input,
  } satisfies PermissionV2.AssertInput
}

function waitForRequest(input: Partial<PermissionV2.AssertInput> = {}) {
  return Effect.gen(function* () {
    const service = yield* PermissionV2.Service
    const events = yield* EventV2.Service
    const requestInput = assertion(input)
    const asked = yield* Deferred.make<PermissionV2.Request>()
    const unsubscribe = yield* events.listen((event) => {
      if (event.type !== PermissionV2.Event.Asked.type) return Effect.void
      const request = event.data as PermissionV2.Request
      return request.id === requestInput.id ? Deferred.succeed(asked, request).pipe(Effect.asVoid) : Effect.void
    })
    yield* Effect.addFinalizer(() => unsubscribe)
    const fiber = yield* service.assert(requestInput).pipe(Effect.forkScoped)
    const request = yield* Deferred.await(asked)
    return { service, fiber, request }
  })
}

describe("PermissionV2", () => {
  it.effect("returns the evaluated effect and only queues prompts", () =>
    Effect.gen(function* () {
      yield* setup([{ action: "read", resource: "*", effect: "allow" }])
      const service = yield* PermissionV2.Service
      expect(yield* service.ask(assertion())).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "allow" })
      expect(yield* service.list()).toEqual([])
      yield* setRules([{ action: "read", resource: "*", effect: "deny" }])
      expect(yield* service.ask(assertion())).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "deny" })
      expect(yield* service.list()).toEqual([])
      yield* setRules([])
      expect(yield* service.ask(assertion())).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "ask" })
      expect(yield* service.get(PermissionV2.ID.create("per_test"))).toBeDefined()
    }),
  )

  it.effect("evaluates against an explicit provider-turn agent", () =>
    Effect.gen(function* () {
      yield* setup([{ action: "read", resource: "*", effect: "allow" }])
      const agents = yield* AgentV2.Service
      yield* agents.transform((editor) =>
        editor.update(AgentV2.ID.make("reviewer"), (agent) => {
          agent.permissions.push({ action: "read", resource: "*", effect: "deny" })
        }),
      )
      const service = yield* PermissionV2.Service

      expect(yield* service.ask(assertion())).toMatchObject({ effect: "allow" })
      expect(yield* service.ask(assertion({ agent: AgentV2.ID.make("reviewer") }))).toMatchObject({ effect: "deny" })
      yield* agents.transform((editor) =>
        editor.update(AgentV2.ID.make("reviewer"), (agent) => {
          agent.permissions = []
        }),
      )
      expect(yield* service.ask(assertion({ agent: AgentV2.ID.make("reviewer") }))).toMatchObject({ effect: "ask" })
      expect(yield* service.get(PermissionV2.ID.create("per_test"))).not.toHaveProperty("agent")
    }),
  )

  it.effect("allows and denies from explicit rules without asking", () =>
    Effect.gen(function* () {
      yield* setup([{ action: "read", resource: "*", effect: "allow" }])
      const service = yield* PermissionV2.Service
      yield* service.assert(assertion())
      yield* setRules([{ action: "read", resource: "*", effect: "deny" }])
      const blocked = yield* service.assert(assertion()).pipe(Effect.flip)
      expect(blocked).toBeInstanceOf(PermissionV2.BlockedError)
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("allows managed output reads without granting external directory access", () =>
    Effect.gen(function* () {
      yield* setup([
        { action: "*", resource: "*", effect: "deny" },
        { action: "read", resource: "*", effect: "allow" },
      ])
      const service = yield* PermissionV2.Service

      expect(yield* service.ask(assertion({ resources: ["tool_123"] }))).toMatchObject({ effect: "allow" })
      expect(
        yield* service.ask(assertion({ action: "external_directory", resources: ["/tmp/tool-output/*"] })),
      ).toMatchObject({ effect: "deny" })
    }),
  )

  it.effect("uses build permissions when the Session agent is omitted", () =>
    Effect.gen(function* () {
      yield* setup()
      const { db } = yield* Database.Service
      yield* db
        .update(SessionTable)
        .set({ agent: null })
        .where(eq(SessionTable.id, SessionV2.ID.make("ses_test")))
        .run()
        .pipe(Effect.orDie)
      const agents = yield* AgentV2.Service
      yield* agents.transform((editor) =>
        editor.update(AgentV2.ID.make("build"), (agent) => {
          agent.permissions = [{ action: "todowrite", resource: "*", effect: "allow" }]
        }),
      )

      const service = yield* PermissionV2.Service
      expect(yield* service.ask(assertion({ action: "todowrite", resources: ["*"] }))).toEqual({
        id: PermissionV2.ID.create("per_test"),
        effect: "allow",
      })
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("denies omitted-agent permissions when no primary default agent exists", () =>
    Effect.gen(function* () {
      yield* setup()
      const { db } = yield* Database.Service
      yield* db
        .update(SessionTable)
        .set({ agent: null })
        .where(eq(SessionTable.id, SessionV2.ID.make("ses_test")))
        .run()
        .pipe(Effect.orDie)
      const agents = yield* AgentV2.Service
      yield* agents.transform((editor) => {
        editor.remove(AgentV2.ID.make("test"))
        editor.remove(AgentV2.ID.make("build"))
      })

      const service = yield* PermissionV2.Service
      expect(yield* service.ask(assertion())).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "deny" })
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("evaluates bash with the normal configured-rule semantics", () =>
    Effect.gen(function* () {
      yield* setup([{ action: "*", resource: "*", effect: "allow" }])
      const service = yield* PermissionV2.Service
      const bash = assertion({ action: "bash", resources: ["pwd"] })
      expect(yield* service.ask(bash)).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "allow" })

      yield* setRules([])
      expect(yield* service.ask(bash)).toEqual({ id: PermissionV2.ID.create("per_test"), effect: "ask" })
      expect(yield* service.get(PermissionV2.ID.create("per_test"))).toBeDefined()
    }),
  )

  it.effect("uses saved bash approvals while preserving configured deny precedence", () =>
    Effect.gen(function* () {
      yield* setup()
      const saved = yield* PermissionSaved.Service
      yield* saved.add({ projectID: Project.ID.global, action: "bash", resources: ["pwd"] })

      const service = yield* PermissionV2.Service
      expect(yield* service.ask(assertion({ action: "bash", resources: ["pwd"] }))).toEqual({
        id: PermissionV2.ID.create("per_test"),
        effect: "allow",
      })
      expect(yield* service.list()).toEqual([])

      yield* setRules([{ action: "bash", resource: "*", effect: "deny" }])
      expect(yield* service.ask(assertion({ action: "bash", resources: ["pwd"] }))).toEqual({
        id: PermissionV2.ID.create("per_test"),
        effect: "deny",
      })
    }),
  )

  it.effect("resolves an asked permission once", () =>
    Effect.gen(function* () {
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest()
      expect(yield* service.list()).toEqual([request])
      expect(yield* service.forSession(request.sessionID)).toEqual([request])
      expect(yield* service.forSession(SessionV2.ID.make("ses_other"))).toEqual([])
      expect(yield* service.get(request.id)).toEqual(request)
      yield* service.reply({ requestID: request.id, reply: "once" })
      yield* Fiber.join(fiber)
      expect(yield* service.list()).toEqual([])
      expect(yield* service.get(request.id)).toBeUndefined()
    }),
  )

  it.effect("allows only one concurrent responder to claim a request", () =>
    Effect.gen(function* () {
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest()
      const events = yield* EventV2.Service
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const replies: PermissionV2.Reply[] = []
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID; reply: PermissionV2.Reply }
        if (data.requestID !== request.id) return Effect.void
        replies.push(data.reply)
        return data.reply === "once"
          ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void
      })
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(Effect.andThen(unsubscribe), Effect.asVoid),
      )

      const winner = yield* service.reply({ requestID: request.id, reply: "once" }).pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(entered)
      expect(yield* service.reply({ requestID: request.id, reply: "reject" }).pipe(Effect.flip)).toEqual(
        new PermissionV2.NotFoundError({ requestID: request.id }),
      )
      yield* Deferred.succeed(release, undefined)

      expect(yield* Fiber.join(winner)).toMatchObject({ _tag: "Success" })
      yield* Fiber.join(fiber)
      expect(replies).toEqual(["once"])
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("commits a claimed permission when reply notification fails", () =>
    Effect.gen(function* () {
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest()
      const events = yield* EventV2.Service
      let failReply = true
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type || !failReply) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID }
        if (data.requestID !== request.id) return Effect.void
        failReply = false
        return Effect.die("injected permission reply listener failure")
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      yield* service.reply({ requestID: request.id, reply: "once" })
      yield* Fiber.join(fiber)
      expect(yield* service.list()).toEqual([])
      expect(yield* service.reply({ requestID: request.id, reply: "once" }).pipe(Effect.flip)).toEqual(
        new PermissionV2.NotFoundError({ requestID: request.id }),
      )
    }),
  )

  it.effect("restores a claimed permission when saving an always reply fails", () =>
    Effect.gen(function* () {
      yield* setup()
      const { db } = yield* Database.Service
      const { service, fiber, request } = yield* waitForRequest({ save: ["src/*"] })
      const events = yield* EventV2.Service
      const replies: PermissionV2.Reply[] = []
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID; reply: PermissionV2.Reply }
        if (data.requestID === request.id) replies.push(data.reply)
        return Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      yield* db.run("ALTER TABLE permission RENAME TO permission_reply_failure")
      const first = yield* service
        .reply({ requestID: request.id, reply: "always" })
        .pipe(
          Effect.exit,
          Effect.ensuring(db.run("ALTER TABLE permission_reply_failure RENAME TO permission").pipe(Effect.ignore)),
        )
      expect(Exit.isFailure(first)).toBe(true)
      expect(yield* service.list()).toEqual([request])
      expect(replies).toEqual([])

      yield* service.reply({ requestID: request.id, reply: "always" })
      yield* Fiber.join(fiber)
      expect(replies).toEqual(["always"])
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("commits a saved always reply when notification fails", () =>
    Effect.gen(function* () {
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest({ save: ["src/*"] })
      const events = yield* EventV2.Service
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID }
        return data.requestID === request.id ? Effect.die("injected always reply listener failure") : Effect.void
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      yield* service.reply({ requestID: request.id, reply: "always" })
      yield* Fiber.join(fiber)

      expect(yield* service.list()).toEqual([])
      expect(
        yield* service.ask(
          assertion({ id: PermissionV2.ID.create("per_saved_after_notification"), resources: ["src/next.ts"] }),
        ),
      ).toMatchObject({ effect: "allow" })
    }),
  )

  it.effect("reports cancellation when a wait is interrupted during reply publication", () =>
    Effect.gen(function* () {
      observedPermissionWaits.length = 0
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest()
      const events = yield* EventV2.Service
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID }
        if (data.requestID !== request.id) return Effect.void
        return Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
      })
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(Effect.andThen(unsubscribe), Effect.asVoid),
      )

      const reply = yield* service.reply({ requestID: request.id, reply: "once" }).pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(entered)
      yield* Fiber.interrupt(fiber)

      expect(observedPermissionWaits.map(({ resolution }) => resolution)).toEqual(["cancelled"])
      yield* Deferred.succeed(release, undefined)
      expect(yield* Fiber.join(reply)).toMatchObject({ _tag: "Success" })
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("reject fan-out skips a sibling claimed by another responder", () =>
    Effect.gen(function* () {
      yield* setup()
      const first = yield* waitForRequest({ id: PermissionV2.ID.create("per_reject_primary") })
      const second = yield* waitForRequest({ id: PermissionV2.ID.create("per_reject_claimed") })
      const events = yield* EventV2.Service
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const secondReplies: PermissionV2.Reply[] = []
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID; reply: PermissionV2.Reply }
        if (data.requestID !== second.request.id) return Effect.void
        secondReplies.push(data.reply)
        return data.reply === "once"
          ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void
      })
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(Effect.andThen(unsubscribe), Effect.asVoid),
      )

      const secondReply = yield* second.service
        .reply({ requestID: second.request.id, reply: "once" })
        .pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(entered)
      yield* first.service.reply({ requestID: first.request.id, reply: "reject" })
      yield* Deferred.succeed(release, undefined)

      expect(yield* Fiber.await(first.fiber)).toMatchObject({ _tag: "Failure" })
      expect(yield* Fiber.join(secondReply)).toMatchObject({ _tag: "Success" })
      yield* Fiber.join(second.fiber)
      expect(secondReplies).toEqual(["once"])
      expect(yield* first.service.list()).toEqual([])
    }),
  )

  it.effect("always fan-out skips an eligible sibling claimed by another responder", () =>
    Effect.gen(function* () {
      yield* setup()
      const first = yield* waitForRequest({
        id: PermissionV2.ID.create("per_always_primary"),
        resources: ["src/a.ts"],
        save: ["src/*"],
      })
      const second = yield* waitForRequest({
        id: PermissionV2.ID.create("per_always_claimed"),
        resources: ["src/b.ts"],
      })
      const events = yield* EventV2.Service
      const entered = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const secondReplies: PermissionV2.Reply[] = []
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID; reply: PermissionV2.Reply }
        if (data.requestID !== second.request.id) return Effect.void
        secondReplies.push(data.reply)
        return data.reply === "once"
          ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
          : Effect.void
      })
      yield* Effect.addFinalizer(() =>
        Deferred.succeed(release, undefined).pipe(Effect.andThen(unsubscribe), Effect.asVoid),
      )

      const secondReply = yield* second.service
        .reply({ requestID: second.request.id, reply: "once" })
        .pipe(Effect.exit, Effect.forkScoped)
      yield* Deferred.await(entered)
      yield* first.service.reply({ requestID: first.request.id, reply: "always" })
      yield* Deferred.succeed(release, undefined)

      yield* Fiber.join(first.fiber)
      expect(yield* Fiber.join(secondReply)).toMatchObject({ _tag: "Success" })
      yield* Fiber.join(second.fiber)
      expect(secondReplies).toEqual(["once"])
      expect(yield* first.service.list()).toEqual([])
    }),
  )

  it.effect("commits synthesized fan-out when reply notification fails", () =>
    Effect.gen(function* () {
      yield* setup()
      const first = yield* waitForRequest({
        id: PermissionV2.ID.create("per_fanout_primary"),
        resources: ["src/a.ts"],
        save: ["src/*"],
      })
      const second = yield* waitForRequest({
        id: PermissionV2.ID.create("per_fanout_retry"),
        resources: ["src/b.ts"],
      })
      const events = yield* EventV2.Service
      let failFanout = true
      const unsubscribe = yield* events.listen((event) => {
        if (event.type !== PermissionV2.Event.Replied.type || !failFanout) return Effect.void
        const data = event.data as { requestID: PermissionV2.ID; reply: PermissionV2.Reply }
        if (data.requestID !== second.request.id || data.reply !== "always") return Effect.void
        failFanout = false
        return Effect.die("injected permission fan-out listener failure")
      })
      yield* Effect.addFinalizer(() => unsubscribe)

      yield* first.service.reply({ requestID: first.request.id, reply: "always" })
      yield* Fiber.join(first.fiber)
      yield* Fiber.join(second.fiber)
      expect(yield* first.service.list()).toEqual([])
    }),
  )

  it.effect("defects when an asked permission is declined", () =>
    Effect.gen(function* () {
      yield* setup()
      const { service, fiber, request } = yield* waitForRequest()
      yield* service.reply({ requestID: request.id, reply: "reject" })
      const exit = yield* Fiber.await(fiber)

      expect(exit._tag).toBe("Failure")
      if (exit._tag === "Failure")
        expect(
          exit.cause.reasons.some(
            (reason) => Cause.isDieReason(reason) && reason.defect instanceof PermissionV2.DeclinedError,
          ),
        ).toBe(true)
      expect(yield* service.list()).toEqual([])
    }),
  )

  it.effect("stores and removes saved resources for a project", () =>
    Effect.gen(function* () {
      yield* setup()
      const service = yield* PermissionV2.Service
      const asked = yield* Deferred.make<PermissionV2.Request>()
      const events = yield* EventV2.Service
      const unsubscribe = yield* events.listen((event) =>
        event.type === PermissionV2.Event.Asked.type
          ? Deferred.succeed(asked, event.data as PermissionV2.Request).pipe(Effect.asVoid)
          : Effect.void,
      )
      yield* Effect.addFinalizer(() => unsubscribe)
      const fiber = yield* service.assert(assertion({ save: ["src/*"] })).pipe(Effect.forkScoped)
      const request = yield* Deferred.await(asked)
      yield* service.reply({ requestID: request.id, reply: "always" })
      yield* Fiber.join(fiber)

      const { db } = yield* Database.Service
      expect(
        yield* db.select().from(PermissionTable).where(eq(PermissionTable.project_id, Project.ID.global)).all(),
      ).toMatchObject([{ action: "read", resource: "src/*" }])
      const saved = yield* PermissionSaved.Service
      const id = (yield* saved.list())[0]!.id
      expect(yield* saved.list()).toEqual([{ id, projectID: Project.ID.global, action: "read", resource: "src/*" }])
      yield* service.assert(assertion({ id: PermissionV2.ID.create("per_next"), resources: ["src/next.ts"] }))
      yield* saved.remove(id)
      expect(yield* saved.list()).toEqual([])
    }),
  )
})
