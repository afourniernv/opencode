import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { PermissionV1 } from "@opencode-ai/core/v1/permission"
import { Image } from "@/image/image"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Context, Option, Scope, Schema } from "effect"
import * as Stream from "effect/Stream"
import { Agent } from "@/agent/agent"
import { Config } from "@/config/config"
import { Permission } from "@/permission"
import { Plugin } from "@/plugin"
import { Snapshot } from "@/snapshot"
import { Session } from "./session"
import { LLM } from "./llm"
import { MessageV2 } from "./message-v2"
import { isOverflow } from "./overflow"
import { PartID } from "./schema"
import type { SessionID } from "./schema"
import { SessionRetry } from "./retry"
import { SessionStatus } from "./status"
import { SessionSummary } from "./summary"
import type { Provider } from "@/provider/provider"
import { Question } from "@/question"
import { errorMessage } from "@/util/error"
import { isRecord } from "@/util/record"
import { EventV2Bridge } from "@/event-v2-bridge"
import { Database } from "@opencode-ai/core/database/database"
import { Usage, type LLMEvent } from "@opencode-ai/llm"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"
import { KeyedMutex } from "@opencode-ai/core/effect/keyed-mutex"
import { EffectBridge } from "@/effect/bridge"
import { toolSemanticCategory } from "./tool-semantics"

const DOOM_LOOP_THRESHOLD = 3
export type Result = "compact" | "stop" | "continue"

export interface Handle {
  readonly message: SessionV1.Assistant
  readonly executeTool: <A, E>(
    input: {
      readonly toolCallID: string
      readonly name: string
      readonly input: Record<string, unknown>
    },
    effect: Effect.Effect<A, E>,
  ) => Promise<A>
  readonly updateToolCall: (
    toolCallID: string,
    update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
  ) => Effect.Effect<SessionV1.ToolPart | undefined>
  readonly completeToolCall: (
    toolCallID: string,
    output: {
      title: string
      metadata: Record<string, any>
      output: string
      attachments?: SessionV1.FilePart[]
    },
  ) => Effect.Effect<void>
  readonly process: (streamInput: LLM.StreamInput) => Effect.Effect<Result>
}

type Input = {
  assistantMessage: SessionV1.Assistant
  sessionID: SessionID
  model: Provider.Model
}

export interface Interface {
  readonly create: (input: Input) => Effect.Effect<Handle>
}

type ToolCall = {
  partID: SessionV1.ToolPart["id"]
  messageID: SessionV1.ToolPart["messageID"]
  sessionID: SessionV1.ToolPart["sessionID"]
  done: Deferred.Deferred<void>
  relay?: NemoRelay.ToolObservation
  callbackClaimed: boolean
}

type ToolRuntime = {
  readonly bridge: EffectBridge.Shape
  readonly fibers: Set<Fiber.Fiber<any, any>>
  accepting: boolean
}

interface ProcessorContext extends Input {
  toolcalls: Map<string, ToolCall>
  shouldBreak: boolean
  snapshot: string | undefined
  blocked: boolean
  needsCompaction: boolean
  currentText: SessionV1.TextPart | undefined
  reasoningMap: Record<string, SessionV1.ReasoningPart>
}

type StreamEvent = LLMEvent

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionProcessor") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const session = yield* Session.Service
    const config = yield* Config.Service
    const snapshot = yield* Snapshot.Service
    const agents = yield* Agent.Service
    const llm = yield* LLM.Service
    const permission = yield* Permission.Service
    const plugin = yield* Plugin.Service
    const summary = yield* SessionSummary.Service
    const scope = yield* Scope.Scope
    const status = yield* SessionStatus.Service
    const image = yield* Image.Service
    const events = yield* EventV2Bridge.Service
    const database = yield* Database.Service
    const relay = yield* NemoRelay.Service

    const callRole = (agent: string): NemoRelay.CallRole => {
      if (agent === "compaction") return "compaction"
      if (agent === "title") return "title"
      if (agent === "summary") return "summary"
      return "primary"
    }

    const create = Effect.fn("SessionProcessor.create")(function* (input: Input) {
      // Pre-capture snapshot before the LLM stream starts. The AI SDK
      // may execute tools internally before emitting start-step events,
      // so capturing inside the event handler can be too late.
      const initialSnapshot = yield* snapshot.track()
      const ctx: ProcessorContext = {
        assistantMessage: input.assistantMessage,
        sessionID: input.sessionID,
        model: input.model,
        toolcalls: new Map(),
        shouldBreak: false,
        snapshot: initialSnapshot,
        blocked: false,
        needsCompaction: false,
        currentText: undefined,
        reasoningMap: {},
      }
      const fallbackBridge = yield* EffectBridge.make()
      const toolCallLocks = KeyedMutex.makeUnsafe<string>()
      let aborted = false
      let activeToolCategories = new Map<string, NemoRelay.ToolCategory>()
      let activeToolRuntime: ToolRuntime | undefined

      const parse = (e: unknown) =>
        MessageV2.fromError(e, {
          providerID: input.model.providerID,
          aborted,
        })

      const blockedError = (error: unknown) =>
        error instanceof PermissionV1.DeniedError ||
        error instanceof PermissionV1.RejectedError ||
        error instanceof PermissionV1.CorrectedError ||
        error instanceof Question.RejectedError

      // Preserve the host's pre-observability control flow. Configured denials
      // and correction feedback are model-facing tool errors; only an explicit
      // user rejection or dismissed question stops the loop.
      const stopsToolLoop = (error: unknown) =>
        error instanceof PermissionV1.RejectedError || error instanceof Question.RejectedError

      const settleToolCall = Effect.fn("SessionProcessor.settleToolCall")(function* (
        toolCallID: string,
        call: ToolCall,
      ) {
        if (ctx.toolcalls.get(toolCallID) === call) ctx.toolcalls.delete(toolCallID)
        yield* Deferred.succeed(call.done, undefined).pipe(Effect.ignore)
      })

      // Every caller holds toolCallLocks for toolCallID. Keeping the durable
      // part and its Relay observation behind the same lock prevents a stale
      // metadata write or cleanup path from stealing terminal ownership.
      const readToolCall = Effect.fn("SessionProcessor.readToolCall")(function* (toolCallID: string) {
        const call = ctx.toolcalls.get(toolCallID)
        if (!call) return undefined
        const part = yield* session.getPart({
          partID: call.partID,
          messageID: call.messageID,
          sessionID: call.sessionID,
        })
        if (!part || part.type !== "tool") {
          yield* (call.relay?.complete("failed") ?? Effect.void).pipe(
            Effect.catchCause(() => Effect.void),
            Effect.ensuring(settleToolCall(toolCallID, call)),
          )
          return undefined
        }
        return { call, part }
      })

      const updateToolCallUnlocked = Effect.fn("SessionProcessor.updateToolCallUnlocked")(function* (
        toolCallID: string,
        update: (part: SessionV1.ToolPart) => SessionV1.ToolPart,
      ) {
        const match = yield* readToolCall(toolCallID)
        if (!match) return undefined
        const part = yield* session.updatePart(update(match.part))
        match.call.partID = part.id
        match.call.messageID = part.messageID
        match.call.sessionID = part.sessionID
        return part
      })

      const admitToolCall = <A, E, R>(toolCallID: string, effect: Effect.Effect<A, E, R>) =>
        toolCallLocks.withLock(toolCallID)(effect)

      const updateToolCall = Effect.fn("SessionProcessor.updateToolCall")(
        (toolCallID: string, update: (part: SessionV1.ToolPart) => SessionV1.ToolPart) =>
          admitToolCall(toolCallID, updateToolCallUnlocked(toolCallID, update)),
      )

      const completeToolCall = Effect.fn("SessionProcessor.completeToolCall")(function* (
        toolCallID: string,
        output: {
          title: string
          metadata: Record<string, any>
          output: string
          attachments?: SessionV1.FilePart[]
        },
      ) {
        yield* admitToolCall(
          toolCallID,
          Effect.uninterruptible(
            Effect.gen(function* () {
              const match = yield* readToolCall(toolCallID)
              if (!match) return
              const state = match.part.state
              if (state.status !== "running") return
              const end = Date.now()
              const updated = yield* session
                .updatePart({
                  ...match.part,
                  state: {
                    status: "completed",
                    input: state.input,
                    output: output.output,
                    metadata: output.metadata,
                    title: output.title,
                    time: { start: state.time.start, end },
                    attachments: output.attachments,
                  },
                })
                .pipe(Effect.exit)
              if (Exit.isFailure(updated)) yield* Effect.failCause(updated.cause)
              const terminalResult =
                match.part.metadata?.providerExecuted !== true && NemoRelay.toolCategory(match.part.tool) === "terminal"
                  ? NemoRelay.terminalResultFamily(output.metadata)
                  : undefined
              const relayCompletion = match.call.relay?.complete(
                "success",
                terminalResult === undefined ? undefined : { terminalResult },
              )
              if (relayCompletion) yield* relayCompletion.pipe(Effect.catchCause(() => Effect.void))
              yield* settleToolCall(toolCallID, match.call)
            }),
          ),
        )
      })

      const failToolCall = Effect.fn("SessionProcessor.failToolCall")(function* (toolCallID: string, error: unknown) {
        return yield* admitToolCall(
          toolCallID,
          Effect.uninterruptible(
            Effect.gen(function* () {
              const match = yield* readToolCall(toolCallID)
              if (!match) return false
              const state = match.part.state
              if (state.status !== "running") return false
              const end = Date.now()
              const blocked = blockedError(error)
              const cancelled =
                !blocked &&
                typeof error === "object" &&
                error !== null &&
                "name" in error &&
                error.name === "AbortError"
              const outcome: NemoRelay.ToolOutcome = blocked ? "blocked" : cancelled ? "cancelled" : "failed"
              const updated = yield* session
                .updatePart({
                  ...match.part,
                  state: {
                    status: "error",
                    input: state.input,
                    error: errorMessage(error),
                    // Keep metadata streamed while running so failures retain progress detail (e.g. execute's child calls).
                    metadata: state.metadata,
                    time: { start: state.time.start, end },
                  },
                })
                .pipe(Effect.exit)
              if (Exit.isFailure(updated)) return yield* Effect.failCause(updated.cause)
              const relayCompletion = match.call.relay?.complete(outcome)
              if (relayCompletion) yield* relayCompletion.pipe(Effect.catchCause(() => Effect.void))
              yield* settleToolCall(toolCallID, match.call)
              if (stopsToolLoop(error)) {
                ctx.blocked = ctx.shouldBreak
              }
              return true
            }),
          ),
        )
      })

      const finishReasoning = Effect.fn("SessionProcessor.finishReasoning")(function* (reasoningID: string) {
        if (!(reasoningID in ctx.reasoningMap)) return
        // oxlint-disable-next-line no-self-assign -- reactivity trigger
        ctx.reasoningMap[reasoningID].text = ctx.reasoningMap[reasoningID].text
        ctx.reasoningMap[reasoningID].time = { ...ctx.reasoningMap[reasoningID].time, end: Date.now() }
        yield* session.updatePart(ctx.reasoningMap[reasoningID])
        delete ctx.reasoningMap[reasoningID]
      })

      const settleOutstandingToolCalls = Effect.fn("SessionProcessor.settleOutstandingToolCalls")(function* (
        unsettledOutcome: NemoRelay.ToolOutcome,
      ) {
        const calls = [...ctx.toolcalls.entries()]
        const failures = yield* Effect.forEach(calls, ([toolCallID, call]) =>
          admitToolCall(
            toolCallID,
            Effect.gen(function* () {
              if (ctx.toolcalls.get(toolCallID) !== call) return undefined
              let outcome: NemoRelay.ToolOutcome = "failed"
              let durableFailure: Cause.Cause<never> | undefined
              const loaded = yield* session
                .getPart({
                  partID: call.partID,
                  messageID: call.messageID,
                  sessionID: call.sessionID,
                })
                .pipe(Effect.exit)
              if (Exit.isFailure(loaded)) durableFailure = loaded.cause
              else if (
                loaded.value?.type === "tool" &&
                (loaded.value.state.status === "pending" || loaded.value.state.status === "running")
              ) {
                const part = loaded.value
                const end = Date.now()
                const metadata = "metadata" in part.state && isRecord(part.state.metadata) ? part.state.metadata : {}
                const updated = yield* session
                  .updatePart({
                    ...part,
                    state: {
                      ...part.state,
                      status: "error",
                      error: "Tool execution aborted",
                      metadata: { ...metadata, interrupted: true },
                      time: { start: "time" in part.state ? part.state.time.start : end, end },
                    },
                  })
                  .pipe(Effect.exit)
                if (Exit.isFailure(updated)) durableFailure = updated.cause
                else outcome = unsettledOutcome
              }

              const completion = call.relay?.complete(outcome)
              if (completion) {
                // Relay is observation-only. A failed completion for one call
                // must not prevent siblings or durable cleanup from settling.
                yield* completion.pipe(Effect.catchCause(() => Effect.void))
              }
              return durableFailure
            }).pipe(Effect.ensuring(settleToolCall(toolCallID, call))),
          ),
        )
        const failure = failures.find((cause) => cause !== undefined)
        if (failure) yield* Effect.failCause(failure)
      })

      const ensureToolCall = Effect.fn("SessionProcessor.ensureToolCall")(function* (input: {
        id: string
        name: string
        providerExecuted?: boolean
      }) {
        const existing = yield* readToolCall(input.id)
        if (existing) {
          if (!input.providerExecuted || existing.part.metadata?.providerExecuted) return existing
          const part = yield* session.updatePart({
            ...existing.part,
            metadata: { ...existing.part.metadata, providerExecuted: true },
          })
          existing.call.partID = part.id
          existing.call.messageID = part.messageID
          existing.call.sessionID = part.sessionID
          return { call: existing.call, part }
        }
        const part = yield* session.updatePart({
          id: PartID.ascending(),
          messageID: ctx.assistantMessage.id,
          sessionID: ctx.assistantMessage.sessionID,
          type: "tool",
          tool: input.name,
          callID: input.id,
          state: { status: "pending", input: {}, raw: "" },
          metadata: input.providerExecuted ? { providerExecuted: true } : undefined,
        } satisfies SessionV1.ToolPart)
        const call: ToolCall = {
          done: yield* Deferred.make<void>(),
          partID: part.id,
          messageID: part.messageID,
          sessionID: part.sessionID,
          callbackClaimed: false,
        }
        ctx.toolcalls.set(input.id, call)
        return { call, part }
      })

      const startToolCall = Effect.fn("SessionProcessor.startToolCall")(function* (input: {
        id: string
        name: string
        value: Record<string, unknown>
        providerExecuted?: boolean
        providerMetadata?: SessionV1.ToolPart["metadata"]
        claimCallback?: boolean
      }) {
        return yield* admitToolCall(
          input.id,
          Effect.gen(function* () {
            const admitted = yield* ensureToolCall({
              id: input.id,
              name: input.name,
              providerExecuted: input.providerExecuted,
            })
            if (input.claimCallback) {
              if (admitted.call.callbackClaimed) throw new Error("Duplicate concurrent tool callback")
              admitted.call.callbackClaimed = true
            }
            return yield* Effect.gen(function* () {
              const running = yield* updateToolCallUnlocked(input.id, (match) => ({
                ...match,
                tool: input.name,
                state:
                  match.state.status === "running"
                    ? { ...match.state, input: input.value }
                    : {
                        status: "running",
                        input: input.value,
                        time: { start: Date.now() },
                      },
                metadata: input.providerExecuted
                  ? { ...input.providerMetadata, providerExecuted: true }
                  : (input.providerMetadata ?? match.metadata),
              }))
              const observed = ctx.toolcalls.get(input.id)
              if (running?.state.status === "running" && observed === admitted.call && !observed.relay) {
                observed.relay = yield* relay.beginTool({
                  name: input.name,
                  category: activeToolCategories.get(input.name),
                  execution: running.metadata?.providerExecuted === true ? "provider" : "local",
                })
              }
              return observed?.relay
            }).pipe(
              Effect.onExit((exit) =>
                input.claimCallback && Exit.isFailure(exit)
                  ? Effect.sync(() => {
                      if (ctx.toolcalls.get(input.id) === admitted.call) admitted.call.callbackClaimed = false
                    })
                  : Effect.void,
              ),
            )
          }),
        )
      })

      const executeTool: Handle["executeTool"] = (input, effect) => {
        const runtime = activeToolRuntime
        // A callback outside the owning Turn still executes for host parity,
        // but Relay context is stripped so it cannot attach work to a closed
        // Run or Turn captured when the processor handle was created.
        if (!runtime?.accepting) return fallbackBridge.promise(relay.detached(effect))
        const category = activeToolCategories.get(input.name) ?? NemoRelay.toolCategory(input.name)
        const task = Effect.gen(function* () {
          const observation = yield* startToolCall({
            id: input.toolCallID,
            name: input.name,
            value: input.input,
            claimCallback: true,
          })
          const observed = observation ? observation.run(effect) : effect
          return yield* observed.pipe(
            Effect.onExit((exit) => {
              if (!observation) return Effect.void
              const outcome: NemoRelay.ToolOutcome = Exit.isSuccess(exit)
                ? "success"
                : Cause.hasInterruptsOnly(exit.cause)
                  ? "cancelled"
                  : blockedError(Cause.squash(exit.cause))
                    ? "blocked"
                    : "failed"
              const terminalResult =
                Exit.isSuccess(exit) && category === "terminal" && isRecord(exit.value)
                  ? NemoRelay.terminalResultFamily(exit.value.metadata)
                  : undefined
              return observation
                .complete(outcome, terminalResult === undefined ? undefined : { terminalResult })
                .pipe(Effect.catchCause(() => Effect.void))
            }),
          )
        })
        const fiber = runtime.bridge.fork(task)
        runtime.fibers.add(fiber)
        return runtime.bridge.promise(Fiber.join(fiber)).finally(() => runtime.fibers.delete(fiber))
      }

      const closeToolRuntime = Effect.fn("SessionProcessor.closeToolRuntime")(function* (runtime: ToolRuntime) {
        runtime.accepting = false
        if (activeToolRuntime === runtime) activeToolRuntime = undefined

        // Admission is closed before this stable snapshot. Give callbacks a
        // short grace period, then interrupt the owned fibers and wait for
        // their Tool finalizers before the Turn and its remaining calls close.
        const fibers = [...runtime.fibers]
        if (fibers.length === 0) return
        const drained = yield* Effect.forEach(fibers, Fiber.await, { concurrency: "unbounded" }).pipe(
          Effect.timeoutOption("250 millis"),
        )
        if (Option.isSome(drained)) return
        for (const fiber of fibers) fiber.interruptUnsafe()
        const interrupted = yield* Effect.forEach(fibers, Fiber.await, { concurrency: "unbounded" }).pipe(
          Effect.timeoutOption("250 millis"),
        )
        if (Option.isNone(interrupted))
          yield* Effect.logWarning("tool callbacks did not stop before Turn cleanup", { count: fibers.length })
      })

      const isFilePart = (value: unknown): value is SessionV1.FilePart => Schema.is(SessionV1.FilePart)(value)

      const toolResultOutput = (
        value: Extract<StreamEvent, { type: "tool-result" }>,
      ): { title: string; metadata: Record<string, any>; output: string; attachments?: SessionV1.FilePart[] } => {
        if (isRecord(value.result.value) && typeof value.result.value.output === "string") {
          return {
            title: typeof value.result.value.title === "string" ? value.result.value.title : value.name,
            metadata: isRecord(value.result.value.metadata) ? value.result.value.metadata : {},
            output: value.result.value.output,
            attachments: Array.isArray(value.result.value.attachments)
              ? value.result.value.attachments.filter(isFilePart)
              : undefined,
          }
        }
        return {
          title: value.name,
          metadata: value.result.type === "json" && isRecord(value.result.value) ? value.result.value : {},
          output:
            typeof value.result.value === "string" ? value.result.value : (JSON.stringify(value.result.value) ?? ""),
        }
      }

      const handleEvent = Effect.fnUntraced(function* (value: StreamEvent) {
        switch (value.type) {
          case "reasoning-start":
            if (value.id in ctx.reasoningMap) return
            ctx.reasoningMap[value.id] = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "reasoning",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.reasoningMap[value.id])
            return

          case "reasoning-delta":
            // Match dev: silently drop orphan deltas (no preceding reasoning-start).
            if (!(value.id in ctx.reasoningMap)) return
            ctx.reasoningMap[value.id].text += value.text
            if (value.providerMetadata) ctx.reasoningMap[value.id].metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.reasoningMap[value.id].sessionID,
              messageID: ctx.reasoningMap[value.id].messageID,
              partID: ctx.reasoningMap[value.id].id,
              field: "text",
              delta: value.text,
            })
            return

          case "reasoning-end":
            if (value.providerMetadata && value.id in ctx.reasoningMap) {
              ctx.reasoningMap[value.id].metadata = value.providerMetadata
            }
            yield* finishReasoning(value.id)
            return

          case "tool-input-start":
            if (ctx.assistantMessage.summary) {
              throw new Error(`Tool call not allowed while generating summary: ${value.name}`)
            }
            yield* admitToolCall(value.id, ensureToolCall(value))
            return

          case "tool-input-delta":
            yield* admitToolCall(value.id, ensureToolCall(value))
            return

          case "tool-input-end": {
            yield* admitToolCall(value.id, ensureToolCall(value))
            return
          }

          case "tool-call": {
            if (ctx.assistantMessage.summary) {
              throw new Error(`Tool call not allowed while generating summary: ${value.name}`)
            }
            const input = isRecord(value.input) ? value.input : { value: value.input }
            yield* Effect.uninterruptible(
              startToolCall({
                id: value.id,
                name: value.name,
                value: input,
                providerExecuted: value.providerExecuted,
                providerMetadata: value.providerMetadata,
              }),
            )

            const parts = yield* MessageV2.parts(ctx.assistantMessage.id).pipe(
              Effect.provideService(Database.Service, database),
            )
            const recentParts = parts.slice(-DOOM_LOOP_THRESHOLD)

            if (
              recentParts.length !== DOOM_LOOP_THRESHOLD ||
              !recentParts.every(
                (part) =>
                  part.type === "tool" &&
                  part.tool === value.name &&
                  part.state.status !== "pending" &&
                  JSON.stringify(part.state.input) === JSON.stringify(input),
              )
            ) {
              return
            }

            const agent = yield* agents.get(ctx.assistantMessage.agent)
            yield* permission.ask({
              permission: "doom_loop",
              patterns: [value.name],
              sessionID: ctx.assistantMessage.sessionID,
              metadata: { tool: value.name, input },
              always: [value.name],
              ruleset: agent.permission,
            })
            return
          }

          case "tool-result": {
            if (value.result.type === "error") {
              yield* failToolCall(value.id, value.result.value)
              return
            }
            const rawOutput = toolResultOutput(value)
            const normalized = yield* Effect.forEach(rawOutput.attachments ?? [], (attachment) =>
              attachment.mime.startsWith("image/")
                ? image.normalize(attachment).pipe(
                    Effect.catchIf(
                      (error) => error instanceof Image.ResizerUnavailableError,
                      () => Effect.succeed(attachment),
                    ),
                    Effect.exit,
                  )
                : Effect.succeed(Exit.succeed<SessionV1.FilePart>(attachment)),
            )
            const omitted = normalized.filter(Exit.isFailure).length
            const attachments = normalized.filter(Exit.isSuccess).map((item) => item.value)
            const output = {
              ...rawOutput,
              output:
                omitted === 0
                  ? rawOutput.output
                  : `${rawOutput.output}\n\n[${omitted} image${omitted === 1 ? "" : "s"} omitted: could not be resized below the image size limit.]`,
              attachments: attachments.length ? attachments : undefined,
            }
            yield* completeToolCall(value.id, output)
            return
          }

          case "tool-error": {
            yield* failToolCall(value.id, value.error ?? new Error(value.message))
            return
          }

          case "provider-error":
            throw new Error(value.message)

          case "step-start":
            if (!ctx.snapshot) ctx.snapshot = yield* snapshot.track()
            yield* session.updatePart({
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.sessionID,
              snapshot: ctx.snapshot,
              type: "step-start",
            })
            return

          case "step-finish": {
            const completedSnapshot = yield* snapshot.track()
            yield* Effect.forEach(Object.keys(ctx.reasoningMap), finishReasoning)
            // Anthropic reports thinking blocks it removed before the model saw the
            // prompt. Prefix mismatches mean opencode changed history behind a signed
            // block; log them so the churn can be tracked down.
            const dropped = isRecord(value.providerMetadata?.anthropic)
              ? value.providerMetadata.anthropic.inputTransformations
              : undefined
            if (Array.isArray(dropped) && dropped.length > 0) {
              yield* Effect.logWarning("thinking blocks dropped by provider", {
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.id,
                model: ctx.model.id,
                transformations: JSON.stringify(dropped),
              })
            }
            const usage = Session.getUsage({
              model: ctx.model,
              usage: value.usage ?? new Usage({}),
              metadata: value.providerMetadata,
            })
            if (usage.costSource)
              yield* relay.llmCostRecorded({
                role: callRole(ctx.assistantMessage.agent),
                agentRuntime: "v1",
                provider: ctx.model.providerID,
                model: ctx.model.id,
                costUsd: usage.cost,
                source: usage.costSource,
              })
            ctx.assistantMessage.finish = value.reason
            ctx.assistantMessage.cost += usage.cost
            ctx.assistantMessage.tokens = usage.tokens
            yield* session.updatePart({
              id: PartID.ascending(),
              reason: value.reason,
              snapshot: completedSnapshot,
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "step-finish",
              tokens: usage.tokens,
              cost: usage.cost,
            })
            yield* session.updateMessage(ctx.assistantMessage)
            if (ctx.snapshot) {
              const patch = yield* snapshot.patch(ctx.snapshot)
              if (patch.files.length) {
                yield* session.updatePart({
                  id: PartID.ascending(),
                  messageID: ctx.assistantMessage.id,
                  sessionID: ctx.sessionID,
                  type: "patch",
                  hash: patch.hash,
                  files: patch.files,
                })
              }
              ctx.snapshot = undefined
            }
            yield* summary
              .summarize({
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.parentID,
              })
              .pipe(Effect.ignore, Effect.forkIn(scope))
            if (
              !ctx.assistantMessage.summary &&
              isOverflow({ cfg: yield* config.get(), tokens: usage.tokens, model: ctx.model })
            ) {
              ctx.needsCompaction = true
            }
            return
          }

          case "text-start":
            ctx.currentText = {
              id: PartID.ascending(),
              messageID: ctx.assistantMessage.id,
              sessionID: ctx.assistantMessage.sessionID,
              type: "text",
              text: "",
              time: { start: Date.now() },
              metadata: value.providerMetadata,
            }
            yield* session.updatePart(ctx.currentText)
            return

          case "text-delta":
            if (!ctx.currentText) return
            ctx.currentText.text += value.text
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePartDelta({
              sessionID: ctx.currentText.sessionID,
              messageID: ctx.currentText.messageID,
              partID: ctx.currentText.id,
              field: "text",
              delta: value.text,
            })
            return

          case "text-end":
            if (!ctx.currentText) return
            // oxlint-disable-next-line no-self-assign -- reactivity trigger
            ctx.currentText.text = ctx.currentText.text
            ctx.currentText.text = (yield* plugin.trigger(
              "experimental.text.complete",
              {
                sessionID: ctx.sessionID,
                messageID: ctx.assistantMessage.id,
                partID: ctx.currentText.id,
              },
              { text: ctx.currentText.text },
            )).text
            {
              const end = Date.now()
              ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
            }
            if (value.providerMetadata) ctx.currentText.metadata = value.providerMetadata
            yield* session.updatePart(ctx.currentText)
            ctx.currentText = undefined
            return

          case "finish":
            return
        }
      })

      const cleanup = Effect.fn("SessionProcessor.cleanup")(function* (unsettledOutcome: NemoRelay.ToolOutcome) {
        yield* Effect.gen(function* () {
          if (ctx.snapshot) {
            const patch = yield* snapshot.patch(ctx.snapshot)
            if (patch.files.length) {
              yield* session.updatePart({
                id: PartID.ascending(),
                messageID: ctx.assistantMessage.id,
                sessionID: ctx.sessionID,
                type: "patch",
                hash: patch.hash,
                files: patch.files,
              })
            }
            ctx.snapshot = undefined
          }

          if (ctx.currentText) {
            const end = Date.now()
            ctx.currentText.time = { start: ctx.currentText.time?.start ?? end, end }
            yield* session.updatePart(ctx.currentText)
            ctx.currentText = undefined
          }

          for (const part of Object.values(ctx.reasoningMap)) {
            const end = Date.now()
            yield* session.updatePart({
              ...part,
              time: { start: part.time.start ?? end, end },
            })
          }
          ctx.reasoningMap = {}

          yield* Effect.forEach(
            [...ctx.toolcalls.values()],
            (call) => Deferred.await(call.done).pipe(Effect.timeout("250 millis"), Effect.ignore),
            { concurrency: "unbounded" },
          )
        }).pipe(Effect.ensuring(settleOutstandingToolCalls(unsettledOutcome)))

        // Settle admitted tool observations before the final message write so a
        // failed updateMessage cannot strand them. Earlier cleanup failures are
        // covered by the ensuring finalizer above.
        ctx.assistantMessage.time.completed = Date.now()
        yield* session.updateMessage(ctx.assistantMessage)
      })

      const halt = Effect.fn("SessionProcessor.halt")(function* (e: unknown) {
        yield* Effect.logError("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
          error: errorMessage(e),
          stack: e instanceof Error ? e.stack : undefined,
        })
        const error = parse(e)
        if (SessionV1.ContextOverflowError.isInstance(error)) {
          if ((yield* config.get()).compaction?.auto === false && !ctx.assistantMessage.summary) {
            ctx.assistantMessage.error = error
            ctx.assistantMessage.finish = "error"
            yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
            yield* status.set(ctx.sessionID, { type: "idle" })
            return
          }
          ctx.needsCompaction = true
          yield* events.publish(Session.Event.Error, { sessionID: ctx.sessionID, error })
          return
        }
        ctx.assistantMessage.error = error
        yield* events.publish(Session.Event.Error, {
          sessionID: ctx.assistantMessage.sessionID,
          error: ctx.assistantMessage.error,
        })
        yield* status.set(ctx.sessionID, { type: "idle" })
      })

      const process = Effect.fn("SessionProcessor.process")(function* (streamInput: LLM.StreamInput) {
        activeToolCategories = new Map(
          Object.entries(streamInput.tools).flatMap(([name, item]) => {
            const category = toolSemanticCategory(item)
            return category ? [[name, category] as const] : []
          }),
        )
        yield* Effect.logInfo("process", {
          "session.id": input.sessionID,
          messageID: input.assistantMessage.id,
        })
        ctx.needsCompaction = false
        ctx.blocked = false
        ctx.shouldBreak = (yield* config.get()).experimental?.continue_loop_on_deny !== true
        let unsettledOutcome: NemoRelay.ToolOutcome = "failed"

        const turn = Effect.gen(function* () {
          const bridge = yield* EffectBridge.make()
          const runtime: ToolRuntime = { bridge, fibers: new Set(), accepting: true }
          activeToolRuntime = runtime
          yield* Effect.gen(function* () {
            ctx.currentText = undefined
            ctx.reasoningMap = {}
            yield* status.set(ctx.sessionID, { type: "busy" })
            const stream = llm.stream(streamInput)

            yield* stream.pipe(
              Stream.tap((event) => handleEvent(event)),
              Stream.takeUntil(() => ctx.needsCompaction),
              Stream.runDrain,
            )
          }).pipe(
            Effect.onInterrupt(() =>
              Effect.gen(function* () {
                unsettledOutcome = "cancelled"
                aborted = true
                if (!ctx.assistantMessage.error) {
                  yield* halt(new DOMException("Aborted", "AbortError"))
                }
              }),
            ),
            Effect.catchCauseIf(
              (cause) => !Cause.hasInterruptsOnly(cause),
              (cause) => {
                const error = Cause.squash(cause)
                unsettledOutcome = blockedError(error) ? "blocked" : "failed"
                return Effect.fail(error)
              },
            ),
            Effect.retry(
              SessionRetry.policy({
                provider: input.model.providerID,
                parse,
                set: (info) =>
                  Effect.all(
                    [
                      status.set(ctx.sessionID, {
                        type: "retry",
                        attempt: info.attempt,
                        message: info.message,
                        action: info.action,
                        next: info.next,
                      }),
                      relay.retryScheduled({
                        runtime: "v1",
                        attempt: info.attempt,
                        delayMs: info.delayMs,
                        delaySource: info.delaySource,
                        errorKind: info.errorKind,
                      }),
                    ],
                    { discard: true },
                  ),
              }),
            ),
            Effect.catch(halt),
            Effect.ensuring(
              Effect.suspend(() =>
                closeToolRuntime(runtime).pipe(Effect.ensuring(Effect.suspend(() => cleanup(unsettledOutcome)))),
              ),
            ),
          )

          if (ctx.needsCompaction) return "compact"
          if (ctx.blocked || ctx.assistantMessage.error) return "stop"
          return "continue"
        })
        const role = callRole(streamInput.agent.name)
        return yield* relay.observeTurn({ runtime: "v1", role }, turn, (exit) =>
          Exit.isSuccess(exit)
            ? exit.value === "stop"
              ? ctx.blocked
                ? "blocked"
                : ctx.assistantMessage.error
                  ? "failed"
                  : "success"
              : "success"
            : undefined,
        )
      })

      return {
        get message() {
          return ctx.assistantMessage
        },
        executeTool,
        updateToolCall,
        completeToolCall,
        process,
      } satisfies Handle
    })

    return Service.of({ create })
  }),
)

export const node = LayerNode.make({
  service: Service,
  layer: layer,
  deps: [
    Session.node,
    Config.node,
    Snapshot.node,
    Agent.node,
    LLM.node,
    Permission.node,
    Plugin.node,
    SessionSummary.node,
    SessionStatus.node,
    Image.node,
    EventV2Bridge.node,
    Database.node,
    NemoRelay.node,
  ],
})

export * as SessionProcessor from "./processor"
