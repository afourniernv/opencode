export * as ToolRegistry from "./registry"

import { ToolFailure, ToolOutput, type ToolCall, type ToolDefinition, type ToolResultValue } from "@opencode-ai/llm"
import { Cause, Context, Effect, Exit, Layer, Scope } from "effect"
import { AgentV2 } from "../agent"
import { PermissionV2 } from "../permission"
import { QuestionV2 } from "../question"
import * as NemoRelay from "../observability/nemo-relay"
import { SessionMessage } from "../session/message"
import { SessionSchema } from "../session/schema"
import { ToolOutputStore } from "../tool-output-store"
import { Wildcard } from "../util/wildcard"
import { ApplicationTools } from "./application-tools"
import { definition, permission, settle, validateName, type AnyTool, type RegistrationError } from "./tool"
import { Tools } from "./tools"
import { makeLocationNode } from "../effect/app-node"

export type ExecuteInput = {
  readonly sessionID: SessionSchema.ID
  readonly agent: AgentV2.ID
  readonly assistantMessageID: SessionMessage.ID
  readonly call: ToolCall
}

export interface Interface {
  readonly materialize: (permissions?: PermissionV2.Ruleset) => Effect.Effect<Materialization>
  /** Internal registration capability exposed publicly only through Tools.Service. */
  readonly register: (tools: Readonly<Record<string, AnyTool>>) => Effect.Effect<void, RegistrationError, Scope.Scope>
}

export interface Materialization {
  readonly definitions: ReadonlyArray<ToolDefinition>
  readonly settle: (input: ExecuteInput) => Effect.Effect<Settlement, ToolOutputStore.Error>
}

export interface Settlement {
  readonly result: ToolResultValue
  readonly output?: ToolOutput
  readonly outputPaths?: ReadonlyArray<string>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/v2/ToolRegistry") {}

const registryLayer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const applications = yield* ApplicationTools.Service
    const resources = yield* ToolOutputStore.Service
    const relay = yield* NemoRelay.Service
    type Registration = { readonly identity: object; readonly tool: AnyTool }
    const local = new Map<string, Array<{ readonly token: object; readonly registration: Registration }>>()

    const settleWith = Effect.fn("ToolRegistry.settle")(function* (
      input: ExecuteInput,
      advertised?: object,
      missing = false,
    ) {
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const observation = yield* relay.beginTool({ name: input.call.name, execution: "local" })
          let terminalFailure: ToolFailure | undefined
          const settlement = Effect.gen(function* () {
            if (missing) return { result: { type: "error" as const, value: `Unknown tool: ${input.call.name}` } }
            const registration =
              local.get(input.call.name)?.at(-1)?.registration ?? applications.entries().get(input.call.name)
            if (!registration)
              return {
                result: {
                  type: "error" as const,
                  value: advertised ? `Stale tool call: ${input.call.name}` : `Unknown tool: ${input.call.name}`,
                },
              }
            if (advertised && registration.identity !== advertised)
              return { result: { type: "error" as const, value: `Stale tool call: ${input.call.name}` } }
            const pending = yield* settle(registration.tool, input.call, {
              sessionID: input.sessionID,
              agent: input.agent,
              assistantMessageID: input.assistantMessageID,
              toolCallID: input.call.id,
            }).pipe(
              Effect.map((output) => ({ output })),
              Effect.catchTag("LLM.ToolFailure", (failure) => {
                terminalFailure = failure
                return Effect.succeed({ result: { type: "error" as const, value: failure.message } })
              }),
            )
            if ("result" in pending) return pending
            const output = pending.output
            const bounded = yield* resources.bound({ sessionID: input.sessionID, toolCallID: input.call.id, output })
            const result = ToolOutput.toResultValue(bounded.output)
            if (result.type === "error")
              return bounded.outputPaths.length > 0 ? { result, outputPaths: bounded.outputPaths } : { result }
            return bounded.outputPaths.length > 0
              ? { result, output: bounded.output, outputPaths: bounded.outputPaths }
              : { result, output: bounded.output }
          })
          return yield* observation.run(restore(settlement)).pipe(
            Effect.onExit((exit) => {
              const blocked =
                Exit.isFailure(exit) &&
                exit.cause.reasons.some((reason) => {
                  const error = Cause.isDieReason(reason)
                    ? reason.defect
                    : Cause.isFailReason(reason)
                      ? reason.error
                      : undefined
                  return isBlocked(error)
                })
              const outcome: NemoRelay.ToolOutcome = Exit.isSuccess(exit)
                ? exit.value.result.type === "error"
                  ? isBlocked(terminalFailure?.error)
                    ? "blocked"
                    : "failed"
                  : "success"
                : blocked
                  ? "blocked"
                  : Cause.hasInterruptsOnly(exit.cause)
                    ? "cancelled"
                    : "failed"
              const terminalResult =
                Exit.isSuccess(exit) &&
                exit.value.result.type !== "error" &&
                NemoRelay.toolCategory(input.call.name) === "terminal"
                  ? NemoRelay.terminalResultFamily(exit.value.output?.structured)
                  : undefined
              return observation
                .complete(outcome, terminalResult === undefined ? undefined : { terminalResult })
                .pipe(Effect.catchCause(() => Effect.void))
            }),
          )
        }),
      )
    })

    return Service.of({
      register: Effect.fn("ToolRegistry.register")(function* (tools) {
        const entries = Object.entries(tools)
        if (entries.length === 0) return
        yield* Effect.forEach(entries, ([name]) => validateName(name), { discard: true })
        yield* Effect.uninterruptible(
          Effect.gen(function* () {
            const token = {}
            for (const [name, tool] of entries)
              local.set(name, [...(local.get(name) ?? []), { token, registration: { identity: {}, tool } }])
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                for (const [name] of entries) {
                  const registrations = local.get(name)?.filter((registration) => registration.token !== token) ?? []
                  if (registrations.length > 0) local.set(name, registrations)
                  else local.delete(name)
                }
              }),
            )
          }),
        )
      }),
      materialize: Effect.fn("ToolRegistry.materialize")(function* (permissions = []) {
        const registrations = new Map(applications.entries())
        for (const [name, entries] of local) {
          const registration = entries.at(-1)?.registration
          if (registration) registrations.set(name, registration)
        }
        for (const [name, registration] of registrations)
          if (whollyDisabled(permission(registration.tool, name), permissions)) registrations.delete(name)
        return {
          definitions: Array.from(registrations, ([name, registration]) => definition(name, registration.tool)),
          settle: (input) => {
            const registration = registrations.get(input.call.name)
            return settleWith(input, registration?.identity, registration === undefined)
          },
        }
      }),
    })
  }),
)

const layer = Layer.effect(
  Tools.Service,
  Service.use((registry) => Effect.succeed(Tools.Service.of({ register: registry.register }))),
).pipe(Layer.provideMerge(registryLayer))

function whollyDisabled(action: string, rules: PermissionV2.Ruleset) {
  const rule = rules.findLast((rule) => Wildcard.match(action, rule.action))
  return rule?.resource === "*" && rule.effect === "deny"
}

function isBlocked(error: unknown) {
  return (
    error instanceof PermissionV2.BlockedError ||
    error instanceof PermissionV2.DeclinedError ||
    error instanceof PermissionV2.CorrectedError ||
    error instanceof QuestionV2.RejectedError
  )
}

export const node = makeLocationNode({
  service: Service,
  layer,
  deps: [ApplicationTools.node, ToolOutputStore.node, NemoRelay.node],
})

export const toolsNode = makeLocationNode({
  service: Tools.Service,
  layer,
  deps: [ApplicationTools.node, ToolOutputStore.node, NemoRelay.node],
})
