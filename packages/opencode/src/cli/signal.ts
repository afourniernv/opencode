import { Effect } from "effect"

/** Wait for a terminating signal without bypassing the root async cleanup path. */
export const waitForTerminationSignal = Effect.callback<void>((resume) => {
  const finish = (code: number) => {
    process.exitCode = code
    resume(Effect.void)
  }
  const sigint = () => finish(130)
  const sigterm = () => finish(143)
  process.once("SIGINT", sigint)
  process.once("SIGTERM", sigterm)
  return Effect.sync(() => {
    process.off("SIGINT", sigint)
    process.off("SIGTERM", sigterm)
  })
})

type ServerHandle = {
  readonly stop: (closeActiveConnections?: boolean) => Promise<void>
}

/**
 * Stop external admission, then dispose every reachable instance. Each stage
 * is bounded and fail-open so one broken cleanup callback cannot skip the next.
 */
export const shutdownServer = (server: ServerHandle) =>
  Effect.gen(function* () {
    const attempt = <A, E, R>(stage: string, timeout: number, action: Effect.Effect<A, E, R>) =>
      action.pipe(
        Effect.timeout(timeout),
        Effect.asVoid,
        Effect.catchCause((cause) => Effect.logWarning("server cleanup stage failed", { stage, cause })),
      )

    yield* attempt(
      "server",
      1_500,
      Effect.promise(() => server.stop(true)),
    )
    const lifecycle = yield* Effect.promise(() => import("../server/global-lifecycle")).pipe(
      Effect.timeout(500),
      Effect.option,
    )
    if (lifecycle._tag === "None") {
      yield* Effect.logWarning("server cleanup stage failed", { stage: "load_instance_lifecycle" })
      return
    }
    yield* attempt(
      "instances",
      2_500,
      lifecycle.value.disposeAllInstancesAndEmitGlobalDisposed({ swallowErrors: true }),
    )
  }).pipe(Effect.catchCause((cause) => Effect.logWarning("server cleanup failed", { cause })))
