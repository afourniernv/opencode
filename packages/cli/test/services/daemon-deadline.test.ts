import { describe, expect, test } from "bun:test"
import { Cause, Effect, Exit } from "effect"
import {
  DAEMON_FORCE_STOP_MS,
  DAEMON_GRACEFUL_STOP_MS,
  DAEMON_HTTP_SHUTDOWN_MS,
  matchesDaemonGeneration,
  probeDaemonControl,
  startDaemonControl,
  stopRegisteredProcess,
  type DaemonRegistration,
  type DaemonStopOperations,
} from "../../src/services/daemon"

const owner: DaemonRegistration = {
  id: "owner",
  version: "test",
  url: "http://127.0.0.1:4096",
  pid: 7421,
}
const replacement: DaemonRegistration = { ...owner, id: "replacement", pid: 7422 }
const timing = { gracefulMs: 12, forceMs: 5, pollMs: 1 } as const

function operations(input: {
  readonly authenticate?: boolean | (() => boolean)
  readonly confirmOwnership?: boolean | (() => boolean)
  readonly registration?: DaemonRegistration
  readonly running: () => boolean
  readonly onSignal?: (signal: NodeJS.Signals) => Effect.Effect<void, unknown>
  readonly onSleep?: (milliseconds: number) => void
  readonly now?: () => number
}) {
  const signals: NodeJS.Signals[] = []
  let clock = 0
  const state: { registration?: DaemonRegistration } = { registration: input.registration }
  const value: DaemonStopOperations = {
    authenticate: () =>
      Effect.sync(() =>
        typeof input.authenticate === "function" ? input.authenticate() : (input.authenticate ?? true),
      ),
    registration: () => Effect.succeed(state.registration),
    confirmOwnership: () =>
      Effect.sync(() =>
        typeof input.confirmOwnership === "function" ? input.confirmOwnership() : (input.confirmOwnership ?? true),
      ),
    signal: (_pid, signal) =>
      Effect.sync(() => signals.push(signal)).pipe(
        Effect.andThen(input.onSignal ? input.onSignal(signal) : Effect.void),
      ),
    running: () => Effect.sync(input.running),
    sleep: (milliseconds) =>
      Effect.sync(() => {
        clock += milliseconds
        input.onSleep?.(milliseconds)
      }),
    now: input.now ?? (() => clock),
  }
  return { signals, state, value }
}

describe("daemon shutdown deadlines", () => {
  test("reserves HTTP and Relay teardown before force-stop", () => {
    expect(DAEMON_GRACEFUL_STOP_MS).toBeGreaterThan(DAEMON_HTTP_SHUTDOWN_MS + 5_000)
    expect(DAEMON_FORCE_STOP_MS).toBeGreaterThanOrEqual(5_000)
  })

  test("binds health authentication to the registered daemon generation", () => {
    expect(matchesDaemonGeneration(owner, owner.id ?? null)).toBe(true)
    expect(matchesDaemonGeneration(owner, replacement.id ?? null)).toBe(false)
    expect(matchesDaemonGeneration({ ...owner, id: undefined }, null)).toBe(false)
  })

  test("keeps a generation proof alive independently of the HTTP listener", async () => {
    const control = await startDaemonControl(owner.id ?? "owner")
    const registered = { ...owner, controlPort: control.port }

    try {
      expect(await probeDaemonControl(registered)).toBe(true)
      expect(await probeDaemonControl({ ...registered, id: "other" })).toBe(false)
    } finally {
      await control.close()
    }
    expect(await probeDaemonControl(registered, 25)).toBe(false)
  })

  test("allows a graceful exit after the former five-second window", async () => {
    let elapsed = 0
    const op = operations({
      registration: owner,
      running: () => elapsed < 6_000,
      onSleep: (milliseconds) => (elapsed += milliseconds),
      now: () => elapsed,
    })

    const outcome = await Effect.runPromise(
      stopRegisteredProcess(owner, op.value, { gracefulMs: 12_000, forceMs: 5_000, pollMs: 1_000 }),
    )

    expect(outcome).toBe("stopped")
    expect(elapsed).toBe(6_000)
    expect(op.signals).toEqual(["SIGTERM"])
    expect(op.state.registration).toEqual(owner)
  })

  test("does not consume a replacement registered as graceful exit is observed", async () => {
    let checks = 0
    let op: ReturnType<typeof operations>
    op = operations({
      registration: owner,
      running: () => {
        checks += 1
        if (checks === 1) return true
        op.state.registration = replacement
        return false
      },
    })

    const outcome = await Effect.runPromise(stopRegisteredProcess(owner, op.value, timing))

    expect(outcome).toBe("stopped")
    expect(op.signals).toEqual(["SIGTERM"])
    expect(op.state.registration).toEqual(replacement)
  })

  test("force-stops only the same stuck registered owner", async () => {
    let killed = false
    let authentication = 0
    const op = operations({
      registration: owner,
      running: () => !killed,
      // HTTP disappears during normal shutdown; ownership escalation uses the
      // independent generation control rather than attempting this twice.
      authenticate: () => ++authentication === 1,
      confirmOwnership: true,
      onSignal: (signal) =>
        Effect.sync(() => {
          killed = signal === "SIGKILL"
        }),
    })

    const outcome = await Effect.runPromise(stopRegisteredProcess(owner, op.value, timing))

    expect(outcome).toBe("stopped")
    expect(authentication).toBe(1)
    expect(op.signals).toEqual(["SIGTERM", "SIGKILL"])
    expect(op.state.registration).toEqual(owner)
  })

  test("does not force-stop or consume a replacement registration", async () => {
    const op = operations({ registration: replacement, running: () => true })
    const outcome = await Effect.runPromise(stopRegisteredProcess(owner, op.value, timing))

    expect(outcome).toBe("ownership_lost")
    expect(op.signals).toEqual(["SIGTERM"])
    expect(op.state.registration).toEqual(replacement)
  })

  test("does not force-stop when registration identity is missing", async () => {
    const op = operations({ running: () => true })
    const outcome = await Effect.runPromise(stopRegisteredProcess(owner, op.value, timing))

    expect(outcome).toBe("ownership_lost")
    expect(op.signals).toEqual(["SIGTERM"])
  })

  test("does not force-stop when the post-HTTP generation control cannot prove ownership", async () => {
    const op = operations({
      registration: owner,
      running: () => true,
      confirmOwnership: false,
    })

    const outcome = await Effect.runPromise(stopRegisteredProcess(owner, op.value, timing))

    expect(outcome).toBe("ownership_lost")
    expect(op.signals).toEqual(["SIGTERM"])
  })

  test("propagates SIGKILL failure and a force-stop timeout", async () => {
    const failedSignal = operations({
      registration: owner,
      running: () => true,
      onSignal: (signal) => (signal === "SIGKILL" ? Effect.fail(new Error("kill denied")) : Effect.void),
    })
    const failedSignalExit = await Effect.runPromiseExit(stopRegisteredProcess(owner, failedSignal.value, timing))
    expect(Exit.isFailure(failedSignalExit)).toBe(true)
    if (Exit.isFailure(failedSignalExit))
      expect(Cause.squash(failedSignalExit.cause)).toHaveProperty("message", "kill denied")

    const stuck = operations({ registration: owner, running: () => true })
    const stuckExit = await Effect.runPromiseExit(stopRegisteredProcess(owner, stuck.value, timing))
    expect(Exit.isFailure(stuckExit)).toBe(true)
    if (Exit.isFailure(stuckExit))
      expect(Cause.squash(stuckExit.cause)).toHaveProperty(
        "message",
        `Server process ${owner.pid} is still running after SIGKILL`,
      )
    expect(stuck.signals).toEqual(["SIGTERM", "SIGKILL"])

    const inaccessible = operations({ registration: owner, running: () => true })
    const inaccessibleExit = await Effect.runPromiseExit(
      stopRegisteredProcess(
        owner,
        {
          ...inaccessible.value,
          running: () => Effect.fail(Object.assign(new Error("denied"), { code: "EPERM" })),
        },
        timing,
      ),
    )
    expect(Exit.isFailure(inaccessibleExit)).toBe(true)
    if (Exit.isFailure(inaccessibleExit))
      expect(Cause.squash(inaccessibleExit.cause)).toHaveProperty("message", "denied")
  })

  test("uses monotonic deadlines when polling sleeps overshoot", async () => {
    let now = 0
    const sleeps: number[] = []
    const op = operations({
      registration: owner,
      running: () => true,
      now: () => now,
      onSleep: (milliseconds) => {
        sleeps.push(milliseconds)
        now += milliseconds + 100
      },
    })

    const exit = await Effect.runPromiseExit(stopRegisteredProcess(owner, op.value, timing))

    expect(Exit.isFailure(exit)).toBe(true)
    expect(op.signals).toEqual(["SIGTERM", "SIGKILL"])
    expect(sleeps).toEqual([1, 1])
  })
})
