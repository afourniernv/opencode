import { Global } from "@opencode-ai/core/global"
import { InstallationVersion } from "@opencode-ai/core/installation/version"
import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import { ServerAuth } from "@opencode-ai/server/auth"
import { Context, Effect, FileSystem, Layer, Option, Schedule, Schema, Scope } from "effect"
import { HttpServer } from "effect/unstable/http"
import { randomBytes } from "crypto"
import { spawn } from "node:child_process"
import { once } from "node:events"
import { connect, createServer as createNetServer, type Server as NetServer } from "node:net"
import path from "path"

export interface Interface {
  readonly client: () => Effect.Effect<ReturnType<typeof createOpencodeClient>, unknown>
  readonly transport: () => Effect.Effect<{ url: string; headers: RequestInit["headers"] }, unknown>
  readonly start: () => Effect.Effect<string, Error>
  readonly status: () => Effect.Effect<string | undefined>
  readonly stop: () => Effect.Effect<void, unknown>
  readonly password: (value?: string) => Effect.Effect<string, unknown>
  readonly register: (
    address: HttpServer.Address,
    generation: string,
    controlPort: number,
  ) => Effect.Effect<void, unknown, Scope.Scope>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/cli/Daemon") {}

const Registration = Schema.Struct({
  id: Schema.optional(Schema.String),
  version: Schema.optional(Schema.String),
  url: Schema.String,
  pid: Schema.Int.check(Schema.isGreaterThan(0)),
  controlPort: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
})
export type DaemonRegistration = typeof Registration.Type

export const DAEMON_HTTP_SHUTDOWN_MS = 1_000
export const DAEMON_GRACEFUL_STOP_MS = 12_000
export const DAEMON_FORCE_STOP_MS = 5_000
export const DAEMON_GENERATION_HEADER = "x-opencode-daemon-generation"
const DAEMON_STOP_POLL_MS = 50

function sameRegistration(left: DaemonRegistration, right: DaemonRegistration) {
  return (
    left.id === right.id &&
    left.version === right.version &&
    left.url === right.url &&
    left.pid === right.pid &&
    left.controlPort === right.controlPort
  )
}

export function matchesDaemonGeneration(info: DaemonRegistration, generation: string | null) {
  return info.id !== undefined && generation === info.id
}

export type DaemonControl = {
  readonly port: number
  readonly close: () => Promise<void>
}

export async function startDaemonControl(generation: string): Promise<DaemonControl> {
  const server = createNetServer((socket) => socket.end(generation))
  server.listen({ host: "127.0.0.1", port: 0 })
  await once(server, "listening")
  const address = server.address()
  if (address === null || typeof address === "string") {
    await closeNetServer(server)
    throw new Error("Daemon generation control did not bind a TCP address")
  }
  return { port: address.port, close: () => closeNetServer(server) }
}

function closeNetServer(server: NetServer) {
  if (!server.listening) return Promise.resolve()
  return new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve())))
}

export function probeDaemonControl(info: DaemonRegistration, timeoutMs = 500): Promise<boolean> {
  const generation = info.id
  const controlPort = info.controlPort
  if (generation === undefined || controlPort === undefined) return Promise.resolve(false)
  return new Promise((resolve) => {
    const socket = connect({ host: "127.0.0.1", port: controlPort })
    let settled = false
    let value = ""
    const finish = (result: boolean) => {
      if (settled) return
      settled = true
      socket.destroy()
      resolve(result)
    }
    socket.setEncoding("utf8")
    socket.setTimeout(Math.max(1, timeoutMs), () => finish(false))
    socket.on("data", (chunk: string) => {
      value += chunk
      if (value.length > 128) finish(false)
    })
    socket.on("end", () => finish(value === generation))
    socket.on("error", () => finish(false))
  })
}

export type DaemonStopOutcome = "stopped" | "ownership_lost"

export class DaemonOwnershipError extends Error {
  override readonly name = "DaemonOwnershipError"

  constructor(readonly pid: number) {
    super(`Refusing to stop process ${pid} because daemon ownership could not be verified`)
  }
}

export interface DaemonStopOperations {
  readonly authenticate: (expected: DaemonRegistration) => Effect.Effect<boolean, unknown>
  readonly registration: () => Effect.Effect<DaemonRegistration | undefined, unknown>
  readonly confirmOwnership: (expected: DaemonRegistration) => Effect.Effect<boolean, unknown>
  readonly signal: (pid: number, signal: NodeJS.Signals) => Effect.Effect<void, unknown>
  readonly running: (pid: number) => Effect.Effect<boolean, unknown>
  readonly sleep: (milliseconds: number) => Effect.Effect<void, unknown>
  readonly now: () => number
}

export function stopRegisteredProcess(
  info: DaemonRegistration,
  operations: DaemonStopOperations,
  timing: {
    readonly gracefulMs: number
    readonly forceMs: number
    readonly pollMs: number
  } = {
    gracefulMs: DAEMON_GRACEFUL_STOP_MS,
    forceMs: DAEMON_FORCE_STOP_MS,
    pollMs: DAEMON_STOP_POLL_MS,
  },
): Effect.Effect<DaemonStopOutcome, unknown> {
  const awaitExit = (timeoutMs: number) =>
    Effect.gen(function* () {
      const deadline = operations.now() + Math.max(0, timeoutMs)
      const pollMs = Math.max(1, timing.pollMs)
      while (true) {
        if (!(yield* operations.running(info.pid))) return true
        const remaining = deadline - operations.now()
        if (remaining <= 0) return false
        const delay = Math.min(pollMs, remaining)
        yield* operations.sleep(delay)
      }
    })

  return Effect.gen(function* () {
    if (!(yield* operations.authenticate(info))) return "ownership_lost" as const
    yield* operations.signal(info.pid, "SIGTERM")
    if (yield* awaitExit(timing.gracefulMs)) return "stopped" as const

    const latest = yield* operations.registration()
    if (!latest || !sameRegistration(latest, info)) return "ownership_lost" as const
    // A persisted registration alone cannot prove that a recycled PID still
    // belongs to this daemon. The loopback generation control remains alive
    // through HTTP and Relay teardown, so require it before escalation.
    if (!(yield* operations.confirmOwnership(info))) return "ownership_lost" as const
    yield* operations.signal(info.pid, "SIGKILL")
    if (yield* awaitExit(timing.forceMs)) return "stopped" as const
    return yield* Effect.fail(new Error(`Server process ${info.pid} is still running after SIGKILL`))
  })
}

export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const directory = Global.Path.state
    const file = path.join(directory, "server.json")
    const passwordFile = path.join(directory, "password")
    const decodeRegistration = Schema.decodeUnknownEffect(Schema.fromJsonString(Registration))

    const password = Effect.fn("cli.daemon.password")(function* (value?: string) {
      const existing = yield* fs.readFileString(passwordFile).pipe(Effect.catch(() => Effect.succeed(undefined)))
      if (value === undefined && existing) return existing

      // Keep one private credential across server restarts so discovered clients
      // can reconnect without exposing a password flag or environment variable.
      const generated = value ?? randomBytes(32).toString("base64url")
      const temp = passwordFile + ".tmp"
      yield* fs.makeDirectory(directory, { recursive: true })
      yield* fs.writeFileString(temp, generated, { mode: 0o600 })
      yield* fs.rename(temp, passwordFile)
      return generated
    })

    const registration = Effect.fnUntraced(function* () {
      return yield* fs.readFileString(file).pipe(Effect.flatMap(decodeRegistration))
    })

    const createClient = Effect.fnUntraced(function* (url: string) {
      return createOpencodeClient({ baseUrl: url, headers: ServerAuth.headers({ password: yield* password() }) })
    })

    const healthy = Effect.fnUntraced(function* () {
      const info = yield* registration()
      const client = yield* createClient(info.url)
      const response = yield* Effect.tryPromise(() => client.v2.health.get({ signal: AbortSignal.timeout(2_000) }))
      if (
        response.data?.healthy === true &&
        matchesDaemonGeneration(info, response.response.headers.get(DAEMON_GENERATION_HEADER))
      )
        return info
      return yield* Effect.fail(new Error("Registered server is not healthy"))
    })

    const compatible = Effect.fnUntraced(function* () {
      const info = yield* healthy()
      if (info.version === InstallationVersion) return info
      return yield* Effect.fail(new Error("Registered server version does not match the client"))
    })

    const signal = (pid: number, next: NodeJS.Signals) =>
      Effect.try({ try: () => process.kill(pid, next), catch: (cause) => cause }).pipe(
        Effect.catch((cause) =>
          typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ESRCH"
            ? Effect.void
            : Effect.fail(cause),
        ),
      )

    const running = (pid: number) =>
      Effect.try({
        try: () => {
          process.kill(pid, 0)
          return true
        },
        catch: (cause) => cause,
      }).pipe(
        Effect.catch((cause) =>
          typeof cause === "object" && cause !== null && "code" in cause && cause.code === "ESRCH"
            ? Effect.succeed(false)
            : Effect.fail(cause),
        ),
      )

    const stopProcess = (info: DaemonRegistration) =>
      stopRegisteredProcess(info, {
        authenticate: (expected) =>
          healthy().pipe(
            Effect.option,
            Effect.map((current) => Option.isSome(current) && sameRegistration(current.value, expected)),
          ),
        registration: () => registration().pipe(Effect.option, Effect.map(Option.getOrUndefined)),
        confirmOwnership: (expected) => Effect.tryPromise(() => probeDaemonControl(expected)),
        signal,
        running,
        sleep: (milliseconds) => Effect.sleep(`${milliseconds} millis`),
        now: () => performance.now(),
      })

    const start = Effect.fn("cli.daemon.start")(function* () {
      const existing = yield* healthy().pipe(Effect.option)
      const found = Option.getOrUndefined(existing)
      const compiled = path.basename(process.execPath).replace(/\.exe$/, "") !== "bun"
      if (found?.version === InstallationVersion && compiled) return found.url
      if (found) yield* stopProcess(found).pipe(Effect.ignore)

      const entrypoint = compiled ? undefined : process.argv[1]
      if (!compiled && entrypoint === undefined)
        return yield* Effect.fail(new Error("Failed to resolve CLI entrypoint"))
      yield* Effect.try({
        try: () => {
          spawn(process.execPath, [...(entrypoint ? [entrypoint] : []), "serve", "--register"], {
            detached: true,
            stdio: "ignore",
          }).unref()
        },
        catch: (cause) => new Error("Failed to start server", { cause }),
      })

      return yield* compatible().pipe(
        Effect.retry(Schedule.spaced("50 millis").pipe(Schedule.both(Schedule.recurs(100)))),
        Effect.map((info) => info.url),
        Effect.mapError(() => new Error("Failed to start server")),
      )
    })

    const transport = Effect.fn("cli.daemon.transport")(function* () {
      return { url: yield* start(), headers: ServerAuth.headers({ password: yield* password() }) }
    })

    const client = Effect.fn("cli.daemon.client")(function* () {
      const connection = yield* transport()
      return createOpencodeClient({ baseUrl: connection.url, headers: connection.headers })
    })

    const status = Effect.fn("cli.daemon.status")(function* () {
      const existing = yield* healthy().pipe(Effect.option)
      const found = Option.getOrUndefined(existing)
      if (found?.version === InstallationVersion) return found.url
      return undefined
    })

    const stop = Effect.fn("cli.daemon.stop")(function* () {
      const registered = yield* registration().pipe(Effect.option)
      if (Option.isNone(registered)) return
      const existing = yield* healthy().pipe(Effect.option)
      // A stale registration may point at a PID that has since been reused by
      // another process. Only signal the PID after authenticating the server.
      if (Option.isNone(existing)) {
        if (!(yield* running(registered.value.pid))) return
        return yield* Effect.fail(new DaemonOwnershipError(registered.value.pid))
      }
      const outcome = yield* stopProcess(existing.value)
      if (outcome === "ownership_lost" && (yield* running(existing.value.pid)))
        return yield* Effect.fail(new DaemonOwnershipError(existing.value.pid))
    })

    const register = Effect.fn("cli.daemon.register")(function* (
      address: HttpServer.Address,
      id: string,
      controlPort: number,
    ) {
      const temp = file + "." + id + ".tmp"
      yield* fs.makeDirectory(directory, { recursive: true })
      yield* fs.writeFileString(
        temp,
        JSON.stringify({
          id,
          version: InstallationVersion,
          url: HttpServer.formatAddress(address),
          pid: process.pid,
          controlPort,
        }),
        { mode: 0o600 },
      )
      yield* fs.rename(temp, file)
      yield* registration().pipe(
        Effect.flatMap((info) => (info.id === id ? Effect.void : signal(process.pid, "SIGTERM"))),
        Effect.catch(() => signal(process.pid, "SIGTERM")),
        Effect.repeat(Schedule.spaced("10 seconds")),
        Effect.forkScoped,
      )
      // Keep the identity token through process exit. The controlling stop
      // path deliberately retains the last token: read/compare/unlink is
      // not atomic and could delete a concurrently registered replacement.
      // A later registration safely replaces stale state with an atomic rename.
    })

    return Service.of({ client, transport, start, status, stop, password, register })
  }),
)

export * as Daemon from "./daemon"
