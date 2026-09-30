import { NodeHttpServer } from "@effect/platform-node"
import { Credential } from "@opencode-ai/core/credential"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { PermissionSaved } from "@opencode-ai/core/permission/saved"
import { Context, Layer, Option } from "effect"
import * as Effect from "effect/Effect"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { createServer } from "node:http"
import { randomUUID } from "node:crypto"
import { createRoutes } from "@opencode-ai/server/routes"
import { Commands } from "../commands"
import { Runtime } from "../../framework/runtime"
import { DAEMON_GENERATION_HEADER, DAEMON_HTTP_SHUTDOWN_MS, Daemon, startDaemonControl } from "../../services/daemon"

export default Runtime.handler(
  Commands.commands.serve,
  Effect.fn("cli.serve")(function* (input) {
    const daemon = yield* Daemon.Service
    const generation = randomUUID()
    // This bracket deliberately encloses the HTTP graph's nested scope. The
    // loopback control therefore keeps proving PID ownership while HTTP and
    // Relay finalizers run, and closes only after they finish.
    return yield* Effect.acquireUseRelease(
      Effect.tryPromise(() => startDaemonControl(generation)),
      (control) =>
        Effect.scoped(
          Effect.gen(function* () {
            const address = yield* listen(input.hostname, input.port, yield* daemon.password(), generation)
            if (input.register) yield* daemon.register(address, generation, control.port)
            console.log(`server listening on ${HttpServer.formatAddress(address)}`)
            return yield* Effect.never
          }),
        ),
      (control) => Effect.tryPromise(() => control.close()).pipe(Effect.ignore),
    )
  }),
)

function listen(hostname: string, port: Option.Option<number>, password: string, generation: string) {
  if (Option.isSome(port)) return bind(hostname, port.value, password, generation)
  const next = (port: number): ReturnType<typeof bind> =>
    bind(hostname, port, password, generation).pipe(
      Effect.catch((error) => (port === 65_535 ? Effect.fail(error) : next(port + 1))),
    )
  return next(4096)
}

function bind(hostname: string, port: number, password: string, generation: string) {
  return Layer.build(
    HttpRouter.serve(createRoutes(password), { disableListenLog: true, disableLogger: true }).pipe(
      Layer.provideMerge(
        NodeHttpServer.layer(
          () => {
            const server = createServer()
            server.on("request", (_request, response) => response.setHeader(DAEMON_GENERATION_HEADER, generation))
            return server
          },
          {
            port,
            host: hostname,
            gracefulShutdownTimeout: `${DAEMON_HTTP_SHUTDOWN_MS} millis`,
          },
        ),
      ),
      Layer.provide(AppNodeBuilder.build(LayerNode.group([Credential.node, PermissionSaved.node]))),
    ),
  ).pipe(Effect.map((context) => Context.get(context, HttpServer.HttpServer).address))
}
