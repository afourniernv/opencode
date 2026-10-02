import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import * as NemoRelay from "@opencode-ai/core/observability/nemo-relay"
import { Context, Effect, Layer } from "effect"
import { EOL } from "os"
import { Commands } from "../../commands"
import { Runtime } from "../../../framework/runtime"

export default Runtime.handler(
  Commands.commands.debug.commands["nemo-relay"],
  Effect.fn("cli.debug.nemoRelay")(function* () {
    yield* Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(AppNodeBuilder.build(NemoRelay.node))
        const relay = Context.get(context, NemoRelay.Service)
        process.stdout.write(
          JSON.stringify(
            {
              service: relay.status,
              process: NemoRelay.health(),
              exporterDelivery: "not_probed",
            },
            null,
            2,
          ) + EOL,
        )
      }),
    )
  }),
)
