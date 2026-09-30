import { describe, expect, test } from "bun:test"
import { createDefaultServerLifecycle } from "@/server/default-lifecycle"
import { Effect, Stream } from "effect"
import { HttpEffect, HttpServerResponse } from "effect/unstable/http"

const encoder = new TextEncoder()

function streamingResource(events: string[], onFinalize?: () => void) {
  const handler = Effect.gen(function* () {
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        onFinalize?.()
        events.push("request-finalizer")
      }),
    )
    return HttpServerResponse.stream(Stream.concat(Stream.make(encoder.encode("chunk")), Stream.never))
  })
  const webHandler = HttpEffect.toWebHandler(handler)

  return {
    fetch: webHandler,
    dispose() {
      events.push("web-handler-dispose")
    },
  }
}

describe("default server lifecycle with an Effect web handler", () => {
  test("holds the request scope through streaming and disposes after consumer cancellation", async () => {
    const events: string[] = []
    const lifecycle = createDefaultServerLifecycle({
      create: () => streamingResource(events),
      naturalDrainMs: 1_000,
      cancellationDrainMs: 100,
    })
    const handle = lifecycle.get()
    const response = await handle.app.fetch(new Request("http://localhost/stream"))
    const reader = response.body!.getReader()

    const first = await reader.read()
    expect(first.done).toBe(false)
    expect(new TextDecoder().decode(first.value)).toBe("chunk")
    expect(events).toEqual([])

    let closed = false
    const closing = lifecycle.close({ timeoutMs: 1_000 }).finally(() => {
      closed = true
    })
    await Promise.resolve()

    expect(closed).toBe(false)
    expect(events).toEqual([])
    const late = handle.app.fetch(new Request("http://localhost/late"))
    if (!(late instanceof Response)) throw new Error("fenced admission must return synchronously")
    expect(late.status).toBe(503)

    await reader.cancel("consumer finished")
    await closing

    expect(events).toEqual(["request-finalizer", "web-handler-dispose"])
  })

  test("natural-drain timeout cancels the Effect stream and finalizes exactly once", async () => {
    const events: string[] = []
    let finalizers = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () =>
        streamingResource(events, () => {
          finalizers++
        }),
      naturalDrainMs: 5,
      cancellationDrainMs: 100,
    })
    const handle = lifecycle.get()
    const response = await handle.app.fetch(new Request("http://localhost/stream"))
    const reader = response.body!.getReader()

    expect(await reader.read()).toMatchObject({ done: false })
    await lifecycle.close({ timeoutMs: 500 })

    expect(events).toEqual(["request-finalizer", "web-handler-dispose"])
    expect(finalizers).toBe(1)

    await Promise.all([handle.dispose(), handle.dispose(), reader.cancel("already closed")])
    expect(events).toEqual(["request-finalizer", "web-handler-dispose"])
    expect(finalizers).toBe(1)
  })
})
