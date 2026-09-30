import { describe, expect, test } from "bun:test"
import {
  createDefaultServerLifecycle,
  DefaultServerCloseTimeoutError,
  type DefaultServerHandle,
  type DefaultServerResource,
} from "@/server/default-lifecycle"
import { Effect, Exit, Scope } from "effect"

const request = (path = "/") => new Request(new URL(path, "http://localhost"))

function requireResponse(value: Response | Promise<Response>): Response {
  if (value instanceof Response) return value
  throw new Error("expected a synchronous response")
}

async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try {
    await promise
  } catch (error) {
    return error
  }
  throw new Error("expected promise to reject")
}

describe("default server lifecycle manager", () => {
  test("fences admission synchronously while accepted requests drain in reverse order", async () => {
    const first = Promise.withResolvers<Response>()
    const second = Promise.withResolvers<Response>()
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch(input) {
          return input.url.endsWith("/first") ? first.promise : second.promise
        },
        dispose() {
          disposed++
        },
      }),
      naturalDrainMs: 100,
      cancellationDrainMs: 0,
    })
    const handle = lifecycle.get()
    const firstResponse = handle.app.fetch(request("/first"))
    const secondResponse = handle.app.fetch(request("/second"))

    const closing = lifecycle.close({ timeoutMs: 500 })
    const fenced = handle.app.fetch(request("/late"))
    expect(requireResponse(fenced).status).toBe(503)

    second.resolve(new Response(null, { status: 204 }))
    await secondResponse
    expect(disposed).toBe(0)
    first.resolve(new Response(null, { status: 204 }))
    await firstResponse
    await closing
    expect(disposed).toBe(1)
    expect(lifecycle.loaded()).toBe(false)
  })

  test("forwards caller abort and releases a rejected handler", async () => {
    const pending = Promise.withResolvers<Response>()
    let observed: AbortSignal | undefined
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch(input) {
          observed = input.signal
          return pending.promise
        },
        dispose() {},
      }),
      naturalDrainMs: 0,
      cancellationDrainMs: 0,
    })
    const controller = new AbortController()
    const response = lifecycle.get().app.fetch(new Request("http://localhost", { signal: controller.signal }))
    const reason = new Error("caller stopped")
    controller.abort(reason)

    expect(observed?.aborted).toBe(true)
    expect(observed?.reason).toBe(reason)
    pending.reject(reason)
    expect(await rejection(Promise.resolve(response))).toBe(reason)
    await lifecycle.close()
    expect(lifecycle.loaded()).toBe(false)
  })

  test("retains a streaming response lease until EOF", async () => {
    let bodyController: ReadableStreamDefaultController<Uint8Array> | undefined
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () =>
          new Response(
            new ReadableStream<Uint8Array>({
              start(controller) {
                bodyController = controller
              },
            }),
          ),
        dispose() {
          disposed++
        },
      }),
      naturalDrainMs: 500,
      cancellationDrainMs: 0,
    })
    const result = lifecycle.get().app.fetch(request())
    const response = requireResponse(result)
    const closing = lifecycle.close({ timeoutMs: 1_000 })

    expect(disposed).toBe(0)
    bodyController?.enqueue(new TextEncoder().encode("done"))
    bodyController?.close()
    expect(await response.text()).toBe("done")
    await closing
    expect(disposed).toBe(1)
  })

  test("closes the Effect scope only after a streaming response reaches EOF", async () => {
    const order: string[] = []
    const scope = Scope.makeUnsafe()
    await Effect.runPromise(
      Scope.addFinalizer(
        scope,
        Effect.sync(() => {
          order.push("effect-finalizer")
        }),
      ),
    )
    let pull = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () =>
          new Response(
            new ReadableStream<Uint8Array>({
              pull(controller) {
                if (pull++ === 0) {
                  controller.enqueue(new TextEncoder().encode("done"))
                  return
                }
                order.push("source-eof")
                controller.close()
              },
            }),
          ),
        dispose: () => Effect.runPromise(Scope.close(scope, Exit.void)),
      }),
      naturalDrainMs: 500,
    })
    const result = lifecycle.get().app.fetch(request())
    const response = requireResponse(result)
    const closing = lifecycle.close({ timeoutMs: 1_000 })

    expect(order).not.toContain("effect-finalizer")
    expect(await response.text()).toBe("done")
    await closing
    expect(order).toEqual(["source-eof", "effect-finalizer"])
  })

  test("shutdown aborts the private request and cancels the original response reader", async () => {
    let observed: AbortSignal | undefined
    let cancelReason: unknown
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch(input) {
          observed = input.signal
          return new Response(
            new ReadableStream({
              cancel(reason) {
                cancelReason = reason
              },
            }),
          )
        },
        dispose() {
          disposed++
        },
      }),
      naturalDrainMs: 0,
      cancellationDrainMs: 0,
    })
    const response = lifecycle.get().app.fetch(request())
    expect(response).toBeInstanceOf(Response)

    const closing = lifecycle.shutdown({ timeoutMs: 100 })
    expect(observed?.aborted).toBe(true)
    expect(cancelReason).toBeInstanceOf(Error)
    await closing
    expect(disposed).toBe(1)
    expect(lifecycle.get().app.fetch(request())).toHaveProperty("status", 503)
  })

  test("consumer cancellation cancels the original reader and releases its lease", async () => {
    let cancelReason: unknown
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () =>
          new Response(
            new ReadableStream({
              cancel(reason) {
                cancelReason = reason
              },
            }),
          ),
        dispose() {
          disposed++
        },
      }),
    })
    const result = lifecycle.get().app.fetch(request())
    const reader = requireResponse(result).body?.getReader()
    await reader?.cancel("reader done")
    await lifecycle.close()

    expect(cancelReason).toBe("reader done")
    expect(disposed).toBe(1)
  })

  test("preserves a source cancellation failure for the response consumer", async () => {
    const failure = new Error("source cancellation failed")
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () =>
          new Response(
            new ReadableStream({
              cancel() {
                throw failure
              },
            }),
          ),
        dispose() {
          disposed++
        },
      }),
    })
    const response = requireResponse(lifecycle.get().app.fetch(request()))
    const reader = response.body!.getReader()

    expect(await rejection(reader.cancel("consumer stopped"))).toBe(failure)
    await lifecycle.close()
    expect(disposed).toBe(1)
  })

  test("preserves fetch response metadata while wrapping its body", async () => {
    const source = await fetch("data:text/plain,hello")
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => source,
        dispose() {},
      }),
    })
    const response = requireResponse(lifecycle.get().app.fetch(request()))

    expect(response.url).toBe(source.url)
    expect(response.redirected).toBe(source.redirected)
    expect(response.type).toBe(source.type)
    const clone = response.clone()
    expect(clone.url).toBe(source.url)
    expect(clone.redirected).toBe(source.redirected)
    expect(clone.type).toBe(source.type)
    expect(await Promise.all([response.text(), clone.text()])).toEqual(["hello", "hello"])
    await lifecycle.close()
  })

  test("releases its lease when a handler returns an already-locked body", async () => {
    const source = new Response(
      new ReadableStream({
        start(controller) {
          controller.close()
        },
      }),
    )
    const reader = source.body?.getReader()
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => source,
        dispose() {
          disposed++
        },
      }),
    })

    expect(() => lifecycle.get().app.fetch(request())).toThrow()
    await lifecycle.close()
    expect(disposed).toBe(1)
    reader?.releaseLock()
  })

  test("waits for original reader cancellation when response wrapping fails", async () => {
    const cancellation = Promise.withResolvers<void>()
    const failure = new Error("response metadata failed")
    let cancelStarted = false
    let disposed = 0
    const source = new Response(
      new ReadableStream({
        cancel() {
          cancelStarted = true
          return cancellation.promise
        },
      }),
    )
    // The proxy keeps a genuine response/body while forcing metadata access to
    // fail after the lifecycle manager has locked the original reader.
    const invalid = new Proxy(source, {
      get(target, property) {
        if (property === "statusText") throw failure
        return Reflect.get(target, property, target)
      },
    })
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => invalid,
        dispose() {
          disposed++
        },
      }),
    })

    expect(() => lifecycle.get().app.fetch(request())).toThrow(failure)
    expect(cancelStarted).toBe(true)
    const closing = lifecycle.close({ timeoutMs: 500 })
    expect(disposed).toBe(0)
    cancellation.resolve()
    await closing
    expect(disposed).toBe(1)
  })

  test("lets reader cancellation own release after a pending pull observes EOF", async () => {
    const cancellation = Promise.withResolvers<void>()
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () =>
          new Response(
            new ReadableStream({
              cancel: () => cancellation.promise,
            }),
          ),
        dispose() {
          disposed++
        },
      }),
      naturalDrainMs: 0,
      cancellationDrainMs: 0,
    })
    const handle = lifecycle.get()
    const response = requireResponse(handle.app.fetch(request()))
    const pendingRead = response.body!.getReader().read()

    expect(await rejection(lifecycle.close({ timeoutMs: 5 }))).toBeInstanceOf(DefaultServerCloseTimeoutError)
    expect(disposed).toBe(1)
    expect(lifecycle.loaded()).toBe(true)
    expect(lifecycle.get()).toBe(handle)

    cancellation.resolve()
    expect(await pendingRead).toEqual({ value: undefined, done: true })
    await handle.dispose({ timeoutMs: 500 })
    expect(lifecycle.loaded()).toBe(false)
  })

  test("caches concurrent close and inner dispose exactly once", async () => {
    const disposal = Promise.withResolvers<void>()
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => new Response(null, { status: 204 }),
        dispose() {
          disposed++
          return disposal.promise
        },
      }),
    })
    const handle = lifecycle.get()
    const one = lifecycle.close({ timeoutMs: 500 })
    const two = lifecycle.close({ timeoutMs: 500 })
    const three = handle.dispose({ timeoutMs: 500 })

    expect(disposed).toBe(1)
    disposal.resolve()
    await Promise.all([one, two, three])
    await handle.dispose({ timeoutMs: 0 })
    expect(disposed).toBe(1)
  })

  test("publishes close identity before a synchronous disposer re-enters", async () => {
    const reentered: Promise<void>[] = []
    let handle: DefaultServerHandle | undefined
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => new Response(null, { status: 204 }),
        dispose() {
          disposed++
          if (handle) reentered.push(handle.dispose())
        },
      }),
    })
    handle = lifecycle.get()

    await lifecycle.close()
    await Promise.all(reentered)
    expect(disposed).toBe(1)
    expect(lifecycle.loaded()).toBe(false)
  })

  test("permits a fresh generation only after restartable close completes", async () => {
    const firstDisposal = Promise.withResolvers<void>()
    const disposed: number[] = []
    let created = 0
    const lifecycle = createDefaultServerLifecycle({
      create: (): DefaultServerResource => {
        const id = ++created
        return {
          fetch: () => new Response(String(id)),
          dispose() {
            disposed.push(id)
            return id === 1 ? firstDisposal.promise : Promise.resolve()
          },
        }
      },
    })
    const first = lifecycle.get()
    const closing = lifecycle.close({ timeoutMs: 500 })

    expect(lifecycle.get()).toBe(first)
    expect(requireResponse(lifecycle.get().app.fetch(request())).status).toBe(503)
    firstDisposal.resolve()
    await closing

    const second = lifecycle.get()
    expect(second).not.toBe(first)
    expect(await requireResponse(second.app.fetch(request())).text()).toBe("2")
    await first.dispose()
    expect(disposed).toEqual([1])
    expect(lifecycle.get()).toBe(second)

    await lifecycle.close()
    expect(disposed).toEqual([1, 2])
  })

  test("a permanent shutdown never reopens the resource", async () => {
    let created = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => {
        created++
        return {
          fetch: () => new Response("ok"),
          dispose() {},
        }
      },
    })
    lifecycle.get()
    await lifecycle.shutdown()

    expect(lifecycle.loaded()).toBe(false)
    expect(requireResponse(lifecycle.get().app.fetch(request())).status).toBe(503)
    expect(requireResponse(lifecycle.get().app.fetch(request())).status).toBe(503)
    expect(created).toBe(1)
  })

  test("enforces one total deadline while an inner dispose remains pending", async () => {
    const disposal = Promise.withResolvers<void>()
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch: () => new Response(null, { status: 204 }),
        dispose() {
          disposed++
          return disposal.promise
        },
      }),
    })
    const handle = lifecycle.get()

    expect(await rejection(lifecycle.close({ timeoutMs: 0 }))).toBeInstanceOf(DefaultServerCloseTimeoutError)
    expect(disposed).toBe(1)
    expect(lifecycle.get()).toBe(handle)
    expect(requireResponse(handle.app.fetch(request())).status).toBe(503)

    disposal.resolve()
    await handle.dispose()
    expect(lifecycle.loaded()).toBe(false)
  })

  test("does not reopen after disposal while a cancelled request is still settling", async () => {
    const response = Promise.withResolvers<Response>()
    let created = 0
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => {
        created++
        return {
          fetch: () => response.promise,
          dispose() {
            disposed++
          },
        }
      },
      naturalDrainMs: 0,
      cancellationDrainMs: 0,
    })
    const handle = lifecycle.get()
    const admitted = handle.app.fetch(request())

    expect(await rejection(lifecycle.close({ timeoutMs: 5 }))).toBeInstanceOf(DefaultServerCloseTimeoutError)
    expect(disposed).toBe(1)
    expect(lifecycle.loaded()).toBe(true)
    expect(lifecycle.get()).toBe(handle)
    expect(requireResponse(handle.app.fetch(request())).status).toBe(503)
    expect(created).toBe(1)

    response.resolve(new Response(null, { status: 204 }))
    await admitted
    await handle.dispose({ timeoutMs: 500 })
    expect(lifecycle.loaded()).toBe(false)
  })

  test("keeps a generation fenced when its inner dispose rejects", async () => {
    const failure = new Error("dispose failed")
    let created = 0
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => {
        created++
        return {
          fetch: () => new Response("ok"),
          dispose() {
            disposed++
            throw failure
          },
        }
      },
    })
    const handle = lifecycle.get()

    expect(await rejection(lifecycle.close())).toBe(failure)
    expect(lifecycle.loaded()).toBe(true)
    expect(lifecycle.get()).toBe(handle)
    expect(requireResponse(handle.app.fetch(request())).status).toBe(503)
    expect(await rejection(handle.dispose({ timeoutMs: 0 }))).toBe(failure)
    expect(created).toBe(1)
    expect(disposed).toBe(1)
  })

  test("releases a synchronously throwing handler before disposal", async () => {
    const failure = new Error("handler failed")
    let disposed = 0
    const lifecycle = createDefaultServerLifecycle({
      create: () => ({
        fetch() {
          throw failure
        },
        dispose() {
          disposed++
        },
      }),
    })

    expect(() => lifecycle.get().app.fetch(request())).toThrow(failure)
    await lifecycle.close()
    expect(disposed).toBe(1)
  })
})
