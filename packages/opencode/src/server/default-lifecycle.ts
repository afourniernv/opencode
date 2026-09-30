export type DefaultServerCloseOptions = {
  /** Total monotonic deadline for draining requests and disposing the resource. */
  readonly timeoutMs?: number
}

export type DefaultServerResource = {
  fetch(request: Request): Response | Promise<Response>
  dispose(): void | Promise<void>
}

export type DefaultServerApp = {
  fetch(request: Request): Response | Promise<Response>
  request(input: string | URL | Request, init?: RequestInit): Response | Promise<Response>
}

export type DefaultServerHandle = {
  readonly app: DefaultServerApp
  /** Close only the generation represented by this handle. */
  dispose(options?: DefaultServerCloseOptions): Promise<void>
}

export type DefaultServerLifecycle = {
  get(): DefaultServerHandle
  loaded(): boolean
  /** Close the current generation and allow a later call to get() to create another. */
  close(options?: DefaultServerCloseOptions): Promise<void>
  /** Permanently fence admission, then close the current generation. */
  shutdown(options?: DefaultServerCloseOptions): Promise<void>
}

export type DefaultServerLifecycleOptions = {
  readonly create: () => DefaultServerResource
  readonly unavailable?: () => Response
  readonly defaultTimeoutMs?: number
  readonly naturalDrainMs?: number
  readonly cancellationDrainMs?: number
}

export class DefaultServerCloseTimeoutError extends Error {
  override readonly name = "DefaultServerCloseTimeoutError"

  constructor(readonly timeoutMs: number) {
    super(`Default in-process server did not close within ${timeoutMs}ms`)
  }
}

type RequestLease = {
  readonly request: Request
  readonly signal: AbortSignal
  readonly released: () => boolean
  release(): void
  complete(): void
  attach(reader: ReadableStreamDefaultReader<Uint8Array>): void
  cancel(reason: unknown): Promise<void>
}

type Generation = {
  readonly resource: DefaultServerResource
  readonly active: Set<RequestLease>
  readonly waiters: Set<() => void>
  readonly handle: DefaultServerHandle
  accepting: boolean
  closeSettled: boolean
  closeCompletion?: Promise<void>
  disposeCompletion?: Promise<void>
}

const DEFAULT_TIMEOUT_MS = 1_500
const DEFAULT_NATURAL_DRAIN_MS = 750
const DEFAULT_CANCELLATION_DRAIN_MS = 250

/**
 * Owns the lifetime of a lazily-created, in-process HTTP handler generation.
 *
 * Admission and request leases are deliberately synchronous. Once close() or
 * shutdown() is called, no request can race through the fence and reach state
 * that teardown has begun disposing.
 */
export function createDefaultServerLifecycle(options: DefaultServerLifecycleOptions): DefaultServerLifecycle {
  const defaultTimeoutMs = duration(options.defaultTimeoutMs, DEFAULT_TIMEOUT_MS)
  const naturalDrainMs = duration(options.naturalDrainMs, DEFAULT_NATURAL_DRAIN_MS)
  const cancellationDrainMs = duration(options.cancellationDrainMs, DEFAULT_CANCELLATION_DRAIN_MS)
  let current: Generation | undefined
  let terminated = false

  const unavailable = (): Response =>
    options.unavailable?.() ??
    new Response("The in-process server is shutting down", {
      status: 503,
      headers: { "content-type": "text/plain; charset=utf-8", "retry-after": "1" },
    })

  const unavailableApp: DefaultServerApp = {
    fetch: () => unavailable(),
    request: () => unavailable(),
  }
  const unavailableHandle: DefaultServerHandle = {
    app: unavailableApp,
    dispose: async () => {},
  }

  const get = (): DefaultServerHandle => {
    if (terminated) return unavailableHandle
    if (current) return current.handle

    const resource = options.create()
    // The handle closes over this exact generation. An old handle can never
    // consult or dispose whatever generation happens to be current later.
    let generation: Generation
    const app: DefaultServerApp = {
      fetch: (request) => dispatch(generation, request, unavailable),
      request(input, init) {
        return app.fetch(input instanceof Request ? input : new Request(new URL(input, "http://localhost"), init))
      },
    }
    const handle: DefaultServerHandle = {
      app,
      dispose: (closeOptions) => closeGeneration(generation, closeOptions),
    }
    generation = {
      resource,
      active: new Set<RequestLease>(),
      waiters: new Set<() => void>(),
      handle,
      accepting: true,
      closeSettled: false,
    }
    current = generation
    return handle
  }

  const closeGeneration = (generation: Generation, closeOptions?: DefaultServerCloseOptions): Promise<void> => {
    // A completed idempotent close has no remaining work to time out. Returning
    // the cached Promise also preserves its original failure, if any.
    if (generation.closeSettled && generation.closeCompletion) return generation.closeCompletion
    const timeoutMs = duration(closeOptions?.timeoutMs, defaultTimeoutMs)
    const deadline = performance.now() + timeoutMs
    const completion = beginClose(generation, timeoutMs)
    return beforeDeadline(completion, deadline, () => new DefaultServerCloseTimeoutError(timeoutMs))
  }

  const beginClose = (generation: Generation, timeoutMs: number): Promise<void> => {
    // The write is synchronous with close(), before its Promise is returned.
    generation.accepting = false
    if (generation.closeCompletion) return generation.closeCompletion

    // Keep a meaningful share of short caller-provided budgets for both
    // cancellation and inner disposal. The configured values remain caps for
    // normal (longer) shutdowns.
    const naturalBudget = Math.min(naturalDrainMs, timeoutMs * 0.5)
    const cancellationBudget = Math.min(cancellationDrainMs, timeoutMs * 0.25)

    const completion = Promise.withResolvers<void>()
    // Publish the single-flight Promise before any resource/user code can run.
    // A synchronous disposer is allowed to re-enter close without starting a
    // second teardown.
    generation.closeCompletion = completion.promise
    void (async () => {
      let disposed = false
      try {
        if (generation.active.size > 0 && naturalBudget > 0) {
          await waitForDrain(generation, performance.now() + naturalBudget)
        }

        if (generation.active.size > 0) {
          const reason = new Error("Default in-process server is shutting down")
          // Cancellation starts synchronously. Shutdown suppresses reader
          // failures while each lease still releases on either outcome.
          // Teardown must never wait unboundedly on a stream implementation
          // that ignores cancel().
          for (const lease of generation.active) void lease.cancel(reason).catch(() => undefined)
        }

        if (generation.active.size > 0 && cancellationBudget > 0) {
          await waitForDrain(generation, performance.now() + cancellationBudget)
        }

        await disposeOnce(generation)
        // A disposer can return even when a request implementation ignored its
        // abort/cancel signal. Keep this generation fenced until every admitted
        // request actually settles so restartable close cannot overlap it with
        // a fresh service graph. Individual close callers remain bounded by
        // beforeDeadline() while this shared completion continues safely.
        if (generation.active.size > 0) await waitUntilDrained(generation)
        disposed = true
      } finally {
        // A stale completion may settle after a newer generation was created;
        // identity guards keep it from clearing that replacement. A failed
        // inner disposal stays fenced and current: reopening could overlap a
        // fresh service graph with resources that never actually closed.
        if (disposed && current === generation) current = undefined
      }
    })().then(
      () => {
        generation.closeSettled = true
        completion.resolve()
      },
      (error) => {
        generation.closeSettled = true
        completion.reject(error)
      },
    )

    return completion.promise
  }

  const close = (closeOptions?: DefaultServerCloseOptions): Promise<void> => {
    const generation = current
    if (!generation) return Promise.resolve()
    return closeGeneration(generation, closeOptions)
  }

  const shutdown = (closeOptions?: DefaultServerCloseOptions): Promise<void> => {
    // This is intentionally irreversible and synchronous: even an immediate
    // get() in the same turn observes the permanent admission fence.
    terminated = true
    return close(closeOptions)
  }

  return {
    get,
    loaded: () => current !== undefined,
    close,
    shutdown,
  }
}

function dispatch(generation: Generation, request: Request, unavailable: () => Response): Response | Promise<Response> {
  if (!generation.accepting) return unavailable()

  const lease = acquire(generation, request)
  let result: Response | Promise<Response>
  try {
    result = generation.resource.fetch(lease.request)
  } catch (error) {
    lease.release()
    throw error
  }

  if (result instanceof Response) return retainBody(result, lease)
  return Promise.resolve(result).then(
    (response) => retainBody(response, lease),
    (error) => {
      lease.release()
      throw error
    },
  )
}

function acquire(generation: Generation, request: Request): RequestLease {
  const controller = new AbortController()
  const forwardAbort = () => controller.abort(request.signal.reason)
  if (request.signal.aborted) forwardAbort()
  else request.signal.addEventListener("abort", forwardAbort, { once: true })

  let cloned: Request
  try {
    cloned = new Request(request, { signal: controller.signal })
  } catch (error) {
    request.signal.removeEventListener("abort", forwardAbort)
    throw error
  }

  let released = false
  let reader: ReadableStreamDefaultReader<Uint8Array> | undefined
  let cancellation: Promise<void> | undefined
  let cancelRequested = false
  let cancelReason: unknown

  const release = () => {
    if (released) return
    released = true
    request.signal.removeEventListener("abort", forwardAbort)
    generation.active.delete(lease)
    if (generation.active.size !== 0) return
    for (const notify of generation.waiters) notify()
    generation.waiters.clear()
  }

  const cancelReader = (): Promise<void> => {
    if (!reader) return Promise.resolve()
    if (!cancellation) {
      try {
        cancellation = Promise.resolve(reader.cancel(cancelReason))
      } catch (error) {
        cancellation = Promise.reject(error)
      }
      // Preserve cancellation failures for the response consumer while still
      // releasing the lifecycle lease on either outcome.
      void cancellation.then(release, release)
    }
    return cancellation
  }

  const lease: RequestLease = {
    request: cloned,
    signal: controller.signal,
    released: () => released,
    release,
    complete() {
      // reader.cancel() resolves pending reads with done=true before its own
      // cancellation/finalizer Promise necessarily settles. Once cancellation
      // starts, that Promise exclusively owns release.
      if (!cancelRequested) release()
    },
    attach(value) {
      reader = value
      if (cancelRequested) void cancelReader().catch(() => undefined)
    },
    cancel(reason) {
      cancelRequested = true
      cancelReason ??= reason
      if (!controller.signal.aborted) controller.abort(reason)
      return cancelReader()
    },
  }

  generation.active.add(lease)
  return lease
}

function retainBody(response: Response, lease: RequestLease): Response {
  if (!response.body) {
    lease.release()
    return response
  }

  let reader: ReadableStreamDefaultReader<Uint8Array>
  try {
    reader = response.body.getReader()
  } catch (error) {
    lease.release()
    throw error
  }
  lease.attach(reader)
  try {
    const body = new ReadableStream<Uint8Array>({
      async pull(controller) {
        try {
          const chunk = await reader.read()
          if (chunk.done) {
            controller.close()
            lease.complete()
            return
          }
          controller.enqueue(chunk.value)
        } catch (error) {
          controller.error(error)
          lease.complete()
        }
      },
      async cancel(reason) {
        await lease.cancel(reason)
      },
    })

    return preserveResponseMetadata(
      new Response(body, {
        status: response.status,
        statusText: response.statusText,
        headers: response.headers,
      }),
      response,
    )
  } catch (error) {
    // Once the original body is locked to our reader, best-effort cancellation
    // prevents its producer from being stranded if wrapping itself fails. The
    // cancellation completion owns release so disposal cannot overtake an
    // asynchronous stream finalizer.
    void lease.cancel(error).catch(() => undefined)
    throw error
  }
}

function preserveResponseMetadata(response: Response, source: Response): Response {
  const clone = response.clone.bind(response)
  Object.defineProperties(response, {
    url: { configurable: true, value: source.url },
    redirected: { configurable: true, value: source.redirected },
    type: { configurable: true, value: source.type },
    clone: {
      configurable: true,
      value: () => preserveResponseMetadata(clone(), source),
    },
  })
  return response
}

function disposeOnce(generation: Generation): Promise<void> {
  if (generation.disposeCompletion) return generation.disposeCompletion
  const completion = Promise.withResolvers<void>()
  generation.disposeCompletion = completion.promise
  try {
    void Promise.resolve(generation.resource.dispose()).then(completion.resolve, completion.reject)
  } catch (error) {
    completion.reject(error)
  }
  return completion.promise
}

function waitForDrain(generation: Generation, deadline: number): Promise<boolean> {
  if (generation.active.size === 0) return Promise.resolve(true)
  const remaining = deadline - performance.now()
  if (remaining <= 0) return Promise.resolve(false)

  return new Promise((resolve) => {
    let settled = false
    const finish = (drained: boolean) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      generation.waiters.delete(onDrain)
      resolve(drained)
    }
    const onDrain = () => finish(true)
    const timer = setTimeout(() => finish(false), remaining)
    generation.waiters.add(onDrain)
    // Recheck after registration so a release between the first check and the
    // waiter insertion cannot strand teardown until its timeout.
    if (generation.active.size === 0) finish(true)
  })
}

function waitUntilDrained(generation: Generation): Promise<void> {
  if (generation.active.size === 0) return Promise.resolve()

  return new Promise((resolve) => {
    const onDrain = () => resolve()
    generation.waiters.add(onDrain)
    if (generation.active.size !== 0) return
    generation.waiters.delete(onDrain)
    resolve()
  })
}

function beforeDeadline<T>(promise: Promise<T>, deadline: number, timeout: () => Error): Promise<T> {
  const remaining = deadline - performance.now()
  if (remaining <= 0) {
    // The close continues in the background so a later caller can observe its
    // cached completion. Attach a rejection handler even though this caller's
    // deadline is already exhausted.
    void promise.catch(() => {})
    return Promise.reject(timeout())
  }

  return new Promise<T>((resolve, reject) => {
    let settled = false
    const succeed = (value: T) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      resolve(value)
    }
    const fail = (error: unknown) => {
      if (settled) return
      settled = true
      clearTimeout(timer)
      reject(error)
    }
    const timer = setTimeout(() => fail(timeout()), remaining)
    promise.then(succeed, fail)
  })
}

function duration(value: number | undefined, fallback: number): number {
  if (value === undefined) return fallback
  if (!Number.isFinite(value)) return fallback
  return Math.max(0, value)
}
