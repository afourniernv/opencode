import { describe, expect, test } from "bun:test"
import {
  PROCESS_SHUTDOWN_TIMEOUT_MS,
  PROCESS_SHUTDOWN_WATCHDOG_MS,
  shutdownProcess,
  type ProcessShutdownStage,
} from "../../src/cli/process-shutdown"

const complete = { drained: true, flushed: true, closed: true } as const

describe("process shutdown", () => {
  test("keeps the worker watchdog outside the complete process deadline", () => {
    expect(PROCESS_SHUTDOWN_WATCHDOG_MS).toBeGreaterThan(PROCESS_SHUTDOWN_TIMEOUT_MS)
  })

  test("stops admission and disposes producers before Relay", async () => {
    const order: ProcessShutdownStage[] = []
    const result = await shutdownProcess({
      stopServer: async (timeoutMs) => {
        expect(timeoutMs).toBe(1_500)
        order.push("server")
      },
      disposeInstances: async () => {
        order.push("instances")
      },
      disposeRuntime: async () => {
        order.push("runtime")
      },
      shutdownRelay: async (timeoutMs) => {
        expect(timeoutMs).toBe(5_000)
        order.push("relay")
        return complete
      },
    })

    expect(order).toEqual(["server", "instances", "runtime", "relay"])
    expect(result).toEqual({ ok: true, failures: [] })
  })

  test("still attempts Relay after producer cleanup rejects", async () => {
    const order: ProcessShutdownStage[] = []
    const result = await shutdownProcess({
      disposeInstances: async () => {
        order.push("instances")
        throw new Error("instance cleanup failed")
      },
      disposeRuntime: async () => {
        order.push("runtime")
        throw new Error("runtime cleanup failed")
      },
      shutdownRelay: async (timeoutMs) => {
        expect(timeoutMs).toBe(5_000)
        order.push("relay")
        return complete
      },
    })

    expect(order).toEqual(["instances", "runtime", "relay"])
    expect(result).toEqual({ ok: false, failures: ["instances", "runtime"] })
  })

  test("still attempts Relay after producer cleanup times out", async () => {
    const order: ProcessShutdownStage[] = []
    const result = await shutdownProcess({
      disposeRuntime: () => {
        order.push("runtime")
        // Finish after the stage watchdog so the test proves timeout behavior
        // without leaking a permanently pending task into suite teardown.
        return new Promise((resolve) => setTimeout(resolve, 20))
      },
      shutdownRelay: async (timeoutMs) => {
        expect(timeoutMs).toBe(45)
        order.push("relay")
        return complete
      },
      timeoutMs: 100,
      stageBudgets: { runtime: 5, relay: 50 },
    })

    expect(order).toEqual(["runtime", "relay"])
    expect(result).toEqual({ ok: false, failures: ["runtime"] })
  })

  test("reports an incomplete Relay close", async () => {
    const result = await shutdownProcess({
      disposeRuntime: async () => {},
      shutdownRelay: async () => ({ drained: false, flushed: false, closed: false }),
    })

    expect(result).toEqual({ ok: false, failures: ["relay"] })
  })
})
