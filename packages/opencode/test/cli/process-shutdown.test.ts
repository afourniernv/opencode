import { describe, expect, test } from "bun:test"
import { shutdownProcess, type ProcessShutdownStage } from "../../src/cli/process-shutdown"

const complete = { drained: true, flushed: true, closed: true } as const

describe("process shutdown", () => {
  test("stops admission and disposes producers before Relay", async () => {
    const order: ProcessShutdownStage[] = []
    const result = await shutdownProcess({
      stopServer: async () => {
        order.push("server")
      },
      disposeInstances: async () => {
        order.push("instances")
      },
      disposeRuntime: async () => {
        order.push("runtime")
      },
      shutdownRelay: async (timeoutMs) => {
        expect(timeoutMs).toBeGreaterThan(0)
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
      shutdownRelay: async () => {
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
        return new Promise(() => {})
      },
      shutdownRelay: async () => {
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
