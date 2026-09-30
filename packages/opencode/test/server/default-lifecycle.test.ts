import { afterEach, describe, expect, test } from "bun:test"
import { HttpApiApp } from "@/server/routes/instance/httpapi/server"
import { Server } from "@/server/server"

describe("default in-process server lifecycle", () => {
  afterEach(async () => {
    await Server.disposeDefault()
  })

  test("disposal resets both lazy wrappers before reuse", async () => {
    const first = Server.Default()

    expect(Server.Default.loaded()).toBe(true)
    expect(HttpApiApp.webHandler.loaded()).toBe(true)

    await Server.disposeDefault()

    expect(Server.Default.loaded()).toBe(false)
    expect(HttpApiApp.webHandler.loaded()).toBe(false)

    const second = Server.Default()
    expect(second).not.toBe(first)
    expect(Server.Default.loaded()).toBe(true)
    expect(HttpApiApp.webHandler.loaded()).toBe(true)
  })
})
