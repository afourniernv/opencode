import { afterEach, describe, expect, test } from "bun:test"
import { HttpApiApp } from "@/server/routes/instance/httpapi/server"
import { Server } from "@/server/server"

describe("default in-process server lifecycle", () => {
  afterEach(async () => {
    await Server.disposeDefault()
    if (HttpApiApp.webHandler.loaded()) {
      await HttpApiApp.webHandler().dispose()
      HttpApiApp.webHandler.reset()
    }
  })

  test("disposal replaces only its owned handler generation", async () => {
    const standalone = HttpApiApp.webHandler()
    const first = Server.Default()

    expect(Server.Default.loaded()).toBe(true)
    expect(HttpApiApp.webHandler.loaded()).toBe(true)

    await Server.disposeDefault()

    expect(Server.Default.loaded()).toBe(false)
    expect(HttpApiApp.webHandler.loaded()).toBe(true)
    expect(HttpApiApp.webHandler()).toBe(standalone)

    const second = Server.Default()
    expect(second).not.toBe(first)
    expect(Server.Default.loaded()).toBe(true)
    expect(HttpApiApp.webHandler.loaded()).toBe(true)
    expect(HttpApiApp.webHandler()).toBe(standalone)
  })
})
