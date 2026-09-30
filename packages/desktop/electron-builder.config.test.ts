import { expect, test } from "bun:test"
import type { Configuration } from "electron-builder"

const legacyDesktopEntry = "resources/linux/opencode-desktop.desktop"
const relayNativePackages = [
  "nemo-relay-node-linux-x64-gnu",
  "nemo-relay-node-linux-arm64-gnu",
  "nemo-relay-node-linux-x64-musl",
  "nemo-relay-node-linux-arm64-musl",
  "nemo-relay-node-darwin-arm64",
  "nemo-relay-node-win32-x64-msvc",
  "nemo-relay-node-win32-arm64-msvc",
] as const

const channels = [
  { channel: "dev", appId: "ai.opencode.desktop.dev" },
  { channel: "beta", appId: "ai.opencode.desktop.beta" },
  { channel: "prod", appId: "ai.opencode.desktop" },
] as const

for (const channel of channels) {
  test(`uses one Linux desktop identity for ${channel.channel}`, async () => {
    const previous = process.env.OPENCODE_CHANNEL
    process.env.OPENCODE_CHANNEL = channel.channel

    const module = await import(`./electron-builder.config.ts?channel=${channel.channel}`)
    const config = module.default as Configuration

    if (previous === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previous

    expect(config.appId).toBe(channel.appId)
    expect(config.extraMetadata?.desktopName).toBe(`${channel.appId}.desktop`)
    expect(config.linux?.executableName).toBe(channel.appId)
    expect(config.linux?.desktop?.entry?.StartupWMClass).toBe(channel.appId)
    expect(config.deb?.fpm).toContainEqual(expect.stringContaining(`/usr/share/metainfo/${channel.appId}.metainfo.xml`))
    expect(config.rpm?.fpm).toContainEqual(expect.stringContaining(`/usr/share/metainfo/${channel.appId}.metainfo.xml`))
  })
}

test("keeps a hidden prod launcher for old Linux pins", async () => {
  const previous = process.env.OPENCODE_CHANNEL
  process.env.OPENCODE_CHANNEL = "prod"

  const module = await import("./electron-builder.config.ts?compat=prod")
  const config = module.default as Configuration

  if (previous === undefined) delete process.env.OPENCODE_CHANNEL
  else process.env.OPENCODE_CHANNEL = previous

  expect(
    config.deb?.fpm?.some((entry) =>
      entry.endsWith("opencode-desktop.desktop=/usr/share/applications/opencode-desktop.desktop"),
    ),
  ).toBe(true)
  expect(
    config.rpm?.fpm?.some((entry) =>
      entry.endsWith("opencode-desktop.desktop=/usr/share/applications/opencode-desktop.desktop"),
    ),
  ).toBe(true)

  const desktop = await Bun.file(legacyDesktopEntry).text()
  expect(desktop).toContain("Exec=/opt/OpenCode/ai.opencode.desktop %U")
  expect(desktop).toContain("Icon=ai.opencode.desktop")
  expect(desktop).toContain("StartupWMClass=ai.opencode.desktop")
  expect(desktop).toContain("NoDisplay=true")
})

test("bundles the CLI outside the dev app archive", async () => {
  const previous = process.env.OPENCODE_CHANNEL
  process.env.OPENCODE_CHANNEL = "dev"
  const module = await import("./electron-builder.config.ts?cli-resource")
  const config = module.default as Configuration
  if (previous === undefined) delete process.env.OPENCODE_CHANNEL
  else process.env.OPENCODE_CHANNEL = previous

  expect(config.files).toContain("!resources/opencode-cli*")
  expect(config.extraResources).toContainEqual({
    from: "resources/",
    to: "",
    filter: ["opencode-cli*"],
  })
})

for (const channel of ["beta", "prod"] as const) {
  test(`does not bundle the CLI in ${channel} builds`, async () => {
    const previous = process.env.OPENCODE_CHANNEL
    process.env.OPENCODE_CHANNEL = channel
    const module = await import(`./electron-builder.config.ts?no-cli-resource=${channel}`)
    const config = module.default as Configuration
    if (previous === undefined) delete process.env.OPENCODE_CHANNEL
    else process.env.OPENCODE_CHANNEL = previous

    expect(config.extraResources).not.toContainEqual({
      from: "resources/",
      to: "",
      filter: ["opencode-cli*"],
    })
  })
}

const relayTargets = [
  { target: "aarch64-apple-darwin", native: "nemo-relay-node-darwin-arm64" },
  { target: "x86_64-apple-darwin", native: undefined },
  { target: "aarch64-pc-windows-msvc", native: "nemo-relay-node-win32-arm64-msvc" },
  { target: "x86_64-pc-windows-msvc", native: "nemo-relay-node-win32-x64-msvc" },
  { target: "aarch64-unknown-linux-gnu", native: "nemo-relay-node-linux-arm64-gnu" },
  { target: "x86_64-unknown-linux-gnu", native: "nemo-relay-node-linux-x64-gnu" },
] as const

for (const item of relayTargets) {
  test(`packages only the selected Relay addon for ${item.target}`, async () => {
    const previous = process.env.RUST_TARGET
    process.env.RUST_TARGET = item.target

    const module = await import(`./electron-builder.config.ts?relay-target=${item.target}`)
    const config = module.default as Configuration

    if (previous === undefined) delete process.env.RUST_TARGET
    else process.env.RUST_TARGET = previous

    const files = (config.files ?? []).filter((value): value is string => typeof value === "string")
    for (const native of relayNativePackages) {
      const exclusion = `!node_modules/${native}/**/*`
      if (native === item.native) expect(files).not.toContain(exclusion)
      else expect(files).toContain(exclusion)
    }
    expect(config.asarUnpack).toContain("node_modules/nemo-relay-node-*/**/*.node")
  })
}

test("rejects an unrecognized Relay packaging target", async () => {
  const previous = process.env.RUST_TARGET
  process.env.RUST_TARGET = "riscv64-unknown-linux-gnu"

  try {
    await expect(import("./electron-builder.config.ts?relay-target=unsupported")).rejects.toThrow(
      "Unsupported RUST_TARGET for Relay desktop packaging: riscv64-unknown-linux-gnu",
    )
  } finally {
    if (previous === undefined) delete process.env.RUST_TARGET
    else process.env.RUST_TARGET = previous
  }
})

test("requires RUST_TARGET when electron-builder targets another platform", async () => {
  const previousTarget = process.env.RUST_TARGET
  const previousArgv = [...process.argv]
  delete process.env.RUST_TARGET
  process.argv.push(process.platform === "win32" ? "--linux" : "--win")

  try {
    await expect(import("./electron-builder.config.ts?relay-target=cross-platform")).rejects.toThrow(
      "RUST_TARGET is required when packaging",
    )
  } finally {
    process.argv.splice(0, process.argv.length, ...previousArgv)
    if (previousTarget === undefined) delete process.env.RUST_TARGET
    else process.env.RUST_TARGET = previousTarget
  }
})

test("requires RUST_TARGET when electron-builder targets another architecture", async () => {
  const previousTarget = process.env.RUST_TARGET
  const previousArgv = [...process.argv]
  delete process.env.RUST_TARGET
  process.argv.push(process.arch === "arm64" ? "--x64" : "--arm64")

  try {
    await expect(import("./electron-builder.config.ts?relay-target=cross-arch")).rejects.toThrow(
      "RUST_TARGET is required when packaging",
    )
  } finally {
    process.argv.splice(0, process.argv.length, ...previousArgv)
    if (previousTarget === undefined) delete process.env.RUST_TARGET
    else process.env.RUST_TARGET = previousTarget
  }
})
