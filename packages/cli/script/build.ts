#!/usr/bin/env bun

import { $ } from "bun"
import { mkdtemp, rm } from "fs/promises"
import { createRequire } from "module"
import { tmpdir } from "os"
import path from "path"
import { fileURLToPath } from "url"
import { Script } from "@opencode-ai/script"
import { createSolidTransformPlugin } from "@opentui/solid/bun-plugin"
import corePkg from "../../core/package.json"
import pkg from "../package.json"
import { modelsData } from "./generate"

const dir = path.resolve(import.meta.dirname, "..")
const binary = "lildax"
process.chdir(dir)

await rm("dist", { recursive: true, force: true })

const singleFlag = process.argv.includes("--single")
const baselineFlag = process.argv.includes("--baseline")
const skipInstall = process.argv.includes("--skip-install")
const sourcemapsFlag = process.argv.includes("--sourcemaps")
const plugin = createSolidTransformPlugin()

type BuildTarget = {
  os: string
  arch: "arm64" | "x64"
  abi?: "musl"
  avx2?: false
}

const relayModule = (target: BuildTarget) => {
  if (target.os === "darwin") return target.arch === "arm64" ? "nemo-relay-node-darwin-arm64" : undefined
  if (target.os === "linux") return `nemo-relay-node-linux-${target.arch}-${target.abi === "musl" ? "musl" : "gnu"}`
  if (target.os === "win32") return `nemo-relay-node-win32-${target.arch}-msvc`
  return undefined
}

const relayShimPath = path.resolve(dir, "lildax-nemo-relay-runtime.gen.ts")
const relayAddonPath = path.resolve(dir, "lildax-nemo-relay.node")
const relayPackageRequire = createRequire(
  createRequire(path.resolve(dir, "../core/package.json")).resolve("nemo-relay-node"),
)

const relaySource = (target: BuildTarget) => {
  const module = relayModule(target)
  if (!module) return undefined
  try {
    return fileURLToPath(import.meta.resolve(module))
  } catch {
    return relayPackageRequire.resolve(module)
  }
}

const relayFiles = async (target: BuildTarget) => {
  const source = relaySource(target)
  if (!source) return {}
  return {
    [relayShimPath]: [
      `import addonPath from ${JSON.stringify("./lildax-nemo-relay.node")} with { type: "file" }`,
      `const relay = require(addonPath)`,
      `export default relay`,
    ].join("\n"),
    [relayAddonPath]: await Bun.file(source).arrayBuffer(),
  }
}

const probeRelayRuntime = async (binaryPath: string, supported: boolean) => {
  const home = await mkdtemp(path.join(tmpdir(), "lildax-relay-build-"))
  try {
    const env = { ...process.env }
    for (const key of Object.keys(env)) {
      if (
        key === "OPENCODE_PURE" ||
        key.startsWith("OPENCODE_NEMO_RELAY") ||
        key.startsWith("NEMO_RELAY_") ||
        key.startsWith("OTEL_")
      )
        delete env[key]
    }
    const child = Bun.spawn([path.resolve(binaryPath), "debug", "nemo-relay"], {
      env: {
        ...env,
        HOME: home,
        USERPROFILE: home,
        XDG_CONFIG_HOME: home,
        APPDATA: home,
        LOCALAPPDATA: home,
        OPENCODE_NEMO_RELAY: "1",
      },
      stdout: "pipe",
      stderr: "pipe",
    })
    const [stdout, stderr, code] = await Promise.all([
      new Response(child.stdout).text(),
      new Response(child.stderr).text(),
      child.exited,
    ])
    if (code !== 0) throw new Error(`debug nemo-relay exited ${code}: ${stderr.trim()}`)
    const marker = '{\n  "service":'
    const start = stdout.lastIndexOf(marker)
    if (start < 0) throw new Error(`debug nemo-relay did not return status JSON: ${stdout.trim()} ${stderr.trim()}`)
    const status = JSON.parse(stdout.slice(start)) as {
      readonly service?: { readonly state?: string; readonly reason?: string }
      readonly process?: { readonly phase?: string }
    }
    if (supported) {
      if (status.service?.state !== "active" || status.process?.phase !== "active")
        throw new Error(`embedded Relay runtime did not activate: ${stdout.trim()}`)
    } else if (status.service?.state !== "unavailable" || status.service.reason !== "unsupported_platform") {
      throw new Error(`unsupported Relay target was not reported explicitly: ${stdout.trim()}`)
    }
  } finally {
    await rm(home, { recursive: true, force: true })
  }
}

const allTargets: BuildTarget[] = [
  { os: "linux", arch: "arm64" },
  { os: "linux", arch: "x64" },
  { os: "linux", arch: "x64", avx2: false },
  { os: "linux", arch: "arm64", abi: "musl" },
  { os: "linux", arch: "x64", abi: "musl" },
  { os: "linux", arch: "x64", abi: "musl", avx2: false },
  { os: "darwin", arch: "arm64" },
  { os: "darwin", arch: "x64" },
  { os: "darwin", arch: "x64", avx2: false },
  { os: "win32", arch: "arm64" },
  { os: "win32", arch: "x64" },
  { os: "win32", arch: "x64", avx2: false },
]

const targets = singleFlag
  ? allTargets.filter((item) => {
      if (item.os !== process.platform || item.arch !== process.arch) return false
      if (item.avx2 === false) return baselineFlag
      return item.abi === undefined
    })
  : allTargets

const relayVersion = corePkg.dependencies["nemo-relay-node"]
const relayNativeModules = [
  ...new Set(allTargets.map(relayModule).filter((item): item is string => item !== undefined)),
]

if (!skipInstall) {
  await $`bun install --os="*" --cpu="*" @opentui/core@${pkg.dependencies["@opentui/core"]}`
  // Cross-builds need every native package available to the build script. The
  // compiler embeds only the one selected by relayModule for each artifact.
  await $`bun install --os="*" --cpu="*" --no-save nemo-relay-node@${relayVersion} ${relayNativeModules.map((module) => `${module}@${relayVersion}`)}`
}

for (const item of targets) {
  const target = [
    binary,
    item.os === "win32" ? "windows" : item.os,
    item.arch,
    item.avx2 === false ? "baseline" : undefined,
    item.abi,
  ]
    .filter(Boolean)
    .join("-")
  const name = target.replace(binary, "cli")
  console.log(`building ${name}`)
  const result = await Bun.build({
    entrypoints: ["./src/index.ts"],
    tsconfig: "./tsconfig.json",
    plugins: [plugin],
    external: ["node-gyp"],
    format: "esm",
    minify: true,
    sourcemap: sourcemapsFlag ? "linked" : "none",
    splitting: true,
    compile: {
      autoloadBunfig: false,
      autoloadDotenv: false,
      autoloadTsconfig: true,
      autoloadPackageJson: true,
      target: target.replace(binary, "bun") as Bun.Build.CompileTarget,
      outfile: `./dist/${name}/bin/${binary}`,
      execArgv: [`--user-agent=${binary}/${Script.version}`, "--use-system-ca", "--"],
      windows: {},
    },
    files: await relayFiles(item),
    define: {
      OPENCODE_VERSION: `'${Script.version}'`,
      OPENCODE_CLI_NAME: `'${binary}'`,
      OPENCODE_MODELS_DEV: modelsData,
      OPENCODE_CHANNEL: `'${Script.channel}'`,
      OPENCODE_NEMO_RELAY_BUNDLED_MODULE: relayModule(item) ? JSON.stringify(relayShimPath) : "undefined",
      OPENCODE_LIBC: item.os === "linux" ? `'${item.abi ?? "glibc"}'` : "undefined",
      // FFF_LIBC selects the fff native lib variant: "musl" or "gnu".
      FFF_LIBC: item.os === "linux" ? `'${item.abi ?? "gnu"}'` : "undefined",
      ...(item.os === "linux" ? { "process.env.OPENTUI_LIBC": JSON.stringify(item.abi ?? "glibc") } : {}),
    },
  })

  if (!result.success) {
    for (const log of result.logs) console.error(log)
    process.exit(1)
  }

  const relayNativeSource = relaySource(item)
  await Bun.write(`./dist/${name}/bin/LICENSE`, Bun.file(path.resolve(dir, "../../LICENSE")))
  if (relayNativeSource)
    await Bun.write(
      `./dist/${name}/bin/LICENSE.nemo-relay`,
      Bun.file(path.join(path.dirname(relayNativeSource), "LICENSE")),
    )

  if (item.os === process.platform && item.arch === process.arch && !item.abi) {
    const binaryPath = `./dist/${name}/bin/${binary}${item.os === "win32" ? ".exe" : ""}`
    console.log(`Running packaged Relay activation test: ${binaryPath} debug nemo-relay`)
    await probeRelayRuntime(binaryPath, relayModule(item) !== undefined)
    console.log("Packaged Relay activation test passed")
  }

  await Bun.write(
    `./dist/${name}/package.json`,
    JSON.stringify(
      {
        name: `@opencode-ai/${name}`,
        version: Script.version,
        license: relayNativeSource ? "MIT AND Apache-2.0" : "MIT",
        repository: { type: "git", url: "git+https://github.com/anomalyco/opencode.git" },
        os: [item.os],
        cpu: [item.arch],
      },
      null,
      2,
    ),
  )
}
