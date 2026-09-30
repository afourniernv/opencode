#!/usr/bin/env bun

import { $ } from "bun"
import { mkdtemp, rm } from "fs/promises"
import { createRequire } from "module"
import { tmpdir } from "os"
import path from "path"
import { fileURLToPath } from "url"
import { createSolidTransformPlugin } from "@opentui/solid/bun-plugin"

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const dir = path.resolve(__dirname, "..")

process.chdir(dir)

const generated = await import("./generate.ts")

import { Script } from "@opencode-ai/script"
import corePkg from "../../core/package.json"
import pkg from "../package.json"

const singleFlag = process.argv.includes("--single")
const baselineFlag = process.argv.includes("--baseline")
const skipInstall = process.argv.includes("--skip-install")
const sourcemapsFlag = process.argv.includes("--sourcemaps")
const plugin = createSolidTransformPlugin()
const skipEmbedWebUi = process.argv.includes("--skip-embed-web-ui")

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

const relayShimPath = path.resolve(dir, "opencode-nemo-relay-runtime.gen.ts")
const relayAddonPath = path.resolve(dir, "opencode-nemo-relay.node")
const relayPackageRequire = createRequire(
  createRequire(path.resolve(dir, "../core/package.json")).resolve("nemo-relay-node"),
)

const relaySource = (target: BuildTarget) => {
  const module = relayModule(target)
  if (!module) return undefined
  try {
    // Full cross-builds install each target as a direct, no-save build input.
    return fileURLToPath(import.meta.resolve(module))
  } catch {
    // Target-native/Nix installs expose the selected optional package beside
    // core's pinned metapackage rather than beside this build script.
    return relayPackageRequire.resolve(module)
  }
}

const relayFiles = async (target: BuildTarget) => {
  const source = relaySource(target)
  if (!source) return {}
  return {
    [relayShimPath]: [
      `import addonPath from ${JSON.stringify("./opencode-nemo-relay.node")} with { type: "file" }`,
      `const relay = require(addonPath)`,
      `export default relay`,
    ].join("\n"),
    [relayAddonPath]: await Bun.file(source).arrayBuffer(),
  }
}

const probeRelayRuntime = async (binaryPath: string, supported: boolean) => {
  const home = await mkdtemp(path.join(tmpdir(), "opencode-relay-build-"))
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

const createEmbeddedWebUIBundle = async () => {
  console.log(`Building Web UI to embed in the binary`)
  const appDir = path.join(import.meta.dirname, "../../app")
  const dist = path.join(appDir, "dist")
  await $`OPENCODE_CHANNEL=${Script.channel} bun run --cwd ${appDir} build`
  const files = (await Array.fromAsync(new Bun.Glob("**/*").scan({ cwd: dist })))
    .map((file) => file.replaceAll("\\", "/"))
    .filter((file) => !file.endsWith(".map"))
    .sort()
  const imports = files.map((file, i) => {
    const spec = path.relative(dir, path.join(dist, file)).replaceAll("\\", "/")
    return `import file_${i} from ${JSON.stringify(spec.startsWith(".") ? spec : `./${spec}`)} with { type: "file" };`
  })
  const entries = files.map((file, i) => `  ${JSON.stringify(file)}: file_${i},`)
  return [
    `// Import all files as file_$i with type: "file"`,
    ...imports,
    `// Export with original mappings`,
    `export default {`,
    ...entries,
    `}`,
  ].join("\n")
}

const embeddedFileMap = skipEmbedWebUi ? null : await createEmbeddedWebUIBundle()
const treeSitterWorker = await Bun.file(fileURLToPath(import.meta.resolve("@opentui/core/parser.worker"))).text()

const allTargets: BuildTarget[] = [
  {
    os: "linux",
    arch: "arm64",
  },
  {
    os: "linux",
    arch: "x64",
  },
  {
    os: "linux",
    arch: "x64",
    avx2: false,
  },
  {
    os: "linux",
    arch: "arm64",
    abi: "musl",
  },
  {
    os: "linux",
    arch: "x64",
    abi: "musl",
  },
  {
    os: "linux",
    arch: "x64",
    abi: "musl",
    avx2: false,
  },
  {
    os: "darwin",
    arch: "arm64",
  },
  {
    os: "darwin",
    arch: "x64",
  },
  {
    os: "darwin",
    arch: "x64",
    avx2: false,
  },
  {
    os: "win32",
    arch: "arm64",
  },
  {
    os: "win32",
    arch: "x64",
  },
  {
    os: "win32",
    arch: "x64",
    avx2: false,
  },
]

const relayVersion = corePkg.dependencies["nemo-relay-node"]
const relayNativeModules = [
  ...new Set(allTargets.map(relayModule).filter((item): item is string => item !== undefined)),
]

const targets = singleFlag
  ? allTargets.filter((item) => {
      if (item.os !== process.platform || item.arch !== process.arch) {
        return false
      }

      // When building for the current platform, prefer a single native binary by default.
      // Baseline binaries require additional Bun artifacts and can be flaky to download.
      if (item.avx2 === false) {
        return baselineFlag
      }

      // also skip abi-specific builds for the same reason
      if (item.abi !== undefined) {
        return false
      }

      return true
    })
  : allTargets

await $`rm -rf dist`

const binaries: Record<string, string> = {}
if (!skipInstall) {
  await $`bun install --os="*" --cpu="*" @opentui/core@${pkg.dependencies["@opentui/core"]}`
  await $`bun install --os="*" --cpu="*" @parcel/watcher@${pkg.dependencies["@parcel/watcher"]}`
  await $`bun install --os="*" --cpu="*" @ff-labs/fff-bun@${pkg.dependencies["@ff-labs/fff-bun"]}`
  // Relay's platform packages are transitive optional dependencies. Install
  // every target for cross-compilation, then embed only the selected one.
  await $`bun install --os="*" --cpu="*" --no-save nemo-relay-node@${relayVersion} ${relayNativeModules.map((module) => `${module}@${relayVersion}`)}`
}
for (const item of targets) {
  const name = [
    pkg.name,
    // changing to win32 flags npm for some reason
    item.os === "win32" ? "windows" : item.os,
    item.arch,
    item.avx2 === false ? "baseline" : undefined,
    item.abi === undefined ? undefined : item.abi,
  ]
    .filter(Boolean)
    .join("-")
  console.log(`building ${name}`)
  await $`mkdir -p dist/${name}/bin`

  const workerPath = "./src/cli/tui/worker.ts"
  const treeSitterWorkerPath = "opentui-tree-sitter-worker.js"
  const bunfsRoot = item.os === "win32" ? "B:/~BUN/root/" : "/$bunfs/root/"

  await Bun.build({
    conditions: ["bun", "node"],
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
      target: name.replace(pkg.name, "bun") as any,
      outfile: `dist/${name}/bin/opencode`,
      execArgv: [`--user-agent=opencode/${Script.version}`, "--use-system-ca", "--"],
      windows: {},
    },
    files: {
      [treeSitterWorkerPath]: treeSitterWorker,
      ...(embeddedFileMap ? { "opencode-web-ui.gen.ts": embeddedFileMap } : {}),
      ...(await relayFiles(item)),
    },
    entrypoints: [
      "./src/index.ts",
      workerPath,
      treeSitterWorkerPath,
      ...(embeddedFileMap ? ["opencode-web-ui.gen.ts"] : []),
    ],
    define: {
      FFF_LIBC: JSON.stringify(item.abi === "musl" ? "musl" : "gnu"),
      OPENCODE_VERSION: `'${Script.version}'`,
      OPENCODE_MODELS_DEV: generated.modelsData,
      OTUI_TREE_SITTER_WORKER_PATH: bunfsRoot + treeSitterWorkerPath,
      OPENCODE_WORKER_PATH: workerPath,
      OPENCODE_CHANNEL: `'${Script.channel}'`,
      OPENCODE_NEMO_RELAY_BUNDLED_MODULE: relayModule(item) ? JSON.stringify(relayShimPath) : "undefined",
      OPENCODE_LIBC: item.os === "linux" ? `'${item.abi ?? "glibc"}'` : "",
      ...(item.os === "linux" ? { "process.env.OPENTUI_LIBC": JSON.stringify(item.abi ?? "glibc") } : {}),
    },
  })

  const relayNativeSource = relaySource(item)
  await Bun.write(`dist/${name}/bin/LICENSE`, Bun.file(path.resolve(dir, "../../LICENSE")))
  if (relayNativeSource)
    await Bun.write(
      `dist/${name}/bin/LICENSE.nemo-relay`,
      Bun.file(path.join(path.dirname(relayNativeSource), "LICENSE")),
    )

  // Smoke test: only run if binary is for current platform
  if (item.os === process.platform && item.arch === process.arch && !item.abi) {
    const binaryPath = `dist/${name}/bin/opencode${item.os === "win32" ? ".exe" : ""}`
    console.log(`Running smoke test: ${binaryPath} --version`)
    try {
      const versionOutput = await $`${binaryPath} --version`.text()
      console.log(`Smoke test passed: ${versionOutput.trim()}`)
      console.log(`Running packaged Relay activation test: ${binaryPath} debug nemo-relay`)
      await probeRelayRuntime(binaryPath, relayModule(item) !== undefined)
      console.log("Packaged Relay activation test passed")
    } catch (e) {
      console.error(`Smoke test failed for ${name}:`, e)
      process.exit(1)
    }
  }

  await $`rm -rf ./dist/${name}/bin/tui`
  await Bun.file(`dist/${name}/package.json`).write(
    JSON.stringify(
      {
        name,
        version: Script.version,
        license: relayNativeSource ? "MIT AND Apache-2.0" : "MIT",
        preferUnplugged: true,
        os: [item.os],
        cpu: [item.arch],
        ...(item.abi ? { libc: [item.abi] } : {}),
      },
      null,
      2,
    ),
  )
  binaries[name] = Script.version
}

if (Script.release) {
  for (const key of Object.keys(binaries)) {
    if (key.includes("linux")) {
      await $`tar -czf ../../${key}.tar.gz *`.cwd(`dist/${key}/bin`)
    } else {
      await $`zip -r ../../${key}.zip *`.cwd(`dist/${key}/bin`)
    }
  }
  await $`gh release upload v${Script.version} ./dist/*.zip ./dist/*.tar.gz --clobber --repo ${process.env.GH_REPO}`
}

export { binaries }
