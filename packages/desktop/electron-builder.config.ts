import { execFile } from "node:child_process"
import path from "node:path"
import { fileURLToPath } from "node:url"
import { promisify } from "node:util"

import type { Configuration } from "electron-builder"

const execFileAsync = promisify(execFile)
const packageDir = path.dirname(fileURLToPath(import.meta.url))
const rootDir = path.resolve(packageDir, "../..")
const signScript = path.join(rootDir, "script", "sign-windows.ps1")
// The Electron 42 packaging update briefly installed Linux launchers/icons under
// "opencode-desktop". Keep that hidden desktop entry around so existing GNOME/KDE
// pins still resolve after the canonical app id changes back to ai.opencode.desktop.
const legacyDesktopEntry = path.join(packageDir, "resources", "linux", "opencode-desktop.desktop")
const legacyDesktopEntryFpm = `${legacyDesktopEntry}=/usr/share/applications/opencode-desktop.desktop`

const relayNativePackages = [
  "nemo-relay-node-linux-x64-gnu",
  "nemo-relay-node-linux-arm64-gnu",
  "nemo-relay-node-linux-x64-musl",
  "nemo-relay-node-linux-arm64-musl",
  "nemo-relay-node-darwin-arm64",
  "nemo-relay-node-win32-x64-msvc",
  "nemo-relay-node-win32-arm64-msvc",
] as const

const relayNativePackagesByTarget = new Map<string, (typeof relayNativePackages)[number] | undefined>([
  ["aarch64-apple-darwin", "nemo-relay-node-darwin-arm64"],
  ["x86_64-apple-darwin", undefined],
  ["aarch64-pc-windows-msvc", "nemo-relay-node-win32-arm64-msvc"],
  ["x86_64-pc-windows-msvc", "nemo-relay-node-win32-x64-msvc"],
  ["aarch64-unknown-linux-gnu", "nemo-relay-node-linux-arm64-gnu"],
  ["x86_64-unknown-linux-gnu", "nemo-relay-node-linux-x64-gnu"],
])

const relayNativePackage = (() => {
  const target = process.env.RUST_TARGET
  if (target !== undefined) {
    if (relayNativePackagesByTarget.has(target)) return relayNativePackagesByTarget.get(target)
    throw new Error(`Unsupported RUST_TARGET for Relay desktop packaging: ${target}`)
  }

  const requestedPlatform = process.argv.some((arg) => arg === "--mac" || arg === "-m")
    ? "darwin"
    : process.argv.some((arg) => arg === "--linux" || arg === "-l")
      ? "linux"
      : process.argv.some((arg) => arg === "--win" || arg === "-w")
        ? "win32"
        : process.platform
  const requestedArch = process.argv.includes("--arm64")
    ? "arm64"
    : process.argv.includes("--x64")
      ? "x64"
      : process.arch
  if (requestedPlatform !== process.platform || requestedArch !== process.arch) {
    throw new Error(
      `RUST_TARGET is required when packaging for ${requestedPlatform}-${requestedArch} from ${process.platform}-${process.arch}`,
    )
  }

  if (process.platform === "darwin") return process.arch === "arm64" ? "nemo-relay-node-darwin-arm64" : undefined
  if (process.platform === "linux" && (process.arch === "arm64" || process.arch === "x64"))
    return `nemo-relay-node-linux-${process.arch}-gnu`
  if (process.platform === "win32" && (process.arch === "arm64" || process.arch === "x64"))
    return `nemo-relay-node-win32-${process.arch}-msvc`
  throw new Error(`Unsupported host for Relay desktop packaging: ${process.platform}-${process.arch}`)
})()

const metainfoFpm = (appId: string) =>
  `${path.join(packageDir, "resources", `${appId}.metainfo.xml`)}=/usr/share/metainfo/${appId}.metainfo.xml`

async function signWindows(configuration: { path: string }) {
  if (process.platform !== "win32") return
  if (process.env.GITHUB_ACTIONS !== "true") return

  await execFileAsync(
    "pwsh",
    ["-NoLogo", "-NoProfile", "-ExecutionPolicy", "Bypass", "-File", signScript, configuration.path],
    { cwd: rootDir },
  )
}

const channel = (() => {
  const raw = process.env.OPENCODE_CHANNEL
  if (raw === "dev" || raw === "beta" || raw === "prod") return raw
  return "dev"
})()

const APP_IDS = {
  dev: "ai.opencode.desktop.dev",
  beta: "ai.opencode.desktop.beta",
  prod: "ai.opencode.desktop",
} as const

const getBase = (appId: string): Configuration => ({
  artifactName: "opencode-desktop-${os}-${arch}.${ext}",
  directories: {
    output: "dist",
    buildResources: "resources",
  },
  // Linux launchers are .desktop files, so this is the desktop file name,
  // not just the app id. For prod, app id "ai.opencode.desktop" becomes
  // "ai.opencode.desktop.desktop".
  // https://developer.gnome.org/documentation/guidelines/maintainer/integrating.html
  // https://www.electron.build/docs/linux/
  extraMetadata: {
    desktopName: `${appId}.desktop`,
  },
  files: [
    "out/**/*",
    "resources/**/*",
    "!resources/opencode-cli*",
    ...relayNativePackages.filter((name) => name !== relayNativePackage).map((name) => `!node_modules/${name}/**/*`),
  ],
  // Relay is loaded inside Electron's Node utility process. Native addons
  // cannot be dlopen'd from app.asar, so keep the selected platform package
  // in app.asar.unpacked.
  asarUnpack: ["node_modules/nemo-relay-node-*/**/*.node"],
  extraResources: [
    ...(channel === "dev"
      ? [
          {
            from: "resources/",
            to: "",
            filter: ["opencode-cli*"],
          },
        ]
      : []),
    {
      from: "native/",
      to: "native/",
      filter: ["index.js", "index.d.ts", "build/Release/mac_window.node", "swift-build/**"],
    },
  ],
  mac: {
    category: "public.app-category.developer-tools",
    icon: `resources/icons/icon.icns`,
    hardenedRuntime: true,
    gatekeeperAssess: false,
    entitlements: "resources/entitlements.plist",
    entitlementsInherit: "resources/entitlements.plist",
    notarize: true,
    target: ["dmg", "zip"],
  },
  dmg: {
    sign: true,
  },
  protocols: {
    name: "OpenCode",
    schemes: ["opencode"],
  },
  win: {
    icon: `resources/icons/icon.ico`,
    signtoolOptions: {
      sign: signWindows,
    },
    target: ["nsis"],
    verifyUpdateCodeSignature: false,
  },
  nsis: {
    oneClick: true,
    perMachine: false,
    installerIcon: `resources/icons/icon.ico`,
    installerHeaderIcon: `resources/icons/icon.ico`,
  },
  linux: {
    icon: `resources/icons`,
    category: "Development",
    executableName: appId,
    desktop: {
      entry: {
        // Match the installed .desktop file and hicolor icon basename so
        // Linux shells can associate the running Electron window with its launcher.
        StartupWMClass: appId,
      },
    },
    target: ["AppImage", "deb", "rpm"],
  },
})

function getConfig() {
  const appId = APP_IDS[channel]
  const base = getBase(appId)

  switch (channel) {
    case "dev": {
      return {
        ...base,
        appId,
        productName: "OpenCode Dev",
        deb: { fpm: [metainfoFpm(appId)] },
        rpm: { packageName: "opencode-dev", fpm: [metainfoFpm(appId)] },
      }
    }
    case "beta": {
      return {
        ...base,
        appId,
        productName: "OpenCode Beta",
        protocols: { name: "OpenCode Beta", schemes: ["opencode"] },
        publish: { provider: "github", owner: "anomalyco", repo: "opencode-beta", channel: "latest" },
        deb: { fpm: [metainfoFpm(appId)] },
        rpm: { packageName: "opencode-beta", fpm: [metainfoFpm(appId)] },
      }
    }
    case "prod": {
      return {
        ...base,
        appId,
        productName: "OpenCode",
        protocols: { name: "OpenCode", schemes: ["opencode"] },
        publish: { provider: "github", owner: "anomalyco", repo: "opencode", channel: "latest" },
        deb: { fpm: [metainfoFpm(appId), legacyDesktopEntryFpm] },
        rpm: { packageName: "opencode", fpm: [metainfoFpm(appId), legacyDesktopEntryFpm] },
      }
    }
  }
}

export default getConfig()
