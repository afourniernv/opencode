import { withTimeout } from "@/util/timeout"

export type ProcessShutdownStage = "server" | "instances" | "runtime" | "relay"

export type RelayShutdownResult = {
  readonly drained: boolean
  readonly flushed: boolean
  readonly closed: boolean
}

export type ProcessShutdownResult = {
  readonly ok: boolean
  readonly failures: ProcessShutdownStage[]
}

type StageBudgets = Readonly<Record<ProcessShutdownStage, number>>

export const PROCESS_SHUTDOWN_TIMEOUT_MS = 16_000
export const PROCESS_SHUTDOWN_WATCHDOG_MS = 18_000
const RELAY_TEARDOWN_TIMEOUT_MS = 5_000
const RELAY_WATCHDOG_SLACK_MS = 250

type ProcessShutdownInput = {
  /** Stop request admission before disposing the state those requests can reach. */
  readonly stopServer?: (timeoutMs: number) => Promise<unknown>
  readonly disposeInstances?: () => Promise<unknown>
  /** Close producer scopes before Relay drains its accepted observations. */
  readonly disposeRuntime: () => Promise<unknown>
  readonly shutdownRelay: (timeoutMs: number) => Promise<RelayShutdownResult>
  readonly timeoutMs?: number
  readonly stageBudgets?: Partial<StageBudgets>
}

const defaults: StageBudgets = {
  server: 1_500,
  instances: 2_500,
  runtime: 5_500,
  // Relay receives its full supported five-second teardown window while the
  // outer watchdog retains enough slack to observe that bounded result.
  relay: RELAY_TEARDOWN_TIMEOUT_MS + RELAY_WATCHDOG_SLACK_MS,
}

/**
 * Bounded process teardown shared by direct CLI and worker entry points.
 *
 * Every stage is fail-open so Relay always gets a final drain/flush/close
 * attempt, even when an earlier producer cleanup rejects or times out.
 */
export async function shutdownProcess(input: ProcessShutdownInput): Promise<ProcessShutdownResult> {
  const failures: ProcessShutdownStage[] = []
  const deadline = performance.now() + (input.timeoutMs ?? PROCESS_SHUTDOWN_TIMEOUT_MS)
  const budgets = { ...defaults, ...input.stageBudgets }

  const attempt = async (
    stage: ProcessShutdownStage,
    action: (timeoutMs: number) => Promise<unknown>,
  ): Promise<void> => {
    const timeoutMs = Math.max(1, Math.min(budgets[stage], deadline - performance.now()))
    try {
      await withTimeout(action(timeoutMs), timeoutMs, `${stage} timed out`)
    } catch {
      failures.push(stage)
    }
  }

  if (input.stopServer) await attempt("server", (timeoutMs) => input.stopServer!(timeoutMs))
  if (input.disposeInstances) await attempt("instances", () => input.disposeInstances!())
  await attempt("runtime", () => input.disposeRuntime())
  await attempt("relay", async (timeoutMs) => {
    const slack = Math.min(RELAY_WATCHDOG_SLACK_MS, Math.floor(timeoutMs / 10))
    const relayTimeoutMs = Math.max(1, Math.min(RELAY_TEARDOWN_TIMEOUT_MS, timeoutMs - slack))
    const result = await input.shutdownRelay(relayTimeoutMs)
    if (!result.drained || !result.flushed || !result.closed) throw new Error("Relay teardown incomplete")
  })

  return { ok: failures.length === 0, failures }
}
