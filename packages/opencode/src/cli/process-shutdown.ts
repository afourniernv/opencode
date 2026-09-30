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

type ProcessShutdownInput = {
  /** Stop request admission before disposing the state those requests can reach. */
  readonly stopServer?: () => Promise<unknown>
  readonly disposeInstances?: () => Promise<unknown>
  /** Close producer scopes before Relay drains its accepted observations. */
  readonly disposeRuntime: () => Promise<unknown>
  readonly shutdownRelay: (timeoutMs: number) => Promise<RelayShutdownResult>
  readonly timeoutMs?: number
  readonly stageBudgets?: Partial<StageBudgets>
}

const defaults: StageBudgets = {
  server: 1_000,
  instances: 1_500,
  runtime: 2_500,
  relay: 2_000,
}

/**
 * Bounded process teardown shared by direct CLI and worker entry points.
 *
 * Every stage is fail-open so Relay always gets a final drain/flush/close
 * attempt, even when an earlier producer cleanup rejects or times out.
 */
export async function shutdownProcess(input: ProcessShutdownInput): Promise<ProcessShutdownResult> {
  const failures: ProcessShutdownStage[] = []
  const deadline = Date.now() + (input.timeoutMs ?? 7_000)
  const budgets = { ...defaults, ...input.stageBudgets }

  const attempt = async (
    stage: ProcessShutdownStage,
    action: (timeoutMs: number) => Promise<unknown>,
  ): Promise<void> => {
    const timeoutMs = Math.max(1, Math.min(budgets[stage], deadline - Date.now()))
    try {
      await withTimeout(action(timeoutMs), timeoutMs, `${stage} timed out`)
    } catch {
      failures.push(stage)
    }
  }

  if (input.stopServer) await attempt("server", () => input.stopServer!())
  if (input.disposeInstances) await attempt("instances", () => input.disposeInstances!())
  await attempt("runtime", () => input.disposeRuntime())
  await attempt("relay", async (timeoutMs) => {
    const result = await input.shutdownRelay(timeoutMs)
    if (!result.drained || !result.flushed || !result.closed) throw new Error("Relay teardown incomplete")
  })

  return { ok: failures.length === 0, failures }
}
