/**
 * Structured CLI termination request. Throwing keeps control in the root
 * command boundary so process-wide finalizers (including NeMo Relay) run.
 */
export class ExitRequested extends Error {
  constructor(readonly code: number) {
    super(`CLI requested exit ${code}`)
    this.name = "ExitRequested"
  }
}

export function requestExit(code = 0): never {
  throw new ExitRequested(code)
}
