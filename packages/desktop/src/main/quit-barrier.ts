type QuitEvent = {
  preventDefault(): void
}

export function createQuitBarrier(input: {
  stop: () => Promise<void>
  resume: () => void
  onError: (error: unknown) => void
}) {
  let stopped = false
  let stopping: Promise<void> | undefined
  let resuming = false

  const stop = () => {
    if (stopped) return Promise.resolve()
    if (stopping) return stopping

    stopping = new Promise<void>((resolve) => resolve(input.stop())).finally(() => {
      stopped = true
    })
    return stopping
  }

  return {
    stop,
    onQuit(event: QuitEvent) {
      if (stopped) return

      event.preventDefault()
      if (resuming) return
      resuming = true

      void stop().catch(input.onError).finally(input.resume)
    },
  }
}
