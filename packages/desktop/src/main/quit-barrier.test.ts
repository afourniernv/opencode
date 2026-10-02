import { expect, test } from "bun:test"
import { createQuitBarrier } from "./quit-barrier"

test("waits for sidecar shutdown before resuming an ordinary quit", async () => {
  let finishStop!: () => void
  const stopped = new Promise<void>((resolve) => {
    finishStop = resolve
  })
  let stopCalls = 0
  let quitCalls = 0
  let resumeQuit!: () => void
  const quitResumed = new Promise<void>((resolve) => {
    resumeQuit = resolve
  })
  const barrier = createQuitBarrier({
    stop: () => {
      stopCalls++
      return stopped
    },
    resume: () => {
      quitCalls++
      resumeQuit()
    },
    onError: () => {},
  })
  let prevented = 0

  barrier.onQuit({ preventDefault: () => prevented++ })
  barrier.onQuit({ preventDefault: () => prevented++ })

  expect(prevented).toBe(2)
  expect(stopCalls).toBe(1)
  expect(quitCalls).toBe(0)

  finishStop()
  await quitResumed

  expect(quitCalls).toBe(1)

  barrier.onQuit({ preventDefault: () => prevented++ })
  expect(prevented).toBe(2)
  expect(stopCalls).toBe(1)
  expect(quitCalls).toBe(1)
})

test("shares shutdown started by relaunch, signal, or updater paths", async () => {
  let finishStop!: () => void
  const stopped = new Promise<void>((resolve) => {
    finishStop = resolve
  })
  let stopCalls = 0
  const barrier = createQuitBarrier({
    stop: () => {
      stopCalls++
      return stopped
    },
    resume: () => {},
    onError: () => {},
  })

  const first = barrier.stop()
  const second = barrier.stop()

  expect(second).toBe(first)
  expect(stopCalls).toBe(1)

  finishStop()
  await first

  let prevented = false
  barrier.onQuit({ preventDefault: () => (prevented = true) })
  expect(prevented).toBe(false)
  expect(stopCalls).toBe(1)
})

test("reports shutdown failures and still resumes quitting", async () => {
  const failure = new Error("shutdown failed")
  let reported: unknown
  let resumeQuit!: () => void
  const quitResumed = new Promise<void>((resolve) => {
    resumeQuit = resolve
  })
  const barrier = createQuitBarrier({
    stop: () => {
      throw failure
    },
    resume: resumeQuit,
    onError: (error) => (reported = error),
  })
  let prevented = false

  barrier.onQuit({ preventDefault: () => (prevented = true) })
  await quitResumed

  expect(prevented).toBe(true)
  expect(reported).toBe(failure)

  prevented = false
  barrier.onQuit({ preventDefault: () => (prevented = true) })
  expect(prevented).toBe(false)
})
