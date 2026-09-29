# NeMo Relay observability spike

Status: experimental fork work, not an OpenCode compatibility promise.

## Revision contract

This spike was reviewed against:

- OpenCode `7945de208964a49300d7f770d1a71d078db9a4c4` (version 1.18.33).
- NeMo Relay 0.9.3, commit `52ea6c06d940342c8b20281389335bf60b70b6e0`.
- Bun 1.3.14 on macOS arm64.

Re-audit the attachment seams and packaging matrix when either repository pin
changes.

## Activation

Relay is opt-in and runs in-process. Enable the adapter with:

```text
OPENCODE_NEMO_RELAY=1
```

An optional application configuration can be selected explicitly:

```text
OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml
```

The path is passed to Relay as `additionalPluginsToml`; OpenCode does not add a
repository-local discovery rule. `OPENCODE_NEMO_RELAY_RUNTIME_MODULE` exists for
development and packaged-runtime experiments. The default module is
`nemo-relay-node`.

When the enable flag is absent or false, the native module is not imported.
Loading, configuration, and metric-export failures fail open. The adapter
exposes only bounded `disabled`, `active`, or coarse `unavailable` state and
does not place paths, loader errors, or secrets in metrics.

## Implemented ownership seams

Phase one observes completed host operations. It does not give Relay ownership
of model or tool callbacks.

| Host path      | Owner seam                                      | Current coverage                                                                                                                                          |
| -------------- | ----------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| V1 model calls | `packages/opencode/src/session/llm.ts`          | One logical-call observation when a native, AI SDK, or workflow stream terminates. Title, summary, and compaction are classified from the selected agent. |
| V1 tools       | `packages/opencode/src/session/processor.ts`    | Exactly one observation when the normalized tool state completes, fails, is blocked, or is aborted. Provider-executed tools do not claim local duration.  |
| V2 model calls | `packages/core/src/session/runner/llm.ts`       | One logical-call observation around each explicit runner provider turn.                                                                                   |
| V2 local tools | `packages/core/src/tool/registry.ts`            | One observation around canonical registry settlement. Unknown and stale calls are not reported as executed tools.                                         |
| Process owner  | `packages/core/src/observability/nemo-relay.ts` | Shared activation, serialized teardown/reopen, bounded subscriber flush, and close. The normal CLI exit path requests a two-second bounded shutdown.      |

V2 compaction calls that bypass the runner, agent-generation helpers, session
lifetimes, permission latency, subagent parentage, and direct hard exits from
individual commands are not yet covered. The spike also does not create Relay
scope stacks, so it must not claim causal scope or trace coverage.

## Metric contract

All phase-one instruments are counters. Duration is exported as one bounded
bucket count rather than an unbounded value.

| Measurement                                | Dimensions                                                 |
| ------------------------------------------ | ---------------------------------------------------------- |
| `opencode.runtime.activation.count`        | schema version metadata only                               |
| `opencode.llm.logical_call.count`          | `call_role`, `runtime`, `outcome`                          |
| `opencode.llm.duration_bucket.count`       | `call_role`, bounded `bucket`                              |
| `opencode.llm.finish_reason.count`         | `call_role`, normalized `finish_reason`                    |
| `opencode.model_route.count`               | normalized `provider_family`, normalized `model_family`    |
| `opencode.llm.tokens`                      | `call_role`, fixed token `kind`                            |
| `opencode.tool_call.count`                 | bounded `category`, `execution`, `outcome`                 |
| `opencode.tool_call.duration_bucket.count` | bounded `category`, bounded `bucket`; local execution only |

Token kinds are `input_total`, `input_non_cached`, `input_cache_read`,
`input_cache_write`, `output_total`, and `output_reasoning`. They intentionally
overlap and must not be summed into a second total. Missing provider usage is
omitted rather than invented.

`logical_call` means one host-visible model stream. It is not a physical HTTP
attempt metric: V1 AI SDK retries and V2 `RequestExecutor` retries happen below
these seams. Cost is also omitted because V2 currently persists zero rather
than a reliable normalized cost.

## Privacy and cardinality

The adapter maps provider IDs, model IDs, finish reasons, and tool names into
closed families or categories before emission. Unknown values collapse to
`custom`, `extension`, `other`, or `unknown`.

Prompts, system messages, model output, tool arguments and results, raw errors,
commands, URLs, headers, file paths, working directories, repository or user
names, API material, and raw session/message/tool-call IDs are never Relay
metric attributes. Existing OpenCode OpenTelemetry remains independent; this
spike does not duplicate its spans.

## Lifecycle and disabled parity

One process-global activation is reference counted across Effect application
graphs. The last release flushes subscribers and closes the plugin activation.
A later acquire waits for that close before initializing again. Explicit
shutdown stops new acquisitions and performs a bounded flush/close.

The disabled LLM and tool paths return their original stream or settlement
effect without adding observation wrappers. Relay is never allowed to change a
provider request, tool result, retry decision, cancellation, or error in this
phase.

## Packaging blocker

This branch deliberately does not add `nemo-relay-node` as a normal workspace
dependency. The runtime-dynamic import prevents Bun from eagerly embedding all
native packages, but it also means an installed development module must be
resolvable at runtime. A released standalone OpenCode binary cannot rely on
that arrangement.

Relay 0.9.3 publishes seven native variants covering Linux x64/arm64 glibc and
musl, macOS arm64, and Windows x64/arm64. OpenCode builds twelve standalone
targets, including two macOS x64 variants for which Relay has no artifact. In a
local packaging probe, a naive import embedded every installed Relay addon and
grew the Bun executable to roughly 215 MB.

First-class distribution therefore needs a target-pruned loader/package, an
explicit macOS x64 policy or artifact, packaged-path tests, and CI execution on
the complete OS/architecture/libc matrix. Relay declares Node.js 24 or newer;
Bun compatibility was demonstrated locally but is not yet an advertised Relay
support contract.

## Why managed execution is deferred

Basic Relay 0.9.3 operation works on Bun 1.3.14/macOS arm64, including scope
isolation, metrics, plugin activation, managed tools, and the successful typed
stream path. The OpenCode adapter's activation, metric, flush, and close path
was also exercised against the published 0.9.3 addon. Three semantic gaps
block safe managed execution in OpenCode:

1. A throwing host `AsyncIterable` passed through `typedLlmStreamExecute`
   produced an unhandled Bun rejection while Relay observed normal EOF. Relay
   needs a fallible Node stream bridge before it can own OpenCode streaming.
2. Relay JSON conversion rejects own properties whose value is `undefined`.
   OpenCode tool values require an explicit recursive JSON-boundary codec.
3. Callback errors cross N-API as generic `Error` values. OpenCode relies on
   identities such as `PermissionV1.RejectedError`, so a managed adapter must
   capture and rethrow the original host error.

Relay provider codecs also describe provider-wire payloads, while OpenCode's
default AI SDK seam exposes normalized SDK objects. Codec claims must be made
only at a matching native provider boundary.

## Next qualification gates

Before proposing this upstream, add target-pruned packaging, full disabled and
activation tests, stream error/cancellation coverage for both V1 runtimes,
remaining V2 model-call coverage, command-wide graceful shutdown, concurrent
session tests, and an end-to-end run using a real Relay subscriber. Managed
model/tool middleware should remain a separate phase after the Relay stream
bridge and JSON/error adapters are fixed.
