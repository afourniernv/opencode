# NeMo Relay observability integration

Status: experimental fork work. This is not yet an OpenCode or NeMo Relay
compatibility promise.

The integration is a first-party, in-process observation adapter. It emits a
privacy-bounded metric contract through Relay at OpenCode-owned execution
boundaries. It does **not** route provider traffic through the Relay gateway,
create Relay scopes, or hand model and tool callbacks to Relay managed
execution. Relay subscribers and exporters can observe these metrics, but Relay
guardrails and request or execution intercepts cannot block, rewrite, or wrap
the underlying OpenCode operation through this adapter.

## Revision and runtime contract

The implementation was developed and reviewed against:

- OpenCode host baseline `7945de208964a49300d7f770d1a71d078db9a4c4`
  (version 1.18.33).
- NeMo Relay 0.9.3, commit
  `52ea6c06d940342c8b20281389335bf60b70b6e0`.
- Bun 1.3.14 on macOS arm64.

Relay 0.9.3 is the qualification target, not a runtime-enforced version range.
The adapter validates the imported module's `initialize`, `metric`,
`flushSubscribers`, `MetricKind`, and `MetricValueType` surfaces, but the module
does not expose a version that OpenCode checks. Requalify both the API and the
host attachment seams before changing either revision.

The default runtime specifier is `nemo-relay-node`. OpenCode loads it with a
dynamic import only after Relay is explicitly enabled. This branch does not add
that package as a workspace dependency or bundle it into the standalone
OpenCode executable. The operator must make the exact runtime package resolvable
or provide an explicit module specifier:

```text
OPENCODE_NEMO_RELAY_RUNTIME_MODULE=/absolute/path/to/node_modules/nemo-relay-node/index.js
```

The published 0.9.3 Node package declares Node.js 24 or newer and selects one of
seven native packages: Linux x64/arm64 with glibc or musl, macOS arm64, and
Windows x64/arm64. There is no published macOS x64 native package. The default
loader rejects macOS x64 before importing `nemo-relay-node`; a custom runtime
specifier may be used to qualify a separately built addon. Running the Node
binding under Bun worked in the pinned macOS arm64 development environment, but
that is not an upstream Relay support guarantee.

First-class release packaging remains separate work. It needs target-pruned
native artifacts, an explicit macOS x64 decision, and packaged-binary tests for
each supported OS, architecture, and libc combination. A naive Bun import can
embed every installed native variant and is not the intended distribution
shape.

## Activation and health

Relay is strictly opt-in:

```text
OPENCODE_NEMO_RELAY=1
```

Accepted enable values are `1`, `true`, `yes`, and `on`; accepted disable values
are `0`, `false`, `no`, and `off`, case-insensitively. An unknown value is
reported as unavailable. `OPENCODE_PURE=1` or `true` takes precedence and keeps
Relay disabled.

Select an additional Relay plugin configuration with:

```text
OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml
```

OpenCode passes this path to Relay as `additionalPluginsToml`; it does not add a
repository-local discovery rule. Supplying a path without enabling Relay is a
configuration error. An explicitly missing file, a Relay configuration error,
an activation conflict, an incompatible module, and an invalid activation
handle are reported as unavailable. Host execution continues without Relay
instrumentation in every unavailable case.

Runtime loading and plugin-host initialization share a five-second startup
deadline. OpenCode fails open if that deadline expires. Native initialization
cannot be cancelled, so an activation that resolves after the deadline is
retained long enough to flush and close instead of leaking process-global Relay
state.

Inspect the adapter from the same environment that will run OpenCode:

```bash
OPENCODE_NEMO_RELAY=1 \
OPENCODE_NEMO_RELAY_RUNTIME_MODULE=/absolute/path/to/node_modules/nemo-relay-node/index.js \
OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml \
bun run --cwd packages/opencode src/index.ts debug nemo-relay
```

The command prints two related views:

- `service` is the status captured by that Effect service acquisition:
  `disabled`, `active`, or `unavailable`. An active status distinguishes
  `healthy` from `degraded`, `empty` from `present` configuration, and reports
  only bounded counts for configuration paths, components, selected dynamic
  plugins, diagnostics, and dynamic-plugin failures.
- `process` reports the process-global phase (`idle`, `starting`, `active`,
  `draining`, `teardown_failed`, or `stopped`) plus configured owner and admitted
  operation counts.

`exporterDelivery` is deliberately `not_probed`. An active plugin host proves
that Relay accepted the configuration and activation handle; it does not prove
that a remote collector accepted a metric. Verify the configured subscriber or
exporter destination separately.

## Metric schema

Every measurement and metric-event metadata object includes
`opencode.metric.schema_version = "2"`. Duration instruments are Relay
histograms with millisecond values, not synthetic duration-bucket counters.

| Measurement                               | Kind      | Attributes                                                                                | Semantics                                                                           |
| ----------------------------------------- | --------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `opencode.runtime.activation.count`       | Counter   | schema version only                                                                       | One best-effort emission after a valid plugin-host activation.                      |
| `opencode.agent.turn.count`               | Counter   | `runtime`, `call_role`, `outcome`                                                         | One admitted V1 or V2 logical agent turn.                                           |
| `opencode.agent.turn.duration`            | Histogram | `runtime`, `call_role`, `outcome`                                                         | Wall time of the same logical turn.                                                 |
| `opencode.llm.host_stream.count`          | Counter   | `call_role`, `agent_runtime`, `llm_runtime`, `provider_family`, `model_family`, `outcome` | One subscription to an OpenCode-visible LLM event stream.                           |
| `opencode.llm.host_stream.duration`       | Histogram | host-stream attributes                                                                    | Stream lifetime through exhaustion, failure, cancellation, or explicit close.       |
| `opencode.model_route.count`              | Counter   | `call_role`, `agent_runtime`, `llm_runtime`, `provider_family`, `model_family`            | Normalized route selected for that host stream.                                     |
| `opencode.llm.finish_reason.count`        | Counter   | route attributes plus `finish_reason`                                                     | Normalized terminal finish reason when one was observed.                            |
| `opencode.llm.tokens`                     | Counter   | route attributes plus `kind`                                                              | Nonnegative integral usage reported by the provider stream.                         |
| `opencode.tool_call.count`                | Counter   | `category`, `execution`, `outcome`                                                        | One admitted local or provider-executed tool boundary.                              |
| `opencode.tool_call.duration`             | Histogram | `category`, `outcome`                                                                     | Local tool lifetime only; provider-executed duration is not attributed to OpenCode. |
| `opencode.llm.host_retry_scheduled.count` | Counter   | `runtime`, `attempt`                                                                      | One V1 retry scheduled by OpenCode's host retry policy.                             |

LLM and turn histograms use boundaries of 100, 250, 500, 1,000, 2,000,
5,000, 10,000, 30,000, 120,000, and 600,000 ms. Tool histograms use 10,
25, 50, 100, 250, 500, 1,000, 2,000, 5,000, 10,000, and 30,000 ms.

Token kinds are `input_total`, `input_non_cached`, `input_cache_read`,
`input_cache_write`, `output_total`, and `output_reasoning`. These fields can
overlap and must not be summed into another total. Aggregate usage on a terminal
`finish` event wins; otherwise the adapter sums available `step-finish` usage.
Missing, negative, or non-finite values are omitted rather than invented.

### Host-stream cardinality is not network-attempt cardinality

`opencode.llm.host_stream.count` deliberately says `host_stream`. The lease and
timer start when a consumer subscribes, not when an Effect or lazy stream value
is constructed. Re-subscribing is another host stream; never subscribing emits
nothing.

This is not a physical HTTP-attempt metric. AI SDK internal retries and V2
`RequestExecutor` retries occur below the observed stream boundary. A V1 retry
scheduled by OpenCode is reported separately and normally creates a new
host-stream subscription, but the adapter still cannot count transport attempts
inside either provider runtime. The retry `attempt` attribute is closed to
`first`, `second`, or `third_or_later`.

The stream outcome is `success`, `provider_error`, `failed`, `cancelled`,
`incomplete`, or `unknown`. A normally closed stream without a terminal event is
`incomplete`; this includes intentional upstream truncation such as the V1
compaction cutover. Pure interruption is `cancelled`; a mixed failure and
interruption is `failed`.

## Coverage and ownership

The table describes code paths proven at the pinned host revision. “Observed”
means OpenCode emits Relay metrics around a host-owned boundary. It does not
mean Relay manages or can alter that execution.

| Surface                         | V1                                                                                                                                                        | V2                                                                                                                                   | Cardinality and terminal ownership                                                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Primary logical turn            | Observed around `SessionProcessor.process`.                                                                                                               | Observed around each runner turn; overflow recovery remains inside the owning logical turn.                                          | One turn measurement on success, failure, block, or cancellation.                                                                               |
| Primary model stream            | Native, AI SDK, and GitLab workflow streams are observed after normalization.                                                                             | The runner's native provider stream is observed before event publication and tool settlement.                                        | One measurement per host subscription, retained through stream finalization.                                                                    |
| Background model work           | Compaction, title, and summary agents are classified when they use the V1 stream/processor path.                                                          | Automatic compaction has both a `compaction` turn and its own observed native stream.                                                | Background work is separate from the primary stream; there is no causal scope hierarchy.                                                        |
| Local tools                     | Normalized running tool calls are observed through processor settlement. Reused call IDs can start a later generation after the prior generation settles. | Built-in and application tools are observed at canonical `ToolRegistry.settle`, including unknown and stale calls as failures.       | A terminal claim prevents duplicate completion. Durable settlement failure is reported as failure even if tool execution returned successfully. |
| Provider-executed tools         | Observed from normalized provider tool events without local duration.                                                                                     | Observed by the event publisher from provider call/result/error events, including unresolved calls on stream failure.                | Provider tools emit a count only; terminal publication owns completion.                                                                         |
| Policy and human-input outcomes | Denied, rejected, and corrected permissions plus rejected questions are terminal `blocked` outcomes.                                                      | Preserved `ToolFailure.error` identities classify permission and question rejection as `blocked`. A declined turn is also `blocked`. | Policy decisions are outcomes on turns/tools, not a separate decision or latency metric.                                                        |
| Host retry                      | V1 retry scheduling is observed.                                                                                                                          | No separate host-retry hook is present at this seam.                                                                                 | Internal transport retries are not observable here.                                                                                             |
| Direct subtask dispatch         | The explicit task path is observed across pre-hook, child execution, post-hook, durable message/tool persistence, and synthetic summary insertion.        | No canonical V2 task-tool registration exists yet.                                                                                   | The outer task call is measured, but parent/child causal context is not propagated in Relay.                                                    |
| Activation and shutdown         | Shared process owner for all V1 application graphs.                                                                                                       | The same process owner is provided to V2 core layers.                                                                                | Configuration owners and live observations are counted separately.                                                                              |

Tool outcomes are `success`, `failed`, `blocked`, `cancelled`, or `unknown`.
Turn outcomes are `success`, `failed`, `blocked`, or `cancelled`. Telemetry
classification and emission are fail-open: classifier or Relay metric failures
never replace the host result, error, cancellation, retry, or permission
behavior.

## Lifecycle and shutdown

Relay plugin activation and subscribers are process-global. Effect application
graphs acquire a shared owner, while each running turn, subscribed stream, and
admitted tool observation owns a separate operation lease. Releasing the last
application owner stops admission for that activation, waits for accepted
operations, flushes subscribers once, and closes the activation. A later owner
waits for teardown before reopening.

Explicit process shutdown follows the same order:

1. Stop admitting new observations.
2. Wait for admitted turns, streams, and tools to finish within the shared
   deadline.
3. Flush queued subscriber publication once for the activation.
4. Close the plugin-host activation.

The default adapter deadline is five seconds. Flush and close are both attempted
when time permits; a flush failure is retained and cannot later be reported as a
successful shutdown. An incomplete native close retains its activation so a
subsequent shutdown attempt can continue it instead of opening a competing
plugin host. Shutdown calls are single-flight while one attempt is running.

Normal command termination now reaches one root finalizer before the remaining
forced process exit. The `serve` and `web` commands translate SIGINT/SIGTERM into
structured shutdown, stop their server, and dispose reachable instances first.
The TUI worker similarly stops admission, disposes instances and its application
runtime, then gives Relay a bounded final drain. SIGKILL, host termination, or a
deadline overrun can still abandon telemetry; OpenCode prints a warning when the
Relay result is not fully drained, flushed, and closed.

Only `serve` and `web` currently coordinate SIGINT/SIGTERM with structured
shutdown. An OS signal delivered to another command, including the TUI parent,
can bypass the root or worker cleanup path and abandon telemetry. A
command-wide signal coordinator remains required before those paths can claim
graceful signal shutdown.

## Privacy and bounded cardinality

Raw provider and model identifiers are mapped before emission. Provider values
collapse to `anthropic`, `openai`, `azure`, `google`, `amazon`, `github`,
`openrouter`, `opencode`, `local`, `custom`, or `unknown`. Model values collapse
to a known family (`claude`, `gemini`, `gemma`, `nemotron`, `gpt`, `llama`,
`qwen`, `deepseek`, `mistral`, `grok`, `glm`, or `kimi`) or `custom`/`unknown`.
Finish reasons, roles, runtimes, outcomes, retry attempts, and tool execution
modes are closed enums.

Tool names are reduced to categories: code search, file read/write, terminal,
code execution, delegation, planning, human input, web, skill, MCP, provider,
extension, other, or unknown. An unrecognized or dynamically named tool becomes
`extension`; its raw name is not emitted.

Prompts, system messages, model output, tool arguments and results, error text,
commands, URLs, headers, file paths, working directories, repository or user
names, credentials, and raw session, message, or tool-call IDs are not metric
attributes. Durations and token values are measurements rather than attribute
values. The activation health surface exposes diagnostic counts, not paths or
diagnostic text.

Existing OpenCode OpenTelemetry remains independent. This adapter does not
duplicate its spans or attach the Relay metrics to those span contexts.

## Known limitations and deferred work

- This is observation-only integration. It has no Relay scopes, trace
  parentage, context propagation, managed tool or LLM execution, Relay policy
  enforcement, request/result mutation, or provider gateway routing.
- No metric identifies a session, run, message, tool call, user, tenant, or
  repository. That privacy boundary also means downstream metrics cannot
  reconstruct per-session causal trees.
- V1 AI SDK and V2 request-executor physical attempts are hidden below the host
  stream. Cost is omitted because the host does not expose one reliable,
  normalized value at these seams.
- The non-streaming agent-generation call (`generateObject`) bypasses the
  observed LLM stream service.
- Nested work inside an observed tool is not automatically another tool
  boundary. Code-mode child MCP calls and attachment preprocessing that invokes
  helpers directly require their own attachment points.
- V1 dynamic MCP tools are observed at their outer tool boundary, but unknown
  dynamic names collapse to `extension`; MCP provenance is not complete. V2
  core does not yet have canonical MCP or task-tool registration, so those paths
  are not claimed as covered.
- Direct subtasks have an outer delegation metric and child work may emit its
  own independent metrics, but there is no Relay parent/child scope linking the
  two.
- Policy denial is represented as a blocked terminal outcome. There is no
  provider-neutral decision event, permission-wait duration, or approval actor
  dimension.
- Plugin-host health does not probe exporter delivery. A collector outage can
  coexist with `active` health; validate the destination and shutdown flush.
- A standalone OpenCode binary does not yet carry a target-compatible Relay
  addon. Bun compatibility beyond the pinned development environment and the
  complete release target matrix remain unqualified.

Managed execution should remain a separate change. In the pinned Bun probes, a
throwing host `AsyncIterable` did not cross Relay's Node stream bridge with the
required failure semantics, Relay JSON conversion rejected own properties whose
value was `undefined`, and callback errors lost the host-specific class identity
used by permission handling. Dedicated stream, JSON-boundary, and error-identity
adapters need qualification before Relay can safely own OpenCode callbacks.
Relay provider codecs also describe provider-wire payloads, while the default
V1 seam exposes normalized AI SDK objects; attach a codec only at a matching
native provider boundary.

## Qualification

Run tests from their package directories, never from the repository root.

```bash
(cd packages/core && bun test \
  test/observability-nemo-relay.test.ts \
  test/session-runner-tool-events.test.ts \
  test/session-runner-tool-registry.test.ts \
  test/session-runner.test.ts \
  --timeout 30000)

# Run these separately; concurrent Bun test processes can contend for a test port.
(cd packages/opencode && bun test test/session/llm.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/processor-effect.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/prompt.test.ts --timeout 30000)

(cd packages/core && bun typecheck)
(cd packages/opencode && bun typecheck)
```

The focused suite covers disabled parity, invalid configuration, supported and
unsupported runtime loading, report/health projection, lazy streams,
resubscription, usage aggregation, cancellation, blocked outcomes, duplicate
tool terminals, provider tools, V1 retry generations, shared owners, activation
races, bounded drain, failed flush/close, and shutdown retry.

### Relay-backed live exercise

Use the published 0.9.3 addon, a real subscriber, and an actual OpenCode model
stream. A minimal local ATOF file subscriber is sufficient to prove delivery:

```toml
version = 1

[[components]]
kind = "observability"
enabled = true

[components.config]
version = 4

[components.config.atof]
enabled = true

[[components.config.atof.sinks]]
type = "file"
output_directory = "/absolute/path/to/relay-output"
filename = "events.jsonl"
mode = "append"
```

Install `nemo-relay-node@0.9.3` outside the workspace or otherwise make its
entry point resolvable. First check activation, then run inference through the
same CLI and provider configuration used in production:

```bash
export OPENCODE_NEMO_RELAY=1
export OPENCODE_NEMO_RELAY_RUNTIME_MODULE=/absolute/path/to/node_modules/nemo-relay-node/index.js
export OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml

bun run --cwd packages/opencode src/index.ts debug nemo-relay
bun run --cwd packages/opencode src/index.ts run \
  --model provider/model \
  "Read one harmless project file, then reply with a one-line summary."

rg 'opencode\.(runtime|agent|llm|model|tool)' /absolute/path/to/relay-output/events.jsonl
```

A passing exercise requires more than process exit zero:

- health is `active` with the expected configuration and no unexpected
  degradation;
- the model completes through the real host stream and the requested tool, when
  selected, completes normally;
- the file contains activation, turn, host-stream, route, and applicable token
  and tool measurements with schema version 2;
- no prompt, output, path, raw model/provider ID, call ID, or credential appears
  in emitted metric attributes;
- the command exits without an incomplete-shutdown warning; and
- the final records are present after exit, proving that subscriber flush and
  activation close ran.

Repeat the exercise for both V1 AI SDK and native runtimes, V2 when enabled, a
tool failure, policy rejection, cancellation, host retry, partial stream close,
and concurrent sessions before treating a packaged target as qualified.
