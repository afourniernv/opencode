# NeMo Relay observability integration

Status: experimental fork work. This is not yet an OpenCode or NeMo Relay
compatibility promise.

The integration is a first-party, in-process observation adapter. It emits two
parallel, privacy-bounded contracts through Relay at OpenCode-owned execution
boundaries:

- structured counters and histograms for aggregate operational reporting; and
- parented Agent, LLM, Tool, permission Guardrail, and native provider-attempt
  or human-question Function scopes for trace backends such as Arize Phoenix.

It does **not** route provider traffic through the Relay gateway or hand model
and tool callbacks to Relay managed execution. Relay guardrails and request or
execution intercepts therefore cannot block, rewrite, or wrap the underlying
OpenCode operation through this adapter.

## Revision and runtime contract

The implementation was developed and reviewed against:

- OpenCode host baseline `2fa3363c924c5c3e367b84a87ae478296a0ed59b`
  (version 1.18.33).
- NeMo Relay 0.9.3, commit
  `52ea6c06d940342c8b20281389335bf60b70b6e0`.
- Bun 1.3.14 on macOS arm64.

Relay 0.9.3 is a direct, exact dependency of `@opencode-ai/core`. The adapter
validates the imported module's plugin-host, metric, scope-stack, LLM, Tool,
event, flush, and enum surfaces. The module does not expose a version that
OpenCode can check at runtime, so changing the pin still requires API and
packaged-artifact requalification.

The default runtime specifier is `nemo-relay-node`, loaded only after Relay is
explicitly enabled. Source and Node distributions resolve that metapackage in
the normal way. Each supported standalone Bun artifact embeds exactly one
target-native addon behind a generated shim; it does not require an operator
installation and does not carry the other six addons. The Electron distribution
owns the same exact dependency, leaves it external to the JavaScript bundle,
and unpacks the selected `.node` file from ASAR.

`OPENCODE_NEMO_RELAY_RUNTIME_MODULE` remains an advanced development and
qualification override:

```text
OPENCODE_NEMO_RELAY_RUNTIME_MODULE=/absolute/path/to/node_modules/nemo-relay-node/index.js
```

The published 0.9.3 package declares Node.js 24 or newer and ships seven native
packages. OpenCode's distribution contract is:

| Target                         | Relay runtime                                 |
| ------------------------------ | --------------------------------------------- |
| Linux x64/arm64, glibc         | Embedded `*-gnu` addon                        |
| Linux x64/arm64, musl          | Embedded `*-musl` addon                       |
| macOS arm64                    | Embedded Darwin arm64 addon                   |
| Windows x64/arm64              | Embedded `*-msvc` addon                       |
| macOS x64                      | Explicit `unavailable: unsupported_platform`  |
| Electron on a supported target | External metapackage plus unpacked host addon |

Baseline and AVX2 variants share the same Relay addon for their OS,
architecture, and libc. Every standalone artifact includes OpenCode's MIT
license next to the executable; supported Relay artifacts also include Relay's
Apache-2.0 license. The npm postinstall path, shell installer, Docker image, AUR
package, Homebrew formula, and Nix derivation preserve the applicable notices
in their installed output. A custom runtime specifier can be used to qualify a
separately built macOS x64 addon, but it is not a claimed release target.

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

Inspect the adapter from the same packaged artifact and environment that will
run OpenCode:

```bash
OPENCODE_NEMO_RELAY=1 \
OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml \
./opencode debug nemo-relay
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
that a remote collector accepted a trace or metric. Verify the destination
separately.

Both standalone build pipelines run this activation command for the current
host from a clean temporary configuration home. The probe removes ambient
OpenCode/Relay runtime, plugin, pure-mode, logging, and OTLP overrides before it
runs. The build fails when a supported host artifact cannot load its embedded
addon or when a native macOS x64 build does not report the explicit unsupported
state. This is an artifact test, not a substitute for live inference and
exporter delivery.

## Trace schema

The trace plane uses `opencode.trace.schema_version = "3"`. The following is a
representative bounded topology; permission scopes attach to the currently
active Run, Turn, or Tool rather than to a fixed level:

```text
opencode.agent.run                         Agent
└── opencode.agent.turn                    Agent
    ├── opencode.llm                       LLM
    │   └── opencode.llm.provider_attempt  Function
    │       └── opencode.llm.provider_retry.scheduled  span event
    ├── opencode.tool                      Tool
    │   └── nested observed work
    └── opencode.llm.host_retry.scheduled  span event
active Run, Turn, or Tool
├── opencode.permission.evaluated          Guardrail
├── opencode.permission.wait               Guardrail
└── opencode.question.wait                 Function
```

An LLM, Tool, Guardrail, or nested operation receives a propagation-derived
child stack. For manual LLM and Tool handles, the adapter advances the
propagation parent to that handle explicitly. This preserves true tool-child
causality while allowing sibling operations to run concurrently without
violating Relay's per-stack LIFO rules. Title, summary, and prune fibers that
can outlive a run are explicitly detached and become truthful roots instead of
children of an already-completed run.

Here, a turn is one host processor/model step. A run is one admitted V1 prompt
drain or one V2 queued coordinator iteration, including its continuation turns.
It is an execution envelope, not a durable session identifier and not a stable
user-request-counting contract. A request that calls a tool and continues the
model now produces one run Agent with successive turn children. The adapter
does not export a session, message, or user-request identifier to join work.

Agent scopes contain runtime, call role where applicable, terminal outcome,
duration bucket, per-run operation counts, and OpenTelemetry status. LLM scopes
add normalized provider/model families, wire operation when known, stream or
unary mode, finish reason, standardized token usage, first-output metadata, and
status. Native physical attempts are Function children of their logical LLM;
a scheduled retry is an event on the still-open failed attempt before that
attempt closes. Relay 0.9.3 accepts only a `ScopeHandle` as an event parent, not
an `LlmHandle` or `ToolHandle`, so first output remains a metric plus terminal
LLM metadata instead of being emitted as an orphan mark. Tool scopes add only
normalized category, execution mode, outcome, optional duration bucket,
optional bounded terminal-result family, and status. Permission evaluation and wait Guardrail scopes attach beneath the
currently active causal parent, including a Tool when evaluation occurs inside
its owned callback. They contain only runtime, normalized permission family,
effect or resolution, and duration bucket where applicable. Failed and blocked
operations use bounded `error.type` values such as `opencode.failed`; error
text is never exported. A human question owns a Function scope for the actual
wait and records only runtime, terminal resolution, and duration.

Relay's OpenInference exporter projects these lifecycles to Agent, LLM, Tool,
Guardrail, and Function/chain spans. The standardized LLM output fields (`input_tokens`, `output_tokens`,
`total_tokens`, and cache counts) let Phoenix derive its native token
attributes. Relay metric marks remain OTLP metrics and do not appear in Phoenix;
send them to a metrics-capable OTLP backend when both planes are required.

## Metric schema

Every measurement and metric-event metadata object includes
`opencode.metric.schema_version = "3"`. Duration instruments are Relay
histograms with millisecond values, not synthetic duration-bucket counters.

| Measurement                                     | Kind      | Attributes                                                                                                         | Semantics                                                                                         |
| ----------------------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------- |
| `opencode.runtime.activation.count`             | Counter   | schema version only                                                                                                | One best-effort emission after a valid plugin-host activation.                                    |
| `opencode.agent.run.started.count`              | Counter   | `runtime`                                                                                                          | One admitted host execution envelope.                                                             |
| `opencode.agent.run.completed.count`            | Counter   | `runtime`, `outcome`                                                                                               | One terminal host execution envelope. Compare with started count for abandoned work.              |
| `opencode.agent.run.duration`                   | Histogram | `runtime`, `outcome`                                                                                               | Wall time of the same execution envelope.                                                         |
| `opencode.agent.run.operation_count`            | Histogram | run attributes plus `kind`                                                                                         | Per-run turn, LLM, tool, retry/attempt, permission-wait, and question-wait counts.                |
| `opencode.agent.turn.count`                     | Counter   | `runtime`, `call_role`, `outcome`                                                                                  | One admitted V1 or V2 host processor/model turn.                                                  |
| `opencode.agent.turn.duration`                  | Histogram | `runtime`, `call_role`, `outcome`                                                                                  | Wall time of the same logical turn.                                                               |
| `opencode.llm.host_stream.count`                | Counter   | `call_role`, `agent_runtime`, `llm_runtime`, `llm_mode`, `provider_family`, `model_family`, `operation`, `outcome` | One subscription to an OpenCode-visible LLM event stream.                                         |
| `opencode.llm.host_stream.duration`             | Histogram | host-stream attributes                                                                                             | Stream lifetime through exhaustion, failure, cancellation, or explicit close.                     |
| `opencode.llm.unary.count`                      | Counter   | route attributes plus `outcome`                                                                                    | One non-streaming agent-generation operation.                                                     |
| `opencode.llm.unary.duration`                   | Histogram | unary attributes plus `outcome`                                                                                    | Wall time of the same unary operation.                                                            |
| `opencode.llm.time_to_first_output`             | Histogram | stream attributes plus `outcome`, `output_kind`                                                                    | Time to the first non-empty text/reasoning/tool-input delta or concrete tool call.                |
| `opencode.llm.provider_error.count`             | Counter   | stream attributes plus `classification`, `retryable`                                                               | One normalized provider-error terminal event.                                                     |
| `opencode.llm.provider_attempt.started.count`   | Counter   | stream route plus bounded `attempt`                                                                                | One native request-executor HTTP attempt started below the logical host stream.                   |
| `opencode.llm.provider_attempt.completed.count` | Counter   | attempt route plus `outcome`, `status_family`, `error_kind`, `retryable`, `will_retry`                             | One terminal native physical attempt.                                                             |
| `opencode.llm.provider_attempt.time_to_headers` | Histogram | provider-attempt completion attributes                                                                             | Native physical-request latency through response status/header availability, not body drain.      |
| `opencode.llm.provider_retry.scheduled.count`   | Counter   | route, attempt/next-attempt buckets, and `delay_source`                                                            | One native request-executor retry scheduled after a retryable physical failure.                   |
| `opencode.llm.provider_retry.delay`             | Histogram | provider-retry attributes                                                                                          | Retry-After or bounded exponential-backoff delay selected by the native request executor.         |
| `opencode.model_route.count`                    | Counter   | `call_role`, `agent_runtime`, `llm_runtime`, `llm_mode`, `provider_family`, `model_family`, `operation`            | Normalized route selected for a stream or unary operation.                                        |
| `opencode.llm.finish_reason.count`              | Counter   | route attributes plus `finish_reason`                                                                              | Normalized terminal finish reason when one was observed.                                          |
| `opencode.llm.tokens`                           | Counter   | route attributes plus `kind`                                                                                       | Nonnegative integral usage reported by the provider operation.                                    |
| `opencode.llm.input_context_utilization`        | Histogram | route attributes plus `scope`                                                                                      | Provider-step input tokens divided by the selected positive catalog context limit.                |
| `opencode.llm.cost_usd`                         | Counter   | `call_role`, `agent_runtime`, `llm_mode`, `provider_family`, `model_family`, `source`, `scope`                     | V1 provider-step USD cost with explicit `provider_reported` or `price_table_estimate` provenance. |
| `opencode.tool_call.count`                      | Counter   | `category`, `execution`, `outcome`                                                                                 | One admitted local or provider-executed tool boundary.                                            |
| `opencode.tool_call.duration`                   | Histogram | `category`, `outcome`                                                                                              | Local tool lifetime only; provider-executed duration is not attributed to OpenCode.               |
| `opencode.tool.terminal_result.count`           | Counter   | `result_family`, `outcome`                                                                                         | One bounded process result returned by a recognized local terminal tool.                          |
| `opencode.permission.evaluation.count`          | Counter   | `runtime`, `permission_family`, `effect`                                                                           | One final allow, deny, or ask policy evaluation per permission request.                           |
| `opencode.permission.wait.count`                | Counter   | `runtime`, `permission_family`, `resolution`                                                                       | One completed human-wait boundary.                                                                |
| `opencode.permission.wait.duration`             | Histogram | permission-wait attributes                                                                                         | Actual time waiting for once, always, reject, corrected, or cancellation.                         |
| `opencode.question.wait.count`                  | Counter   | `runtime`, `resolution`                                                                                            | One answered, rejected, cancelled, or otherwise unknown human-question wait.                      |
| `opencode.question.wait.duration`               | Histogram | question-wait attributes                                                                                           | Actual human-response wait time; question and answer content are never exported.                  |
| `opencode.compaction.attempt.count`             | Counter   | `runtime`, `trigger`, `outcome`                                                                                    | One V1 or V2 proactive, overflow-recovery, or manual compaction attempt.                          |
| `opencode.compaction.duration`                  | Histogram | compaction-attempt attributes                                                                                      | Wall time of the same compaction attempt, including terminal failure or cancellation.             |
| `opencode.compaction.completed.count`           | Counter   | `runtime`                                                                                                          | One successful V2 compaction with effectiveness estimates.                                        |
| `opencode.compaction.estimated_tokens`          | Counter   | `runtime`, `kind`                                                                                                  | Estimated source, generated-summary, or retained-recent tokens for successful V2 compaction.      |
| `opencode.llm.host_retry_scheduled.count`       | Counter   | `runtime`, `attempt`, `delay_source`, `error_kind`                                                                 | One V1 retry scheduled by OpenCode's host retry policy.                                           |
| `opencode.llm.host_retry.delay`                 | Histogram | host-retry attributes                                                                                              | Exact Retry-After or host-backoff delay selected by the V1 retry policy.                          |

LLM and turn histograms use boundaries of 100, 250, 500, 1,000, 2,000,
5,000, 10,000, 30,000, 120,000, and 600,000 ms. Tool histograms use 10,
25, 50, 100, 250, 500, 1,000, 2,000, 5,000, 10,000, and 30,000 ms.

Token kinds are `input_total`, `input_non_cached`, `input_cache_read`,
`input_cache_write`, `output_total`, and `output_reasoning`. These fields can
overlap and must not be summed into another total. Aggregate usage on a terminal
`finish` event wins; otherwise the adapter sums available `step-finish` usage.
Missing, negative, or non-finite values are omitted rather than invented.
`opencode.llm.input_context_utilization` is emitted only when both the
provider-reported input total and selected catalog context limit are positive.
Stream values are one sample per provider `step-finish` (`scope=provider_step`),
never terminal aggregate usage divided by a single context window. Unary calls
emit one `scope=unary_call` sample. Values above 1 are not capped: they can
expose stale catalog data or provider accounting differences. This per-call
distribution cannot be reconstructed from aggregate token counters and
context-limit configuration.

Cost is emitted only at V1 `step-finish`, where OpenCode already performs its
billing calculation. `provider_reported` currently means an authoritative
Copilot nano-AIU charge; `price_table_estimate` requires a positive resolved
rate for every token category actually used by the step. Missing and all-zero
price-table entries emit no cost measurement, so zero never silently means
"unknown price." The fixed `scope=provider_step` attribute makes this
cardinality explicit; these measurements must not be counted as one cost per
host stream when a stream contains multiple provider steps. V2 currently
persists zero cost and therefore emits no cost measurement.

Compaction attempt outcome, duration, and trigger are observed in both V1 and
V2. Triggers are closed to `proactive`, `overflow_recovery`, or `manual`, and
outcomes to `success`, `failed`, `not_possible`, or `cancelled`. Effectiveness
is success-only and currently V2-only. The `source` estimate covers prior
summary content, prior retained context, and the newly selected head that the
generated summary replaces. `summary` is the new summary text, while
`retained_recent` is the unchanged tail carried alongside it. All three use
OpenCode's estimator and are therefore explicitly named `estimated_tokens`;
summing `source - summary` gives aggregate estimated token reduction without
hiding a compaction whose summary grew. V1 does not yet expose a comparable
before/after record through Relay, and V1 prune passes remain uninstrumented.

The normalized `operation` values are `openai.chat_completions`,
`openai.responses`, `anthropic.messages`, `google.generate_content`,
`aws.converse`, `openrouter.chat_completions`, or `unknown`. V2 supplies its
exact route protocol. V1 supplies the operation only for its native adapter;
AI SDK calls remain `unknown` when the host cannot prove the wire protocol.

Time to first output is intentionally not called time to first protocol chunk.
Structural start events and empty deltas do not stop the timer. The histogram
records the first semantic output OpenCode can consume and classifies it as
`text`, `reasoning`, or `tool`. Relay 0.9.3's manual Node `llmCallEnd` API cannot
receive standard TTFC, so this remains an OpenCode metric rather than the Relay
managed-stream metric.

Permission families are closed to `filesystem`, `terminal`, `network`,
`delegation`, `interaction`, `safety`, `mcp`, `other`, or `unknown`. Evaluation
counts include automatic policy outcomes. Wait metrics are emitted only when a
V1 or V2 permission path actually awaits a reply; reply actors, prompts,
resources, patterns, rules, feedback, and identifiers are never attributes.
`ask()` paths that enqueue a request without awaiting it emit the evaluation but
not a fictitious wait duration.

### Logical stream and physical-attempt cardinality

`opencode.llm.host_stream.count` deliberately says `host_stream`. The lease and
timer start when a consumer subscribes, not when an Effect or lazy stream value
is constructed. Re-subscribing is another host stream; never subscribing emits
nothing.

Native `RequestExecutor` attempts are emitted separately as `provider_attempt`
and `provider_retry` measurements. Their latency ends when response
headers/status are available; logical host-stream duration owns streamed body
consumption. A V1 retry scheduled by OpenCode is a third, distinct layer and
normally creates a new host-stream subscription. Do not sum these layers or
infer billable calls from them. AI SDK-owned physical retries remain below the
available OpenCode hook. Each native physical attempt is also a Function child
span under the logical LLM, while retry delay remains a span event and metric,
not a fabricated sleep-duration span. Attempt attributes are closed to `first`,
`second`, or `third_or_later`.

The stream outcome is `success`, `provider_error`, `failed`, `cancelled`,
`incomplete`, or `unknown`. A normally closed stream without a terminal event is
`incomplete`; this includes intentional upstream truncation such as the V1
compaction cutover. Pure interruption is `cancelled`; a mixed failure and
interruption is `failed`. `content-filter` is a failed LLM outcome rather than a
successful finish. The current canonical event schema identifies
`context_overflow`; absent classifications become `unknown` and any future
unrecognized value becomes `other`.

## Coverage and ownership

The table describes code paths proven at the pinned host revision. “Observed”
means OpenCode emits metrics and, when there is an admitted lifecycle boundary,
a matching Relay trace scope. It does not mean Relay manages or can alter that
execution.

| Surface                         | V1                                                                                                                                                 | V2                                                                                                                                 | Cardinality and terminal ownership                                                                                                |
| ------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Host execution envelope         | One run around an admitted `SessionPrompt` drain; terminal assistant errors override an otherwise successful Effect.                               | One run per queued coordinator iteration, including its steer/continuation turns.                                                  | Started and completed counts are separate; terminal child-turn block/failure is retained instead of being flattened to success.   |
| Primary agent/model turn        | Observed around `SessionProcessor.process`.                                                                                                        | Observed around each runner turn; overflow recovery remains inside the owning turn.                                                | One turn measurement and Agent scope on success, failure, block, or cancellation.                                                 |
| Primary model stream            | Native, AI SDK, and GitLab workflow streams are observed after normalization.                                                                      | The runner's native provider stream is observed before event publication and tool settlement.                                      | One measurement per host subscription, retained through stream finalization.                                                      |
| Unary agent generation          | OAuth `streamObject` and ordinary `generateObject` are observed as unary LLM operations after model alias resolution.                              | No corresponding V2 entry point exists.                                                                                            | The host result/error/cancellation is preserved; successful calls project finish reason and token usage.                          |
| Background model work           | Compaction attempts retain manual/proactive/overflow trigger and terminal outcome; title and summary agents are classified on the stream path.     | Automatic compaction has an attempt lifecycle, a `compaction` turn, its own observed native stream, and success-only estimates.    | Work entered under an active turn is parented; independently scheduled work remains a truthful root.                              |
| Local tools                     | Normalized running tool calls are observed through processor settlement; recognized terminal results are classified after durable completion.      | Built-in and application execution is entered under canonical `ToolRegistry.settle`; Bash structured results are classified there. | A terminal claim prevents duplicate completion. Nested observations use the Tool handle as their propagation parent.              |
| Provider-executed tools         | Observed from normalized provider tool events without local duration.                                                                              | Observed by the event publisher from provider call/result/error events, including unresolved calls on stream failure.              | Provider execution is separate from semantic category; provider tools emit a count only and terminal publication owns completion. |
| Policy and human-input outcomes | Final permission decisions/waits and question waits are observed across reply, rejection, and cancellation.                                        | Permission decisions/waits and question waits are observed with preserved terminal identity.                                       | Automatic decisions do not invent wait time; prompts, answers, actor identity, and request IDs are absent.                        |
| Retry and physical attempts     | V1 host-retry scheduling is observed; native request-executor attempts and delays are distinct.                                                    | Native request-executor attempts and retry delays are observed below the logical stream.                                           | AI SDK-owned physical retries remain hidden; attempt duration is time to headers, not stream lifetime.                            |
| Direct subtask dispatch         | The explicit task path is observed across pre-hook, child execution, post-hook, durable message/tool persistence, and synthetic summary insertion. | No canonical V2 task-tool registration exists yet.                                                                                 | The outer task call is measured and scoped; nested observed work inherits context when it remains in the owning Effect.           |
| Activation and shutdown         | Shared process owner for all V1 application graphs.                                                                                                | The same process owner is provided to V2 core layers.                                                                              | Configuration owners and live observations are counted separately.                                                                |

Tool outcomes are `success`, `failed`, `blocked`, `cancelled`, or `unknown`.
Turn outcomes are `success`, `failed`, `blocked`, or `cancelled`. Telemetry
classification and emission are fail-open: classifier or Relay metric failures
never replace the host result, error, cancellation, retry, or permission
behavior.

A terminal command that returns a non-zero exit remains a successful Tool when
the host launched it, collected its output, persisted the result, and returned
that result to the model. The orthogonal terminal-result family records
`zero_exit`, `nonzero_exit`, `timeout`, `aborted`, `signal`, or `unknown` without
exporting the exact exit code. This keeps expected red-test baselines and other
diagnostic commands out of generic tool-failure SLOs. No terminal-result sample
is invented when a command never produced a result, and provider-executed tools
do not claim a local process result.

## Lifecycle and shutdown

Relay plugin activation and subscribers are process-global. Effect application
graphs acquire a shared owner, while each run, turn, subscribed stream, unary
LLM call, permission wait, and admitted tool observation owns a separate
operation lease. Releasing the last application owner stops admission for that
activation, waits for accepted operations, flushes subscribers once, and closes
the activation. A later owner waits for teardown before reopening.

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
runtime, then gives Relay a bounded final drain. Electron routes ordinary quit,
relaunch, updater-install, and handled signal paths through one idempotent,
awaited quit barrier before resuming application exit. Its sidecar watchdog is
ten seconds so Relay's five-second drain plus HTTP and IPC finalization can
complete. SIGKILL, host termination, or a deadline overrun can still abandon
telemetry; OpenCode prints a warning when the Relay result is not fully drained,
flushed, and closed.

Only `serve` and `web` currently coordinate SIGINT/SIGTERM with structured
shutdown. An OS signal delivered to another command, including the TUI parent,
can bypass the root or worker cleanup path and abandon telemetry. A
command-wide signal coordinator remains required before those paths can claim
graceful signal shutdown.

## Privacy and bounded cardinality

Raw provider and model identifiers are mapped before emission. Provider values
collapse to a checked-in closed family including the major direct providers,
aggregators, local runtimes, `custom`, or `unknown`. Model values collapse to a
checked-in family including Claude, Gemini/Gemma, Nemotron, GPT and explicit
o-series, Llama, Qwen, DeepSeek, Mistral, Grok, GLM, Kimi, MiniMax, MiMo, Nova,
Step, Trinity, and Muse, or `custom`/`unknown`. Finish reasons, roles, runtimes,
wire operations, outcomes, permission effects/resolutions, retry attempts, tool
execution modes, and terminal-result families are closed enums.

Tool names are reduced to categories: code search, file read/write, terminal,
code execution, delegation, planning, human input, web, skill, MCP, provider,
extension, other, or unknown. An unrecognized or dynamically named tool becomes
`extension`; its raw name is not emitted. V1 tools returned by the dynamic MCP
resolver carry a private in-memory marker and are classified as `mcp` without
parsing or exporting their server/tool names.

Prompts, system messages, model output, tool arguments and results, permission
resources/patterns/rules/feedback, error text, commands, URLs, headers, file
paths, working directories, repository or user names, credentials, and raw
session, message, permission, or tool-call IDs are absent from both trace and
metric payloads. Trace request/result objects contain only the same normalized
enums and numeric usage. Durations and token values are measurements rather
than metric attributes. The activation health surface exposes diagnostic
counts, not paths or diagnostic text.

Existing OpenCode OpenTelemetry remains independent. Relay scopes do not join
that context, so enabling both exporters can produce parallel descriptions of
the same host operations.

## Known limitations and deferred work

- This is observation-only integration. It has Relay scopes and propagation,
  but no managed tool or LLM execution, Relay policy enforcement,
  request/result mutation, or provider gateway routing.
- No trace or metric identifies a session, run, message, tool call, user,
  tenant, or repository. Trace parentage reconstructs only the causal hierarchy
  visible inside one process execution.
- The run Agent is a host execution/drain envelope, not a durable session or
  user-request identity. V2 can create multiple run envelopes while draining
  separately queued inputs, and independently scheduled background work is
  deliberately detached.
- Native request-executor physical attempts are observed, but V1 AI SDK-owned
  retries remain hidden below the host stream. V1 cost is emitted only from
  provider-reported billing or a resolved positive price table; V2 still has no
  trustworthy nonzero cost source.
- V1 unary agent generation is observed, but other direct helper/provider calls
  that bypass the canonical stream, unary, or tool boundaries require their own
  attachment points.
- Tool context propagation is installed at V2 registry execution and the V1
  direct-subtask boundary. Event-only provider tools and V1 processor
  settlement do not retroactively parent work that already executed elsewhere.
  Code-mode child MCP calls and attachment preprocessing still need canonical
  attachment points.
- V1 dynamic MCP tools are classified at their outer tool boundary, but MCP
  connection, transport, resource, and protocol-operation health are not yet
  observed. V2 core does not yet have canonical MCP or task-tool registration,
  so those paths are not claimed as covered.
- V2 queue admission-to-promotion latency and active run/LLM/tool concurrency
  have truthful local seams but are not yet instrumented. V1 has no equivalent
  canonical durable queue boundary, so parity must not be inferred.
- Tool-output byte/line pressure and truncation, first-class foreground versus
  background subagent lifecycles, request-level cache-hit outcomes, and runtime
  selection/fallback are useful next direct observations. They require bounded
  semantic plumbing at their owning seams rather than inference from tool or
  stream completion events.
- Success/error rates, percentiles, route share, cache-token ratio, retry rate,
  reasoning share, cost per call/run/token, and aggregate compaction savings
  remain backend-derived views of the direct counters and histograms; Relay
  should not publish redundant pre-aggregated instruments for them.
- Nested operations are parented only while OpenCode's Effect context is
  retained. Independently scheduled background work is not linked by an
  invented session or message identifier.
- Permission policy/wait and human-question wait telemetry are covered. No
  approval actor, prompt, answer, or option dimension is exported, by design,
  and no provider-neutral decision API exists in Relay.
- Logical provider-error attributes are constrained by the normalized event
  schema, which currently exposes only `context-overflow` as a specific
  classification. The native physical-attempt hook reports bounded
  authentication, rate-limit, quota, policy, transport, provider-internal, and
  related error kinds. Effect-channel protocol/decode failures currently close
  the host stream as `failed` without a bounded error-kind dimension; AI SDK
  and logical-stream parity still need a provider-neutral host normalization
  hook.
- Plugin-host health does not probe exporter delivery. A collector outage can
  coexist with `active` health; validate the destination and shutdown flush.
- The packaged macOS arm64 Bun and Electron artifacts are qualified locally.
  All 12 standalone variants cross-build successfully, and both Linux musl
  Docker targets pass their in-image activation gate. Linux glibc, Windows,
  native Linux host execution, Intel macOS unsupported-state, and Nix builds
  still require their release runners before the complete matrix can be claimed
  as qualified.
- `bun.lock` changes intentionally require the repository's four-native-runner
  `nix-hashes` workflow after merge. Cross-generated hashes are not
  byte-identical, so do not guess or hand-edit `nix/hashes.json` in a feature
  branch.

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
(cd packages/llm && bun test \
  test/executor.test.ts \
  test/exports.test.ts \
  --timeout 30000)

(cd packages/core && bun test \
  test/observability-nemo-relay.test.ts \
  test/permission.test.ts \
  test/question.test.ts \
  test/session-runner-tool-events.test.ts \
  test/session-runner-tool-registry.test.ts \
  test/session-runner.test.ts \
  --timeout 30000)

# Run these separately; concurrent Bun test processes can contend for a test port.
(cd packages/opencode && bun test test/session/llm.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/processor-effect.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/prompt.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/compaction.test.ts --timeout 30000)
(cd packages/opencode && bun test test/permission/next.test.ts --timeout 30000)
(cd packages/opencode && bun test test/question/question.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/retry.test.ts --timeout 30000)
(cd packages/opencode && bun test test/session/tools.test.ts --timeout 30000)

(cd packages/llm && bun typecheck)
(cd packages/core && bun typecheck)
(cd packages/opencode && bun typecheck)
(cd packages/cli && bun typecheck)
(cd packages/desktop && bun typecheck)

# Each current-host standalone build runs its packaged Relay activation gate.
(cd packages/opencode && bun run script/build.ts --single)
(cd packages/cli && bun run script/build.ts --single)
```

The focused suite covers disabled parity, invalid configuration, supported and
unsupported runtime loading, report/health projection, lazy streams,
resubscription, usage aggregation, first semantic output, protocol
normalization, unary generation, cancellation, blocked and content-filter
outcomes, native physical-attempt/retry ordering and cleanup, run aggregation,
background detachment, tool-child propagation, permission decision/wait
Guardrail scopes, human-question Function scopes, duplicate tool terminals,
provider and dynamic MCP tools, cost provenance, context utilization, V1/V2
compaction attempts, V2 compaction effectiveness, enriched V1 retry
generations, shared owners, activation races, bounded drain, failed
flush/close, and shutdown retry.

### Relay-backed Phoenix exercise

Phoenix accepts OTLP traces, not Relay's OTLP metric stream. Configure only an
OpenInference trace endpoint for this exercise; use a separate metrics-capable
collector when validating the metric plane.

Start a local Phoenix instance and wait for it to become healthy:

```bash
docker run --rm -d \
  --name opencode-phoenix \
  -p 127.0.0.1:6006:6006 \
  -p 127.0.0.1:4317:4317 \
  -e PHOENIX_WORKING_DIR=/mnt/data \
  -v opencode-phoenix:/mnt/data \
  arizephoenix/phoenix:version-20.16.0

curl -fsS http://127.0.0.1:6006/healthz
```

Use this Relay configuration:

```toml
version = 1

[[components]]
kind = "observability"
enabled = true

[components.config]
version = 4

[components.config.opentelemetry]
enabled = true

[[components.config.opentelemetry.endpoints]]
type = "openinference"
endpoint = "http://127.0.0.1:6006/v1/traces"
transport = "http_binary"
service_name = "opencode"
instrumentation_scope = "nemo-relay"
mark_projection = "event"
scheduled_delay_millis = 250

[components.config.opentelemetry.endpoints.resource_attributes]
"openinference.project.name" = "opencode-relay-live"
"deployment.environment" = "local"
```

Run activation and real inference through the packaged executable. The prompt
should force at least one harmless tool call so the Run, Turn, LLM, and Tool
lifecycles are exercised. A permission-wait span requires an `ask` policy and a
real reply; do not infer it from an automatically allowed tool:

```bash
export OPENCODE_NEMO_RELAY=1
export OPENCODE_NEMO_RELAY_PLUGINS_TOML=/absolute/path/to/plugins.toml

./opencode debug nemo-relay
./opencode run --model provider/model \
  "Use the read tool on README.md, then reply with a one-line summary."

curl -fsS http://127.0.0.1:6006/v1/projects/opencode-relay-live/spans |
  jq '.data[] | {name, span_kind, parent_id, status_code, attributes}'
```

A passing exercise requires more than process exit zero:

- packaged health is `active` with the expected configuration and no unexpected
  degradation;
- the remote model and requested tool complete through real OpenCode paths;
- Phoenix contains `opencode.agent.run`, `opencode.agent.turn`,
  `opencode.llm`, and `opencode.tool`, with the LLM and Tool parented to the
  intended Agent when they ran in that turn; a native run also contains an
  `opencode.llm.provider_attempt` Function child, and a policy evaluation
  contains an `opencode.permission.evaluated` Guardrail under its active causal
  parent;
- successful scopes report `OK`, and the LLM span contains Phoenix-derived
  prompt, completion, total, and applicable cache token counts;
- there are no orphan `mark:` spans; metric marks remain on the OTLP metrics
  plane and provider-attempt retry events remain attached to their Function
  scope;
- no prompt, output, path, raw model/provider/tool name, call ID, or credential
  appears anywhere in the exported payload;
- the command exits without an incomplete-shutdown warning; and
- the final spans are queryable after exit, proving subscriber flush and
  activation close reached the exporter.

Repeat the exercise for both V1 AI SDK and native runtimes, V2 when enabled, a
tool failure, policy rejection, cancellation, host retry, partial stream close,
and concurrent sessions before treating a packaged target as fully qualified.

### Latest local qualification evidence

On 2026-09-29, macOS arm64 with Bun 1.3.14, Electron 42/Node 24.15.0,
Relay 0.9.3, and Phoenix 20.16.0 passed the following checks:

- standalone `opencode` and preview `lildax` both activated after the installed
  source addon directory was made unavailable, proving that each executable is
  self-contained;
- the final main and preview platform packages carried byte-identical MIT and
  Apache-2.0 notices, and the npm postinstall path preserved the Relay notice
  through both normal optional-dependency resolution and forced fallback
  installation;
- all 12 standalone OpenCode targets cross-built from the pinned metapackage;
  Linux amd64 baseline-musl and Linux arm64 musl then activated inside their
  Alpine Docker images, with the Relay license present in both images;
- adversarial ambient runtime-module, plugin-TOML, pure-mode, Relay logging, and
  OTLP variables did not influence either standalone build probe;
- the Electron directory package contained only the Darwin arm64 Relay addon,
  loaded it from `app.asar.unpacked`, initialized Relay, and closed it;
- packaged `opencode` ran live `nvidia/openai/gpt-oss-20b` inference through
  the ordinary AI SDK path, completed a real local file-read tool, and returned
  the file's canary value;
- rebuilt standalone version `0.0.0-nemo-relay-metrics-202609300321` then
  selected the native request executor through a temporary NVIDIA-compatible
  provider alias, completed a real file-read tool, and returned the expected
  marker without a runtime or shutdown failure;
- Phoenix project `opencode-relay-deeper-final-v3` received 11 spans across two
  traces: one Run, two Turns, three LLMs (including detached title generation),
  three native provider-attempt Function children, one Tool, and one
  permission-evaluation Guardrail;
- all 11 spans reported schema version 3 and `OK`; every provider attempt was a
  direct child of its logical LLM, each Tool was under its active Turn, each
  Guardrail was under the active Run context, and the project contained zero
  orphan `mark:` spans;
- the run metrics recorded two turns, two logical LLM operations, one tool
  call, two provider attempts, zero provider retries, zero host retries, zero
  permission waits, and zero question waits. Three stream completions,
  including detached title generation, each emitted one
  `scope=provider_step` context-utilization sample; and
- neither the ATOF output nor exported Phoenix payload contained the canary
  value, prompt file name/path, raw model/provider ID, workspace path, session
  ID, or tool-call ID.

One immediately preceding live attempt received an invalid
`openai-compatible-chat` stream event from the remote endpoint after a tool
step and terminated cleanly with a host error; the deterministic direct-read
repeat above passed. That intermittent provider/protocol failure is not treated
as a qualification pass and motivates the bounded host-stream error
classification noted under known limitations. Automatic replay is deliberately
not inferred because retrying after tool progress can duplicate side effects.

Earlier self-contained OpenCode and lildax binaries were also activation-tested
and hashed during packaging qualification. Those ephemeral hashes are omitted
here because the metrics slice was rebuilt afterward; they are not published
release artifacts or stable qualification identifiers.
