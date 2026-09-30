# NeMo Relay observability integration

Status: experimental fork work. This is not yet an OpenCode or NeMo Relay
compatibility promise.

The integration is a first-party, in-process observation adapter. It emits two
parallel, privacy-bounded contracts through Relay at OpenCode-owned execution
boundaries:

- structured counters and histograms for aggregate operational reporting; and
- parented Agent, LLM, and Tool scopes for trace backends such as Arize Phoenix.

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

The trace plane uses `opencode.trace.schema_version = "2"` and the following
bounded topology:

```text
opencode.agent.turn                 Agent
├── opencode.llm                    LLM
├── opencode.tool                   Tool
└── opencode.llm.host_retry.scheduled (span event)
```

An LLM or Tool operation running inside a turn receives a propagation-derived
child stack. This preserves parentage while allowing sibling operations to run
concurrently without violating Relay's per-stack LIFO rules. Work that runs
outside a turn, such as some background title generation, is intentionally a
root scope rather than being assigned a false parent.

Here, a turn is one host processor/model step. A user request that calls a tool
and then continues the model produces two Agent roots: the first owns the
tool-request LLM and Tool, and the second owns the continuation LLM. The adapter
does not export a session or user-request identifier merely to join those roots.

Agent scopes contain runtime, call role, terminal outcome, duration bucket, and
OpenTelemetry status. LLM scopes add normalized provider/model families,
runtime, finish reason, standardized token usage, and status. Tool scopes add
only normalized category, execution mode, outcome, optional duration bucket,
and status. Failed and blocked operations use bounded `error.type` values such
as `opencode.failed`; error text is never exported.

Relay's OpenInference exporter projects these lifecycles to Agent, LLM, and Tool
spans. The standardized LLM output fields (`input_tokens`, `output_tokens`,
`total_tokens`, and cache counts) let Phoenix derive its native token
attributes. Relay metric marks remain OTLP metrics and do not appear in Phoenix;
send them to a metrics-capable OTLP backend when both planes are required.

## Metric schema

Every measurement and metric-event metadata object includes
`opencode.metric.schema_version = "2"`. Duration instruments are Relay
histograms with millisecond values, not synthetic duration-bucket counters.

| Measurement                               | Kind      | Attributes                                                                                | Semantics                                                                           |
| ----------------------------------------- | --------- | ----------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `opencode.runtime.activation.count`       | Counter   | schema version only                                                                       | One best-effort emission after a valid plugin-host activation.                      |
| `opencode.agent.turn.count`               | Counter   | `runtime`, `call_role`, `outcome`                                                         | One admitted V1 or V2 host processor/model turn.                                    |
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
means OpenCode emits metrics and, when there is an admitted lifecycle boundary,
a matching Relay trace scope. It does not mean Relay manages or can alter that
execution.

| Surface                         | V1                                                                                                                                                        | V2                                                                                                                                   | Cardinality and terminal ownership                                                                                                              |
| ------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Primary agent/model turn        | Observed around `SessionProcessor.process`.                                                                                                               | Observed around each runner turn; overflow recovery remains inside the owning turn.                                                  | One turn measurement and Agent scope on success, failure, block, or cancellation.                                                               |
| Primary model stream            | Native, AI SDK, and GitLab workflow streams are observed after normalization.                                                                             | The runner's native provider stream is observed before event publication and tool settlement.                                        | One measurement per host subscription, retained through stream finalization.                                                                    |
| Background model work           | Compaction, title, and summary agents are classified when they use the V1 stream/processor path.                                                          | Automatic compaction has both a `compaction` turn and its own observed native stream.                                                | Work entered under an active turn is parented; independently scheduled work remains a truthful root.                                            |
| Local tools                     | Normalized running tool calls are observed through processor settlement. Reused call IDs can start a later generation after the prior generation settles. | Built-in and application tools are observed at canonical `ToolRegistry.settle`, including unknown and stale calls as failures.       | A terminal claim prevents duplicate completion. Durable settlement failure is reported as failure even if tool execution returned successfully. |
| Provider-executed tools         | Observed from normalized provider tool events without local duration.                                                                                     | Observed by the event publisher from provider call/result/error events, including unresolved calls on stream failure.                | Provider tools emit a count only; terminal publication owns completion.                                                                         |
| Policy and human-input outcomes | Denied, rejected, and corrected permissions plus rejected questions are terminal `blocked` outcomes.                                                      | Preserved `ToolFailure.error` identities classify permission and question rejection as `blocked`. A declined turn is also `blocked`. | Policy decisions are outcomes on turns/tools, not a separate decision or latency metric.                                                        |
| Host retry                      | V1 retry scheduling is observed.                                                                                                                          | No separate host-retry hook is present at this seam.                                                                                 | Internal transport retries are not observable here.                                                                                             |
| Direct subtask dispatch         | The explicit task path is observed across pre-hook, child execution, post-hook, durable message/tool persistence, and synthetic summary insertion.        | No canonical V2 task-tool registration exists yet.                                                                                   | The outer task call is measured and scoped; nested observed work inherits context when it remains in the owning Effect.                         |
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
collapse to `anthropic`, `openai`, `azure`, `google`, `amazon`, `github`,
`nvidia`, `openrouter`, `opencode`, `local`, `custom`, or `unknown`. Model values
collapse to a known family (`claude`, `gemini`, `gemma`, `nemotron`, `gpt`,
`llama`, `qwen`, `deepseek`, `mistral`, `grok`, `glm`, or `kimi`) or
`custom`/`unknown`. Finish reasons, roles, runtimes, outcomes, retry attempts,
and tool execution modes are closed enums.

Tool names are reduced to categories: code search, file read/write, terminal,
code execution, delegation, planning, human input, web, skill, MCP, provider,
extension, other, or unknown. An unrecognized or dynamically named tool becomes
`extension`; its raw name is not emitted.

Prompts, system messages, model output, tool arguments and results, error text,
commands, URLs, headers, file paths, working directories, repository or user
names, credentials, and raw session, message, or tool-call IDs are absent from
both trace and metric payloads. Trace request/result objects contain only the
same normalized enums and numeric usage. Durations and token values are
measurements rather than metric attributes. The activation health surface
exposes diagnostic counts, not paths or diagnostic text.

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
- There is no privacy-safe session or user-request envelope above successive
  tool-loop turns, so Phoenix displays each processor/model turn as its own
  Agent root.
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
- Nested operations are parented only while OpenCode's Effect context is
  retained. Independently scheduled background work is not linked by an
  invented session or message identifier.
- Policy denial is represented as a blocked terminal outcome. There is no
  provider-neutral decision event, permission-wait duration, or approval actor
  dimension.
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
(cd packages/cli && bun typecheck)
(cd packages/desktop && bun typecheck)

# Each current-host standalone build runs its packaged Relay activation gate.
(cd packages/opencode && bun run script/build.ts --single)
(cd packages/cli && bun run script/build.ts --single)
```

The focused suite covers disabled parity, invalid configuration, supported and
unsupported runtime loading, report/health projection, lazy streams,
resubscription, usage aggregation, cancellation, blocked outcomes, duplicate
tool terminals, provider tools, V1 retry generations, shared owners, activation
races, bounded drain, failed flush/close, and shutdown retry.

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
should force at least one harmless tool call so both LLM and Tool lifecycles are
exercised:

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
- Phoenix contains `opencode.agent.turn`, `opencode.llm`, and `opencode.tool`,
  with the LLM and Tool parented to the intended Agent when they ran in that
  turn;
- successful scopes report `OK`, and the LLM span contains Phoenix-derived
  prompt, completion, total, and applicable cache token counts;
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
- packaged `opencode` ran live `nvidia/openai/gpt-oss-20b` inference, completed
  a real local file-read tool, and returned the file's canary value;
- Phoenix project `opencode-relay-packaged-final` received two `OK` primary
  Agent spans, their two `OK` LLM children, one `OK` `file_read` Tool child, and
  one independent `OK` title-generation LLM root;
- the LLM spans reported normalized provider `nvidia`, normalized model `gpt`,
  and native Phoenix prompt/completion/total token attributes; and
- the exported project payload contained none of the canary value, prompt file
  name/path, raw model ID, session ID, or tool-call ID.

The final locally activation-tested standalone artifacts had SHA-256 values
`ac06d462465ddb98f9323ef53e33c58fb39b59baddb873dbf93ee87d67bb3c7b`
(`opencode`) and
`f0f2c844ae09e3bed2d785999385c02b9f3857511ec4462c87b77f1199450885`
(`lildax`). These hashes identify local proof artifacts, not published release
artifacts.
