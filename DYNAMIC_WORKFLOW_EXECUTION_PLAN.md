# Dynamic workflow execution plan

Status: implementation plan based on `main` at `f7fc3ab15` on 14 July 2026.

This document is intended to be handed to a fresh Codex session. Read
`AGENTS.md` before starting. Implement the slices in order. Give each slice to
one subagent, integrate and verify it, then start the next subagent from the
updated branch.

## Goal

Add an opt-in Temporal dynamic workflow path that accepts a Zigflow workflow
definition as part of the execution input, validates and snapshots it at
execution start, builds the existing task closure tree inside the Temporal
workflow, and interprets that tree against runtime state.

The existing file-backed, statically registered path must continue to work
unchanged during this rollout. In particular, existing workflow and activity
type names, replay behaviour, worker versioning behaviour, and public input
contracts must remain compatible.

## What the code does today

Zigflow is already an interpreter. There is no code generation or compilation
artefact.

The current pipeline is:

```text
YAML or JSON
  -> schema validation when requested
  -> normalisation and model decoding
  -> PostLoad, structural validation, expression determinism validation
  -> TaskBuilder tree
  -> TemporalWorkflowFunc closure tree
  -> tree-walking execution against runtime State
```

The relevant boundary is in `pkg/zigflow/tasks/task_builder.go`:

- `TaskBuilder.Build()` returns `TemporalWorkflowFunc`.
- `TemporalWorkflowFunc` is a Go closure, not generated Go source.
- `NewTaskBuilder` is the AST-node factory.
- `DoTaskBuilder.workflowExecutor` walks the built closures at Temporal
  workflow runtime.
- `SwitchTaskBuilder.Build` evaluates cases against live runtime state.

`pkg/zigflow/workflow_builder.go:NewWorkflow` currently discards the closure
returned by `DoTaskBuilder.Build` because `Build` also registers it on the
worker.

## Corrections to the initial premise

There is no Zigflow dynamic workflow test today. The existing
`RegisterDynamicWorkflow` and `RegisterDynamicActivity` methods are methods on
test doubles which exist only to satisfy `worker.Worker`. They either do
nothing or panic.

Temporal's Go testsuite does support the desired path. The handler signature
is:

```go
func(workflow.Context, converter.EncodedValues) (any, error)
```

`testsuite.TestWorkflowEnvironment.RegisterDynamicWorkflow` can register one
fallback handler and execute arbitrary workflow type strings. The upstream SDK
test is in
[`internal/workflow_testsuite_test.go`](https://github.com/temporalio/sdk-go/blob/v1.46.0/internal/workflow_testsuite_test.go#L1187-L1235).

There are two additional build-time registration dependencies:

1. Built-in HTTP, gRPC, container, script, and shell tasks register per-task
   activity aliases during `Build`. New histories select those aliases through
   `workflow.GetVersion`. The shared receiver objects registered at worker
   startup expose only the fixed historical method names.
2. Do, For, Fork, and Try build and register named Temporal child workflows.
   Those child executions currently receive only `(input, state)`. A dynamic
   fallback handling one of those synthetic types would not have the workflow
   definition required to rebuild its closure.

A nil `worker.Worker` is therefore not yet a usable dynamic build mode. For
currently dereferences it directly. Other containers rely on nested Do
builders to register child workflow types. Built-in activities would schedule
aliases which were never registered.

## Architectural decisions for the first milestone

These decisions keep the first milestone deterministic, additive, and small.

### 1. Dynamic execution is opt-in

Add a repeatable `zigflow run --dynamic-task-queue <queue>` flag. Each value
creates or reuses one worker for that task queue and registers one dynamic
workflow fallback on it.

Static `--file` and `--dir` behaviour remains the default. Static and dynamic
registrations may coexist on one task queue. Temporal resolves an explicitly
registered workflow type before the dynamic fallback.

Do not silently install a fallback on every existing static queue. That would
change an unknown workflow type from a Temporal registration error into a
Zigflow input or validation error.

### 2. Inline definitions come first

The initial public input is one versioned, serialisable envelope:

```go
type DynamicWorkflowInput struct {
    Version    int    `json:"version"`
    Definition []byte `json:"definition"`
    Input      any    `json:"input,omitempty"`
}
```

The exact exported name may change during implementation, but the contract
must have an explicit version, raw definition bytes, and user input. Raw bytes
are recorded in Temporal history as part of the start event, which provides
snapshot-at-start behaviour.

Definition references are a later milestone. Fetching a reference must use an
activity. It must never read a file, network resource, environment-dependent
catalogue, or mutable process-global registry directly from workflow code.

### 3. Validation remains mandatory

The dynamic handler must schema validate, normalise, run PostLoad, validate
task structure, validate expression determinism, and validate the DSL version
before executing the first task. Validation failures must become actionable,
non-retryable Temporal application errors.

Expose one parse-and-prepare API so the handler does not call
`ValidateBytes` and `LoadFromBytes` separately and parse the document twice.
Do not introduce a second validation implementation.

The handler must also reject:

- an unsupported envelope version;
- an empty definition;
- a definition whose `document.taskQueue` differs from
  `workflow.GetInfo(ctx).TaskQueueName`;
- a requested Temporal workflow type that is not one of the executable root
  workflow types built from the definition.

The last rule handles both single-workflow documents and the existing
multiple-root-Do behaviour where top-level Do task names become workflow
types and `document.workflowType` is ignored.

### 4. Building and registration become separate operations

Add a small workflow registration abstraction used only by the build phase:

- A worker-backed implementation preserves current static registration.
- An execution-local implementation records `workflow type ->
  TemporalWorkflowFunc` in a map owned by one dynamic invocation.
- Duplicate names return explicit errors. They must not overwrite an existing
  closure.

The dynamic handler builds the entire local registry deterministically, looks
up `workflow.GetInfo(ctx).WorkflowType.Name`, and invokes that closure. A
synthetic child workflow repeats the same build from the recorded definition
and dispatches its own workflow type through its new local registry.

Do not cache closure trees or parsed definitions in mutable package globals.
Workflow replay must be able to reconstruct the same registry from history
alone.

### 5. Internal boundaries carry a dynamic invocation envelope

Continue-As-New, redirect children, For iterations, Fork branches, and Try or
Catch children need the definition snapshot. Add an internal versioned
invocation envelope containing definition bytes, user input, and `*utils.State`.
In dynamic mode those boundaries pass the envelope as one Temporal argument.
In static mode they continue to pass the historical `(input, state)` arguments.

Keep the definition out of `utils.State`. State is passed to every built-in
activity. Storing the full definition there would duplicate it into every
activity payload and needlessly grow history.

The definition bytes should instead be captured by the built closure through
an explicit build or execution options object. A shared helper on the embedded
base builder can construct static or dynamic child arguments. Each container
slice below applies that helper to its own call sites.

### 6. Dynamic built-in activities use fixed registered names

The worker already registers `activities.Registry` once per task queue. It
contains `CallHTTP`, `CallGRPC`, and `Run`, which expose these fixed names:

- `CallHTTPActivity`
- `CallGRPCActivity`
- `CallContainerActivity`
- `CallScriptActivity`
- `CallShellActivity`

Add an explicit activity dispatch policy. Static builds must retain per-task
aliases and the existing `GetVersion` compatibility marker. Dynamic builds
must schedule the fixed names above and must not attempt worker registration.

Do not add `RegisterDynamicActivity` in the first milestone. The activity type
alias does not encode which implementation should decode the first argument,
so a generic dynamic activity would add dispatch and decoding complexity
without being needed for execution. Custom `call: activity` tasks are already
name-based and remain unchanged.

The observability trade-off is explicit: dynamic built-in activities initially
have fixed activity type names instead of per-task activity type aliases.

### 7. Preserve external child semantics

`run.workflow` references another deployed Temporal workflow by name. The
inline definition does not contain the target workflow definition. Leave this
path unchanged. The target must be explicitly registered elsewhere. Dynamic
catalogue resolution for another definition is out of scope.

### 8. External side effects are disabled in the dynamic milestone

`cloudevents.Events.Emit` currently calls `time.Now`, mutates metrics, and can
send network requests directly from workflow code. It is not safe to load or
use configured CloudEvents clients while dynamically building inside a
workflow.

Add an explicit no-op event emitter for dynamic execution. Do not call
`cloudevents.Load` from workflow code. Moving CloudEvents delivery behind a
generic activity is a separate correctness project.

Worker environment values are also mutable process state. Snapshot the env map
once at the root with `workflow.SideEffect`, or include it in the public input,
then carry the recorded map through internal invocations. Do not reread process
environment during replay.

### 9. Schedules and definition references are deferred

Current schedule creation supplies only schedule input to the workflow. A
dynamic schedule also needs the definition envelope. Do not quietly create a
schedule which will fail at runtime.

For the first milestone, reject or document schedules as unsupported in
dynamic-only mode. Add dynamic schedule wrapping in its own later slice after
the base path is stable.

## Sequential implementation slices

Each slice includes a narrow ownership surface. Later agents may edit a shared
core file only where their slice explicitly says so. An agent must not combine
the next slice for convenience.

### Slice 0. Dynamic testsuite harness

Owner surface:

- new test helper and fixtures under `pkg/zigflow`;
- no production behaviour changes.

Tasks:

1. Add a helper based on `testsuite.WorkflowTestSuite` which registers a probe
   dynamic workflow and executes an arbitrary workflow type string.
2. Add small inline or `testdata/dynamic` fixtures for:
   - Set followed by Switch;
   - invalid schema;
   - unsupported task shape;
   - Continue-As-New;
   - For, Fork, and Try;
   - one built-in activity.
3. Add reusable assertions for workflow result, non-retryable application
   errors, and scheduled activity or child workflow names.
4. Keep feature contract cases in the slices which make them pass. Do not
   commit permanently failing or skipped tests.

Acceptance:

- The probe proves the repository's pinned Temporal v1.46.0 testsuite can
  execute an arbitrary string through `RegisterDynamicWorkflow`.
- The helper does not require a real Temporal server.
- Existing tests are untouched except for shared test-only helper reuse.

### Slice 1. Deterministic traversal prerequisites

Owner surface:

- `pkg/zigflow/tasks/task_builder_try.go`;
- `pkg/zigflow/tasks/task_builder_switch.go`;
- `pkg/zigflow/tasks/task_builder_fork.go` only for future ordering;
- `pkg/utils/cancellable_futures.go`;
- focused tests beside those files.

Tasks:

1. Replace Try's `map` of Try and Catch bodies with a fixed ordered two-entry
   representation. Build, PostLoad, and Validate must traverse the same order.
2. Make Switch validation reject a switch list item containing anything other
   than exactly one named case. This makes the inner map iteration safe because
   a valid item has one entry.
3. Replace `CancellableFutures`' externally ranged map with an insertion-ordered
   representation. Fork goroutine setup and result indexes must follow branch
   definition order. Remove the goroutine-shared incrementing index.
4. Preserve Fork completion and compete semantics.

Acceptance:

- Repeated builds produce the same first validation error.
- Repeated Fork runs install handlers and map results in definition order.
- Existing replay and Fork tests pass.

### Slice 2. Workflow registration seam

Owner surface:

- `pkg/zigflow/tasks/task_builder.go`;
- `pkg/zigflow/tasks/task_builder_do.go`, build and registration code only;
- `pkg/zigflow/tasks/task_builder_for.go`, wrapper registration code only;
- new focused registration tests under `pkg/zigflow/tasks`.

Tasks:

1. Introduce the minimal workflow registrar or registry interface.
2. Add a worker-backed adapter using `RegisterWorkflowWithOptions`.
3. Add an execution-local registry which stores named
   `TemporalWorkflowFunc` values and rejects duplicates.
4. Route Do registration and For's special `forChildResult` wrapper through
   the seam. Declare the wrapper with the uniform `TemporalWorkflowFunc`
   result type while preserving its serialised `forChildResult` value.
5. Thread the registry through `TaskOpts` or a dedicated build options object
   so nested Do builders created by Fork and Try inherit it without changing
   every constructor.
6. Preserve `NewWorkflow`'s current static registration behaviour.

Acceptance:

- A definition containing every supported container can build against a local
  registry with no SDK worker registration call.
- Static root, multiple-root Do, For, Fork, and Try register exactly the same
  type names as before.
- Duplicate generated names return an explicit build error instead of
  overwriting or panicking.
- No activity dispatch changes are included in this slice.

### Slice 3. Dynamic built-in activity dispatch and no-op events

Owner surface:

- `pkg/zigflow/tasks/task_builder.go`, activity helpers only;
- `pkg/zigflow/tasks/task_builder_call_http.go`;
- `pkg/zigflow/tasks/task_builder_call_grpc.go`;
- `pkg/zigflow/tasks/task_builder_run.go`;
- a small no-op constructor or interface in `pkg/cloudevents`;
- focused activity dispatch and replay tests.

Tasks:

1. Add an explicit static versus dynamic activity dispatch policy.
2. In static mode, retain per-task registration, alias names, and
   `activityNamingVersionChangeID` exactly.
3. In dynamic mode, schedule the fixed built-in method name and perform no
   worker registration.
4. Leave custom `call: activity` name and task queue handling unchanged.
5. Provide an explicit no-op emitter which returns before event creation,
   clocks, metrics, or client sends. Dynamic building will use it.

Acceptance:

- A registration-free HTTP, gRPC, container, script, or shell build schedules
  only a name exposed by `ActivitiesList`.
- A new static build still schedules its per-task alias.
- Old static history replay tests remain green.
- Dynamic execution emits no external CloudEvents and does not dereference a
  nil emitter.

### Slice 4. Parse-once build API and flat dynamic handler

Owner surface:

- `pkg/zigflow/loader.go`;
- `pkg/zigflow/workflow_builder.go`;
- new `pkg/zigflow/dynamic_workflow.go`;
- new tests under `pkg/zigflow` using the Slice 0 harness.

Tasks:

1. Extract one bytes-to-prepared-workflow function which performs mandatory
   schema validation, normalisation, model decoding, PostLoad, task validation,
   expression determinism validation, and DSL-version validation.
2. Extract a build API which returns the execution-local registry without
   registering an SDK workflow. Keep `NewWorkflow` as a compatibility wrapper
   around the same preparation and build logic.
3. Define the versioned public input envelope.
4. Implement a dynamic handler factory which captures immutable worker runtime
   options, decodes the root envelope, prepares and builds the registry, checks
   task queue and executable workflow type, then invokes the selected closure.
5. Snapshot worker env with `workflow.SideEffect` before creating root state.
6. Return non-retryable application errors for input, validation, preparation,
   build, and dispatch failures. Keep the underlying actionable message.

Acceptance:

- Arbitrary workflow type names execute a flat Set, Switch, Wait, Listen, or
  Raise definition in the testsuite.
- The user input visible as `$input` is exactly the envelope's `input`, not the
  whole envelope.
- Invalid definitions fail before any timer, child workflow, or activity is
  scheduled.
- Task queue and executable type mismatches fail explicitly.
- `NewWorkflow` tests and static worker registration tests remain green.

### Slice 5. Do boundaries, multiple roots, redirects, and Continue-As-New

Owner surface:

- `pkg/zigflow/tasks/task_builder.go` only for a shared internal invocation
  helper or execution options;
- `pkg/zigflow/tasks/task_builder_do.go`;
- dynamic Do and Continue-As-New tests.

Tasks:

1. Define the versioned internal invocation envelope. It carries definition
   bytes, recorded env, original user input, and state.
2. Capture immutable definition bytes in dynamic build options. Do not add them
   to `utils.State`.
3. Add a base helper which returns historical `(input, state)` arguments for
   static mode and the one internal envelope for dynamic mode.
4. Use the helper for redirect child executions.
5. Use the helper for Continue-As-New. The continued execution must rebuild
   from the identical recorded bytes and resume from `CANStartFrom`.
6. Prove dispatch for both single root and multiple top-level Do workflow
   types.

Acceptance:

- A forced Continue-As-New completes and does not lose or replace the
  definition, env snapshot, input, context, data, output, or resume task ID.
- A Switch redirect executes the correct locally registered named Do closure.
- Static Continue-As-New and redirect histories retain their old argument
  shape.
- Definition bytes do not appear in built-in activity state arguments.

### Slice 6. For dynamic child propagation

Owner surface:

- `pkg/zigflow/tasks/task_builder_for.go` execution call sites;
- `pkg/zigflow/tasks/task_builder_for_test.go` and dynamic For tests only.

Tasks:

1. Use the internal invocation helper for each For iteration child workflow in
   dynamic mode.
2. Preserve the wrapper result shape, state isolation, inter-iteration output
   and context, `while`, `at`, end propagation, and Continue-As-New behaviour.
3. Do not change Fork or Try.

Acceptance:

- Dynamic array and object loops match static outputs.
- Nested tasks resolve from the recorded definition in the synthetic child.
- State isolation tests remain green.
- A nested `then: end` still reaches the true root with the effective output.

### Slice 7. Fork dynamic child propagation

Owner surface:

- `pkg/zigflow/tasks/task_builder_fork.go` execution call sites;
- `pkg/zigflow/tasks/task_builder_fork_test.go` and dynamic Fork tests only.

Tasks:

1. Use the internal invocation helper for every branch child in dynamic mode.
2. Preserve branch workflow IDs, non-competing aggregation, compete winner,
   cancellation, error precedence, and end propagation.
3. Rely on the insertion ordering established in Slice 1. Do not reintroduce
   map iteration.

Acceptance:

- Dynamic Fork and competing Fork match static results.
- Every branch receives an isolated state clone and the same definition
  snapshot.
- Failures and `then: end` retain existing precedence.

### Slice 8. Try and Catch dynamic child propagation

Owner surface:

- `pkg/zigflow/tasks/task_builder_try.go` execution call sites;
- `pkg/zigflow/tasks/task_builder_try_test.go` and dynamic Try tests only.

Tasks:

1. Use the internal invocation helper for Try and Catch children in dynamic
   mode.
2. Preserve error classification, `catch.as`, catch-state isolation, child
   error metadata, and end propagation which bypasses Catch.
3. Do not change For or Fork.

Acceptance:

- A failing dynamic Try runs Catch with the same error data as static mode.
- Catch data does not leak into parent state unless output or export promotes
  it.
- `then: end` in Try bypasses Catch and `then: end` in Catch reaches the root.

### Slice 9. Worker and CLI integration

Owner surface:

- `cmd/run/flags.go` and `flags_test.go`;
- `cmd/run/command.go` and `command_test.go`;
- `cmd/run/workflows.go` and `workflows_test.go`;
- `cmd/run/workers.go` and `workers_test.go`;
- `cmd/run/watch.go` only if the validation cannot stay in command or
  preparation code.

Tasks:

1. Add repeatable `--dynamic-task-queue` and Viper key
   `dynamic_task_queue`.
2. Allow no workflow files only when at least one dynamic task queue exists.
   Preserve `No workflow files found` otherwise.
3. Reject dynamic-only `--watch`. Hybrid mode may continue watching its static
   files without recreating the dynamic registration.
4. Refactor worker construction just enough to collect queues first, create one
   worker per unique queue, then apply static and dynamic registrations.
5. Register shared built-in activities exactly once when each worker is
   created.
6. Register the dynamic workflow exactly once for each configured dynamic
   queue. Deduplicate repeated flags.
7. Preserve worker options, deployment versioning, health task queues,
   schedule updates for static definitions, shutdown, and telemetry.
8. When worker versioning is enabled, provide or validate the dynamic runtime
   versioning behaviour required by `workflow.DynamicRegisterOptions`.

Acceptance matrix:

| Mode | Expected result |
| --- | --- |
| Static files only | Existing behaviour and input contract |
| Dynamic queue only | Worker starts with no workflow files |
| Static and dynamic on different queues | Two workers, correct registrations |
| Static and dynamic on the same queue | One worker, named types plus one fallback |
| Repeated dynamic queue | One worker and one fallback |
| Neither files nor dynamic queue | Fail fast |
| Dynamic only plus watch | Fail fast with actionable message |

Use a recording worker or a narrow injected registration function in
`cmd/run` tests. Do not repurpose the package-private mocks from task tests.

### Slice 10. Real Temporal integration and replay gate

Owner surface:

- one new dynamic E2E fixture or example;
- a small dynamic worker launcher addition in `internal/e2etest/worker.go`, if
  using example-local E2E;
- one replay test file under `pkg/zigflow` or `pkg/zigflow/tasks`.

Tasks:

1. Start Zigflow with `--dynamic-task-queue` and no workflow file.
2. Start an arbitrary workflow type with the inline definition envelope.
3. Exercise Set or Switch, one built-in HTTP activity, For, Fork, Try, and a
   forced Continue-As-New. One compact workflow may cover several constructs.
4. Assert invalid input fails before a mock HTTP endpoint is called.
5. Capture a completed history and replay it with the dynamic handler
   registered on `worker.WorkflowReplayer`.
6. Restart the worker with changed source files or process env and prove replay
   uses the definition and env values recorded at start.

Acceptance:

- Tests pass against a real Temporal server, not only the testsuite.
- Static central and example E2E tests remain unchanged and pass.
- Replay performs no definition fetch and no external CloudEvents send.

### Slice 11. Documentation and terminology correction

Owner surface:

- documentation only;
- generated CLI reference through the repository's generator;
- Helm values and templates only if dynamic deployment is exposed there.

Tasks:

1. Document the dynamic worker flag, versioned input envelope, inline payload
   limits, validation timing, fixed built-in activity names, opt-in rollout,
   unsupported dynamic schedules, and external `run.workflow` requirement.
2. Update deployment guidance for a worker with no mounted workflow file.
3. Correct the execution model terminology. Say that Zigflow parses,
   validates, builds a closure tree, and interprets it at workflow runtime.
   Do not describe this as producing or deploying a compiled workflow
   artefact.
4. Audit at least:
   - `README.md`;
   - `docs/docs/intro.md`;
   - `docs/docs/architecture.md`;
   - `docs/docs/concepts/how-zigflow-runs.md`;
   - `docs/docs/concepts/overview.md`;
   - `docs/docs/concepts/durable-execution-in-yaml.md`;
   - `docs/docs/concepts/temporal-prereqs.md`;
   - `docs/docs/concepts/comparing-zigflow-and-temporal-sdks.md`;
   - `docs/docs/cli/using-the-cli.md`;
   - `docs/docs/deployment/intro.md`;
   - `docs/articles/2026-03-22-from-yaml-to-temporal-compiling-deterministic-workflows.md`;
   - `docs/articles/2026-03-24-why-i-built-a-yaml-dsl-for-temporal-workflows.md`.
5. Preserve legitimate uses such as compiling the Zigflow Go binary and the
   counterfactual discussion in the code-generation article.
6. Use British English, write Zigflow with a lowercase `f`, and do not use em
   dashes.

Acceptance:

- Public docs describe actual code behaviour and the new public contract.
- Examples are executable and use supported flags.
- Generated CLI docs are regenerated, not hand-edited.
- Markdown, link, spelling, and example validation checks pass.

## Deferred slices

Do not pull these into the first milestone.

### Definition reference resolver

Add a versioned `{reference: ...}` source variant only after deciding provider,
authentication, timeout, retry, maximum size, and integrity semantics. Resolve
through one fixed activity. Its byte result is the only snapshot used by all
children and Continue-As-New. Replay and children must not refetch.

### Dynamic schedules

Store canonical raw definition bytes in the file registration model and wrap
schedule action arguments in the dynamic envelope when a schedule explicitly
targets dynamic mode. Decide how schedule updates relate to a definition which
can also be supplied by an external starter.

### Workflow-safe CloudEvents

Replace direct sends from workflow code with activity commands or another
replay-safe boundary. Prove replay cannot duplicate an external send. This is
valuable beyond dynamic execution and should be designed as a separate public
behaviour change.

### Dynamic per-task activity observability

If fixed built-in activity names are insufficient, design a dynamic activity
dispatcher with an explicit transport discriminator. Do not infer the activity
implementation from an ambiguous per-task alias or from untyped payload shape.

## Cross-slice invariants

Every subagent must preserve these invariants:

- No mutable process-global definition or closure catalogue.
- No worker registration from workflow runtime.
- No file, network, wall-clock, random, environment, or CloudEvents operation
  from workflow code unless it uses the appropriate Temporal primitive.
- Validation completes before user tasks execute.
- Unsupported input, envelope versions, workflow types, and task constructs
  fail explicitly.
- Static workflow and activity names remain replay compatible.
- Definition and env snapshots survive every child workflow and
  Continue-As-New boundary.
- State lifecycle and promotion rules remain unchanged.
- `run.workflow` remains an external named Temporal call.

## Verification gates

After each slice:

1. Run focused tests for the owned package.
2. Run `go test ./...`.
3. Run `pre-commit run`.

At Slices 9 through 11, also run `task e2e`. The complete feature is not done
until all three repository commands pass:

```sh
pre-commit run
go test ./...
task e2e
```

Do not disable linters, weaken validation, or add `nolint` directives to make a
slice pass. If an existing static history changes commands without an explicit
and reviewed Temporal version gate, stop and treat it as a blocker.

## Final acceptance criteria

The milestone is complete only when all of the following are true:

1. One Zigflow worker can execute two arbitrary valid inline definitions on the
   same configured dynamic task queue without restart or per-definition
   registration.
2. The definition and env used at execution start are reconstructible from
   Temporal history and remain stable across replay, children, and
   Continue-As-New.
3. Set, Switch, Wait, Listen, Raise, external Call, built-in HTTP or gRPC or
   Run, For, Fork, Try, redirects, output, export, and state isolation retain
   their documented semantics.
4. Invalid or unsupported definitions fail before the first user task.
5. Dynamic building makes no SDK registration call.
6. Dynamic built-in activities schedule only names registered at worker
   startup.
7. Existing static workflow tests, replay tests, E2E tests, CLI behaviour, and
   documentation examples continue to pass.
8. Documentation describes Zigflow as a validated tree-walking interpreter,
   not a code generator or workflow artefact compiler.
