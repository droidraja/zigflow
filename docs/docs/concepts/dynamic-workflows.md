---
title: Dynamic workflows
sidebar_position: 11
description: "Run validated inline Zigflow definitions through an opt-in Temporal dynamic workflow worker."
---

Dynamic workflow execution lets a Temporal client supply a complete Zigflow
definition when it starts an execution. It is opt-in. File-backed static
workflow registration remains the default.

## Start a dynamic worker

Start a worker on one or more task queues with the repeatable
`--dynamic-task-queue` flag:

```sh
zigflow run --dynamic-task-queue dynamic-workflows
```

No `--file` or `--dir` value is required when at least one dynamic task queue
is configured. Repeated queue values are deduplicated.

A static definition and a dynamic fallback can share a task queue. A named
static registration takes precedence over the fallback, so existing static
workflow types and their input contracts remain unchanged. Zigflow never adds
a dynamic fallback to a static queue unless that queue is explicitly supplied
with `--dynamic-task-queue`.

## Public input contract

Dynamic executions accept one versioned input envelope:

```json
{
  "version": 1,
  "definition": "BASE64_ENCODED_YAML_OR_JSON_BYTES",
  "input": {
    "name": "Ada"
  }
}
```

| Field | Required | Meaning |
| --- | --- | --- |
| `version` | Yes | Public envelope version. The supported value is `1`. |
| `definition` | Yes | Complete YAML or JSON definition bytes. With the default JSON data converter, a Go `[]byte` is represented as a base64 string. |
| `input` | No | User input exposed to expressions as `$input`. The envelope itself is not exposed as `$input`. |

The Temporal workflow type must be executable from the supplied definition,
and `document.taskQueue` must match the queue on which the execution started.
For a normal single-root definition, start `document.workflowType`. For a
multiple-root top-level `do`, start one of the root task names built from that
definition.

## Starter example

Save this definition as `workflow.yaml`:

```yaml title="workflow.yaml"
document:
  dsl: 1.0.0
  taskQueue: dynamic-workflows
  workflowType: inline-greeting
  version: 1.0.0
do:
  - greet:
      output:
        as:
          message: ${ "Hello, " + $input.name }
      set:
        complete: true
```

Start the worker:

```sh
zigflow run --dynamic-task-queue dynamic-workflows
```

In another terminal, use `jq` to encode the definition bytes and construct the
versioned starter payload:

```sh
jq -n --rawfile definition workflow.yaml \
  --arg name Ada \
  '{
    version: 1,
    definition: ($definition | @base64),
    input: {name: $name}
  }' > dynamic-input.json

temporal workflow start \
  --type inline-greeting \
  --task-queue dynamic-workflows \
  --workflow-id inline-greeting-1 \
  --input "$(jq -c . dynamic-input.json)"
```

The complete envelope is one Temporal workflow start input. A client using a
Temporal SDK should construct the same three fields and encode `definition` as
bytes according to its configured data converter.

## Validation and snapshots

Before the first user task executes, Zigflow:

1. Decodes the envelope and rejects unsupported versions or an empty
   definition.
2. Parses the YAML or JSON and applies mandatory schema validation.
3. Normalises and decodes the workflow model.
4. Runs `PostLoad`, structural task validation, expression determinism
   validation and DSL version validation.
5. Verifies the task queue and requested workflow type.
6. Builds an execution-local closure tree and interprets the selected root.

Input, preparation, build and dispatch failures are non-retryable Temporal
application errors with the underlying validation message. Invalid definitions
fail before a user timer, activity or child workflow is scheduled.

The raw definition bytes are recorded in the Temporal workflow start event.
The same bytes are carried across Zigflow-generated container child workflows
and Continue-As-New, so replay does not reread a file or fetch a definition.

Environment variables selected by `--env-prefix` are read during worker
startup. The dynamic root records that captured map with a Temporal side effect
before user tasks execute. Child workflows and Continue-As-New carry the
recorded map, so replay does not reread the current process environment.

Configured CloudEvents delivery is disabled for dynamic definitions. Dynamic
workflow execution uses an explicit no-op event emitter because direct external
sends from workflow code are not replay-safe.

## Payload limits

Zigflow does not add a separate size limit for an inline definition. The
definition and user input are encoded into the Temporal workflow start payload
and count towards the payload, message and history limits configured for your
Temporal deployment. A data converter or codec can also change the encoded
size. The definition is also copied into internal child workflow and
Continue-As-New start inputs, so its size affects those histories. Check the
[Temporal workflow execution limits](https://docs.temporal.io/workflow-execution/limits)
for your deployment before accepting large definitions.

Definition references are not supported in this milestone. The complete
definition must be supplied inline.

## Activity names and external calls

Dynamic built-in tasks schedule the fixed activity names registered when the
worker starts:

- `CallHTTPActivity`
- `CallGRPCActivity`
- `CallContainerActivity`
- `CallScriptActivity`
- `CallShellActivity`

Static workflows keep their existing per-task aliases and replay compatibility.
The fixed dynamic names mean activity type names do not include the task name.
Custom `call: activity` tasks remain name-based and are unchanged.

A `run.workflow` task still targets an external named Temporal workflow. The
inline definition does not resolve or register that target. A worker must
register the target workflow separately on the requested task queue.

## Current constraints

- Dynamic schedules are not supported. A schedule in an inline definition is
  not registered. Start the dynamic execution through a Temporal client.
- Dynamic-only watch mode is rejected because there is no file to watch.
- Static and dynamic workers may use watch mode only when their task queues are
  separate. A shared queue cannot reload the static registration without also
  recreating the dynamic fallback.
- Dynamic built-in tasks do not emit configured CloudEvents.

These constraints do not change file-backed static workflow behaviour.

## Related pages

- [Using the CLI](/docs/cli/using-the-cli): common worker commands
- [How Zigflow runs](/docs/concepts/how-zigflow-runs): execution model
- [Deployment overview](/docs/deployment/intro): running without a mounted file
- [Dynamic workflow example](/docs/examples/dynamic-workflow): repository E2E fixture
