---
title: "Durable execution in YAML"
sidebar_position: 10
description: "How Zigflow expresses durable execution in YAML by validating and interpreting declarative workflow definitions on Temporal."
---

Durable execution is the property that a workflow continues to run correctly
across process crashes, host restarts and infrastructure failures. The
execution is durable because the runtime records progress as the workflow
runs. If the worker dies and resumes, the workflow continues from where it
left off, not from the beginning.

Zigflow brings this property to workflows defined in YAML.

## How Temporal provides durable execution

Temporal is a durable execution platform. When a workflow runs on Temporal,
the platform records every step it takes as a sequence of events called a
history. The history is the source of truth for the workflow.

If the worker process restarts:

- Temporal reschedules the workflow onto a healthy worker.
- The worker replays the recorded history.
- Steps that already produced a result are replayed without re-executing
  the underlying side effect.
- The workflow resumes at the point the history ends, with the same
  in-memory state it had before the failure.

For replay to work, the workflow code must be deterministic. Given the same
history, replaying it must produce the same sequence of decisions. Side
effects such as network calls, wall-clock reads, randomness and file I/O
belong in activities, where Temporal records the result and skips
re-execution on replay.

## How Zigflow expresses it in YAML

A Zigflow workflow is YAML or JSON data that describes the steps to run. Zigflow
loads the definition, validates it and builds a deterministic task closure
tree. At workflow runtime, Zigflow walks that tree and interprets each task
against workflow state. Temporal records the commands and results with the
same durability guarantees as an SDK-defined workflow.

Determinism is structural in Zigflow rather than a discipline applied by
the author. Zigflow enforces this in three ways:

1. **Workflow logic is data.** Control flow, branching and iteration are
   expressed as task structures such as `do`, `for`, `switch`, `try` and
   `fork`. The same YAML produces the same workflow shape on every replay.
2. **Side effects are activities.** Anything that touches the outside world
   is modelled as a Temporal activity. Activity results are recorded once
   and replayed from history.
3. **Validation runs before execution.** Unsupported constructs and
   non-deterministic patterns are rejected at validation time, not when the
   worker is already serving traffic.

A minimal example:

```yaml title="workflow.yaml"
document:
  dsl: 1.0.0
  taskQueue: zigflow
  workflowType: charge-customer
  version: 1.0.0
do:
  - validateOrder:
      call: http
      with:
        method: post
        endpoint: https://api.example.com/orders/validate
  - chargeCard:
      call: http
      with:
        method: post
        endpoint: https://api.example.com/payments
  - notifyUser:
      call: http
      with:
        method: post
        endpoint: https://api.example.com/notifications
```

Each `call` is executed as a Temporal activity. If the worker crashes
between `chargeCard` and `notifyUser`, the workflow is rescheduled on a
healthy worker, the history is replayed up to `chargeCard`'s recorded
completion, the card is not charged again and the workflow proceeds to
`notifyUser`.

## What this enables

Expressing durable execution in YAML has several practical implications:

- **No SDK boilerplate.** The workflow definition is the entire workflow.
  There is no project to scaffold, no language toolchain to manage and no
  generated workflow artefact to ship beyond the YAML file.
- **Validation as a first-class step.** Errors that an SDK would surface
  only when a workflow runs are reported by `zigflow validate` before
  deployment.
- **Reviewable as configuration.** Workflows can be diffed, reviewed and
  authored by people outside the engineering team, without losing the
  durability guarantees Temporal provides.
- **Same operational model as code-defined workflows.** Workers, task
  queues, retries, timeouts and signals behave exactly as they would for an
  SDK-defined workflow. Zigflow does not introduce a separate execution
  model.

## Summary

Durable execution is a property of the workflow runtime, not of the
language used to describe the workflow. Temporal supplies the runtime.
Zigflow supplies a declarative YAML surface that it validates, builds into a
closure tree and interprets on that runtime. Determinism is enforced
structurally and validation is performed before user tasks execute. The result
is a workflow that is durable, observable and replayable, defined entirely in
configuration.
