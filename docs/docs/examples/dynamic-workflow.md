---
sidebar_position: 6
---

# Dynamic Workflow

Run an inline workflow definition on an opt-in dynamic task queue and replay it
from Temporal history.

## What this example covers

The repository fixture in
[`examples/dynamic-workflow`](https://github.com/zigflow/zigflow/tree/main/examples/dynamic-workflow)
starts Zigflow with `--dynamic-task-queue` and no workflow file. Its E2E test:

- Starts an arbitrary workflow type with a version `1` input envelope
- Runs Set, Switch, HTTP, For, Fork and Try tasks
- Forces Continue-As-New
- Verifies invalid input fails before the HTTP endpoint is called
- Restarts the worker with a changed environment
- Replays both runs from history with the recorded definition and environment

The fixture replaces its placeholder HTTP endpoint with a local recorder during
the test. Run it as part of the complete E2E suite:

```sh
task e2e
```

For a small manual definition and an executable Temporal CLI starter payload,
see [Dynamic workflows](/docs/concepts/dynamic-workflows#starter-example).

## Related pages

- [Dynamic workflows](/docs/concepts/dynamic-workflows): public contract and constraints
- [HTTP Call](/docs/examples/http-call): built-in HTTP activity
- [How Zigflow runs](/docs/concepts/how-zigflow-runs): execution and replay
