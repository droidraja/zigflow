# Dynamic Workflow Execution

Exercise the opt-in dynamic workflow path against a real Temporal server.

<!-- toc -->

* [Run the E2E fixture](#run-the-e2e-fixture)
* [What this proves](#what-this-proves)
* [Diagram](#diagram)

<!-- Regenerate with "pre-commit run -a markdown-toc" -->

<!-- tocstop -->

## Run the E2E fixture

From the repository root, run:

```sh
task e2e
```

The fixture starts Zigflow with `--dynamic-task-queue dynamic-e2e` and no
workflow file. The test replaces the placeholder HTTP endpoint in
[`workflow.yaml`](./workflow.yaml) with a local recorder before starting the
execution.

## What this proves

The E2E test starts an arbitrary workflow type with a versioned inline
definition, exercises Set, Switch, HTTP, For, Fork, Try and Continue-As-New,
then replays the completed histories. It also proves that invalid input fails
before the HTTP endpoint is called and that replay uses the definition and
environment recorded at execution start.

See the
[dynamic workflow documentation](https://zigflow.dev/docs/concepts/dynamic-workflows)
for the public input envelope and a manual starter payload.

## Diagram

<!-- ZIGFLOW_GRAPH_START -->
```mermaid
flowchart TD
    arbitrary_dynamic_workflow__start([Start])
    arbitrary_dynamic_workflow__end([End])
    arbitrary_dynamic_workflow_initialise["SET (initialise)"]
    arbitrary_dynamic_workflow__start --> arbitrary_dynamic_workflow_initialise
    arbitrary_dynamic_workflow_select{"SWITCH (select)"}
    arbitrary_dynamic_workflow_select -->|"default"| arbitrary_dynamic_workflow__end
    arbitrary_dynamic_workflow_initialise --> arbitrary_dynamic_workflow_select
    arbitrary_dynamic_workflow_request["CALL_HTTP (request)"]
    arbitrary_dynamic_workflow_select --> arbitrary_dynamic_workflow_request
    arbitrary_dynamic_workflow_iterate[["FOR (iterate)"]]
    subgraph body_iterate["iterate (loop body)"]
        direction TB
        arbitrary_dynamic_workflow_iterate_body__start([ ])
        arbitrary_dynamic_workflow_iterate_body_capture["SET (capture)"]
        arbitrary_dynamic_workflow_iterate_body__start --> arbitrary_dynamic_workflow_iterate_body_capture
    end
    arbitrary_dynamic_workflow_iterate --> arbitrary_dynamic_workflow_iterate_body__start
    arbitrary_dynamic_workflow_iterate_body_capture -->|"next iteration"| arbitrary_dynamic_workflow_iterate
    arbitrary_dynamic_workflow_request --> arbitrary_dynamic_workflow_iterate
    arbitrary_dynamic_workflow_parallel["FORK (parallel)"]
    arbitrary_dynamic_workflow_parallel__join((" "))
    subgraph fork_arbitrary_dynamic_workflow_first["first"]
        direction TB
        arbitrary_dynamic_workflow_first__start([ ])
        arbitrary_dynamic_workflow_first__end([ ])
        arbitrary_dynamic_workflow_first__start --> arbitrary_dynamic_workflow_first__end
    end
    arbitrary_dynamic_workflow_parallel --> arbitrary_dynamic_workflow_first__start
    arbitrary_dynamic_workflow_first__end --> arbitrary_dynamic_workflow_parallel__join
    subgraph fork_arbitrary_dynamic_workflow_second["second"]
        direction TB
        arbitrary_dynamic_workflow_second__start([ ])
        arbitrary_dynamic_workflow_second__end([ ])
        arbitrary_dynamic_workflow_second__start --> arbitrary_dynamic_workflow_second__end
    end
    arbitrary_dynamic_workflow_parallel --> arbitrary_dynamic_workflow_second__start
    arbitrary_dynamic_workflow_second__end --> arbitrary_dynamic_workflow_parallel__join
    arbitrary_dynamic_workflow_iterate --> arbitrary_dynamic_workflow_parallel
    subgraph try_guarded["TRY (guarded)"]
        direction TB
        arbitrary_dynamic_workflow_guarded_try__start([ ])
        arbitrary_dynamic_workflow_guarded_try__end([ ])
        arbitrary_dynamic_workflow_guarded_try_fail["RAISE (fail)"]
        arbitrary_dynamic_workflow_guarded_try__start --> arbitrary_dynamic_workflow_guarded_try_fail
        arbitrary_dynamic_workflow_guarded_try_fail --> arbitrary_dynamic_workflow_guarded_try__end
    end
    subgraph catch_guarded["CATCH (guarded)"]
        direction TB
        arbitrary_dynamic_workflow_guarded_catch__start([ ])
        arbitrary_dynamic_workflow_guarded_catch__end([ ])
        arbitrary_dynamic_workflow_guarded_catch_recover["SET (recover)"]
        arbitrary_dynamic_workflow_guarded_catch__start --> arbitrary_dynamic_workflow_guarded_catch_recover
        arbitrary_dynamic_workflow_guarded_catch_recover --> arbitrary_dynamic_workflow_guarded_catch__end
    end
    arbitrary_dynamic_workflow_guarded_try__end -.->|"on error"| arbitrary_dynamic_workflow_guarded_catch__start
    arbitrary_dynamic_workflow_parallel__join --> arbitrary_dynamic_workflow_guarded_try__start
    arbitrary_dynamic_workflow_finish["SET (finish)"]
    arbitrary_dynamic_workflow_guarded_try__end --> arbitrary_dynamic_workflow_finish
    arbitrary_dynamic_workflow_finish --> arbitrary_dynamic_workflow__end
```
<!-- ZIGFLOW_GRAPH_END -->
