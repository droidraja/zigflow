/*
 * Copyright 2025 - 2026 Zigflow authors <https://github.com/zigflow/zigflow/graphs/contributors>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zigflow_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/zigflow"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/workflow"
)

const (
	dynamicForkBranchOnlyKey = "branchOnly"
	dynamicForkEnvKey        = "DEPLOYMENT"
	dynamicForkFirstBranch   = "first"
	dynamicForkPreserved     = "preserved"
	dynamicForkRootID        = "dynamic-fork-root-id"
	dynamicForkRootKey       = "root"
	dynamicForkSecondBranch  = "second"
	dynamicForkSourceKey     = "source"
)

func TestDynamicWorkflowForkPropagatesSnapshotAndIsolatesBranchState(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-fork-isolation", `
  - initialise:
      export:
        as:
          root: preserved
      set:
        shared: parent
  - parallel:
      fork:
        compete: false
        branches:
          - first:
              set:
                branchOnly: first
          - second:
              set:
                branchOnly: second
  - inspect:
      output:
        as:
          aggregate: ${ $output }
          branchOnly: ${ $data.branchOnly }
          context: ${ $context }
          shared: ${ $data.shared }
      set:
        inspected: true`)
	input := map[string]any{"request": "original"}
	recordedEnv := map[string]any{dynamicForkEnvKey: "recorded"}
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{
		Envvars: recordedEnv,
	}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID:        dynamicForkRootID,
		TaskQueue: dynamicTestTaskQueue,
	})

	var childNames []string
	var childIDs []string
	var invocations []tasks.InternalWorkflowInvocation
	var decodeErrors []error
	harness.env.SetOnChildWorkflowStartedListener(
		func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
			childNames = append(childNames, info.WorkflowType.Name)
			childIDs = append(childIDs, info.WorkflowExecution.ID)
			var invocation tasks.InternalWorkflowInvocation
			if err := args.Get(&invocation); err != nil {
				decodeErrors = append(decodeErrors, err)
				return
			}
			invocations = append(invocations, invocation)
		},
	)
	harness.execute("dynamic-fork-isolation", dynamicInputWithUserInput(definition, input))

	requireWorkflowResult(t, harness.env, map[string]any{
		"aggregate": map[string]any{
			dynamicForkFirstBranch:  map[string]any{dynamicForkBranchOnlyKey: dynamicForkFirstBranch},
			dynamicForkSecondBranch: map[string]any{dynamicForkBranchOnlyKey: dynamicForkSecondBranch},
		},
		dynamicForkBranchOnlyKey: nil,
		"context":                map[string]any{dynamicForkRootKey: dynamicForkPreserved},
		"shared":                 "parent",
	})
	require.Empty(t, decodeErrors)
	require.ElementsMatch(t, []string{
		"workflow_fork_parallel_first",
		"workflow_fork_parallel_second",
	}, childNames)
	require.ElementsMatch(t, []string{
		dynamicForkRootID + "_fork_first",
		dynamicForkRootID + "_fork_second",
	}, childIDs)
	require.Len(t, invocations, 2)
	require.NotSame(t, invocations[0].State, invocations[1].State)
	for _, invocation := range invocations {
		require.Equal(t, tasks.InternalWorkflowInvocationVersion, invocation.Version)
		require.Equal(t, definition, invocation.Definition)
		require.Equal(t, recordedEnv, invocation.RecordedEnv)
		require.Equal(t, input, invocation.OriginalInput)
		require.Equal(t, input, invocation.State.Input)
		require.Equal(t, recordedEnv, invocation.State.Env)
		require.Equal(t, map[string]any{dynamicForkRootKey: dynamicForkPreserved}, invocation.State.Context)
		require.Equal(t, "parent", invocation.State.Data["shared"])
		require.Nil(t, invocation.State.Data[dynamicForkBranchOnlyKey])
		require.Nil(t, invocation.State.Output)
	}
}

func TestDynamicWorkflowCompetingForkReturnsWinner(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-fork-compete", `
  - race:
      fork:
        compete: true
        branches:
          - winner:
              set:
                winner: first`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-fork-compete", dynamicInput(definition))

	requireWorkflowResult(t, harness.env, map[string]any{"winner": "first"})
	requireScheduledChildWorkflowNames(
		t, harness,
		"workflow_fork_race_winner",
	)
}

func TestDynamicWorkflowForkWrapsBranchFailure(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-fork-failure", `
  - parallel:
      fork:
        compete: false
        branches:
          - failure:
              raise:
                error:
                  type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                  status: 500`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-fork-failure", dynamicInput(definition))

	require.ErrorContains(t, harness.env.GetWorkflowError(), "error forking task")
}

func TestDynamicWorkflowForkFailureTakesPrecedenceOverEnd(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-fork-error-precedence", `
  - parallel:
      fork:
        compete: false
        branches:
          - failure:
              raise:
                error:
                  type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                  status: 500
          - ending:
              then: end
              set:
                ended: true`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-fork-error-precedence", dynamicInput(definition))

	require.ErrorContains(t, harness.env.GetWorkflowError(), "error forking task")
}

func TestDynamicWorkflowForkEndReachesRootWithEffectiveOutput(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-fork-end", `
  - parallel:
      output:
        as: ${ . }
      fork:
        compete: false
        branches:
          - ending:
              output:
                as:
                  ended: true
                  source: fork
              then: end
              set:
                ended: true
          - must-not-contribute:
              do:
                - wait:
                    wait:
                      hours: 1
                - fail:
                    raise:
                      error:
                        type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                        status: 500
  - must-not-run:
      raise:
        error:
          type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
          status: 500`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-fork-end", dynamicInput(definition))

	requireWorkflowResult(t, harness.env, map[string]any{
		"ended":              true,
		dynamicForkSourceKey: "fork",
	})
}
