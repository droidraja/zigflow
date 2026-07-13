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
	dynamicTryEnvKey        = "DEPLOYMENT"
	dynamicTryOriginalInput = "original"
	dynamicTryRecordedEnv   = "recorded"
	dynamicTryRootID        = "dynamic-try-root-id"
)

func TestDynamicWorkflowTryRunsCatchWithRecordedInvocationAndErrorData(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-try-catch", `
  - initialise:
      set:
        parent: preserved
  - guarded:
      try:
        - fail:
            raise:
              error:
                type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                status: 400
      catch:
        as: failure
        do:
          - recover:
              output:
                as:
                  caught: true
                  errorType: ${ $data.failure.type }
                  errorMessage: ${ $data.failure.message }
                  workflowID: ${ $data.failure.childWorkflow.workflowID }
                  workflowType: ${ $data.failure.childWorkflow.workflowType }
              set:
                recovered: true`)
	input := map[string]any{"request": dynamicTryOriginalInput}
	recordedEnv := map[string]any{dynamicTryEnvKey: dynamicTryRecordedEnv}
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{
		Envvars: recordedEnv,
	}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{
		ID:        dynamicTryRootID,
		TaskQueue: dynamicTestTaskQueue,
	})

	var childNames []string
	var invocations []tasks.InternalWorkflowInvocation
	var decodeErrors []error
	harness.env.SetOnChildWorkflowStartedListener(
		func(info *workflow.Info, _ workflow.Context, args converter.EncodedValues) {
			childNames = append(childNames, info.WorkflowType.Name)
			var invocation tasks.InternalWorkflowInvocation
			if err := args.Get(&invocation); err != nil {
				decodeErrors = append(decodeErrors, err)
				return
			}
			invocations = append(invocations, invocation)
		},
	)
	harness.execute("dynamic-try-catch", dynamicInputWithUserInput(definition, input))

	require.NoError(t, harness.env.GetWorkflowError())
	var result map[string]any
	require.NoError(t, harness.env.GetWorkflowResult(&result))
	require.Equal(t, true, result["caught"])
	require.NotEmpty(t, result["errorType"])
	require.NotEmpty(t, result["errorMessage"])
	require.Equal(t, dynamicTryRootID+"_try", result["workflowID"])
	require.Equal(t, "workflow_try_guarded", result["workflowType"])

	require.Empty(t, decodeErrors)
	require.Equal(t, []string{"workflow_try_guarded", "workflow_catch_guarded"}, childNames)
	require.Len(t, invocations, 2)
	for _, invocation := range invocations {
		require.Equal(t, tasks.InternalWorkflowInvocationVersion, invocation.Version)
		require.Equal(t, definition, invocation.Definition)
		require.Equal(t, recordedEnv, invocation.RecordedEnv)
		require.Equal(t, input, invocation.OriginalInput)
		require.Equal(t, input, invocation.State.Input)
		require.Equal(t, recordedEnv, invocation.State.Env)
		require.Equal(t, "preserved", invocation.State.Data["parent"])
	}
	require.NotContains(t, invocations[0].State.Data, "failure")
	caughtError, ok := invocations[1].State.Data["failure"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, result["errorType"], caughtError["type"])
	require.Equal(t, result["errorMessage"], caughtError["message"])
	require.Contains(t, caughtError, "childWorkflow")
}

func TestDynamicWorkflowCatchDataDoesNotLeakWithoutOutputOrExport(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-try-isolation", `
  - initialise:
      set:
        parent: preserved
  - guarded:
      try:
        - fail:
            raise:
              error:
                type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                status: 400
      catch:
        as: failure
        do:
          - recover:
              set:
                catchOnly: true
  - inspect:
      output:
        as:
          catchOnly: ${ $data.catchOnly }
          failure: ${ $data.failure }
          parent: ${ $data.parent }
      set:
        inspected: true`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-try-isolation", dynamicInput(definition))

	requireWorkflowResult(t, harness.env, map[string]any{
		"catchOnly": nil,
		"failure":   nil,
		"parent":    "preserved",
	})
}

func TestDynamicWorkflowTryEndBypassesCatchAndReachesRoot(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-try-end", `
  - guarded:
      try:
        - finish:
            output:
              as:
                ended: true
                source: try
            then: end
            set:
              ended: true
      catch:
        do:
          - must-not-run:
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
	harness.execute("dynamic-try-end", dynamicInput(definition))

	require.NoError(t, harness.env.GetWorkflowError())
	requireScheduledChildWorkflowNames(t, harness, "workflow_try_guarded")
}

func TestDynamicWorkflowCatchEndReachesRoot(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-catch-end", `
  - guarded:
      try:
        - fail:
            raise:
              error:
                type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
                status: 400
      catch:
        as: failure
        do:
          - finish:
              output:
                as:
                  ended: true
                  errorType: ${ $data.failure.type }
                  source: catch
              then: end
              set:
                ended: true
  - must-not-run:
      raise:
        error:
          type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
          status: 500`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-catch-end", dynamicInput(definition))

	require.NoError(t, harness.env.GetWorkflowError())
	requireScheduledChildWorkflowNames(
		t, harness,
		"workflow_try_guarded",
		"workflow_catch_guarded",
	)
}
