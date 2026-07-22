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

package tasks

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	zigflowModels "github.com/zigflow/zigflow/pkg/zigflow/models"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// TestRunWorkflowExtDefaultsAwaitAcrossBuilderReconstruction reproduces the
// real DoTaskBuilder lifecycle: PostLoad, Validate, and Build each construct a
// FRESH builder over the same parsed task. PostLoad's Await default must
// therefore land on the shared extension model, not on builder-local state —
// the Build-phase closure dereferences *Await and panicked with a nil pointer
// when the authored YAML omitted `await:` (the ether demo regression).
func TestRunWorkflowExtDefaultsAwaitAcrossBuilderReconstruction(t *testing.T) {
	const childType = "reconstruction-child"
	task := &zigflowModels.RunWorkflowExtTask{
		Run: zigflowModels.RunWorkflowExtConfiguration{
			// Await deliberately nil: the authored YAML omitted `await:`.
			Workflow: zigflowModels.RunWorkflowExt{
				Type:      childType,
				TaskQueue: "agent-workers",
			},
		},
	}

	prepBuilder, err := NewRunWorkflowExtTaskBuilder(nil, task, "run-agent", nil, testEvents, nil)
	require.NoError(t, err)
	require.NoError(t, prepBuilder.PostLoad())
	require.NotNil(t, task.Run.Await, "PostLoad must default Await on the shared extension model")
	require.True(t, *task.Run.Await, "omitted await must default to true")

	validateBuilder, err := NewRunWorkflowExtTaskBuilder(nil, task, "run-agent", nil, testEvents, nil)
	require.NoError(t, err)
	require.NoError(t, validateBuilder.Validate())

	buildBuilder, err := NewRunWorkflowExtTaskBuilder(nil, task, "run-agent", nil, testEvents, nil)
	require.NoError(t, err)
	fn, err := buildBuilder.Build()
	require.NoError(t, err)

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(
		func(_ workflow.Context, request map[string]any) (map[string]any, error) {
			return map[string]any{"reconstructed": true}, nil
		},
		workflow.RegisterOptions{Name: childType},
	)
	state := utils.NewState()
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, map[string]any{}, state)
	}, workflow.RegisterOptions{Name: "reconstruction-host"})

	env.ExecuteWorkflow("reconstruction-host")
	require.NoError(t, env.GetWorkflowError(), "Build-phase closure must not nil-panic on the defaulted Await")

	var result map[string]any
	require.NoError(t, env.GetWorkflowResult(&result))
	require.Equal(t, map[string]any{"reconstructed": true}, result)
}
