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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/zigflow"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	dynamicProbeActivityType = "slice-0-probe-activity"
	dynamicProbeChildType    = "slice-0-probe-child"
)

type dynamicProbeResult struct {
	WorkflowType string `json:"workflowType"`
	Input        string `json:"input"`
}

func dynamicWorkflowProbe(ctx workflow.Context, args converter.EncodedValues) (any, error) {
	var input string
	if err := args.Get(&input); err != nil {
		return nil, temporal.NewNonRetryableApplicationError(
			"probe could not decode input",
			"DynamicWorkflowProbeInputError",
			err,
		)
	}

	return dynamicProbeResult{
		WorkflowType: workflow.GetInfo(ctx).WorkflowType.Name,
		Input:        input,
	}, nil
}

func TestDynamicWorkflowTestsuiteExecutesArbitraryWorkflowType(t *testing.T) {
	harness := newDynamicWorkflowTestHarness(t, dynamicWorkflowProbe)
	harness.execute("workflow-type-with-no-static-registration", "probe input")

	requireWorkflowResult(t, harness.env, dynamicProbeResult{
		WorkflowType: "workflow-type-with-no-static-registration",
		Input:        "probe input",
	})
}

func TestDynamicWorkflowHarnessAssertsNonRetryableApplicationError(t *testing.T) {
	harness := newDynamicWorkflowTestHarness(t, dynamicWorkflowProbe)
	harness.execute("probe-invalid-input", 42)

	applicationError := requireNonRetryableApplicationError(t, harness.env)
	require.Equal(t, "DynamicWorkflowProbeInputError", applicationError.Type())
}

func TestDynamicWorkflowHarnessRecordsScheduledTypeNames(t *testing.T) {
	handler := func(ctx workflow.Context, _ converter.EncodedValues) (any, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Second,
		})

		var activityResult string
		if err := workflow.ExecuteActivity(ctx, dynamicProbeActivityType).Get(ctx, &activityResult); err != nil {
			return nil, err
		}

		var childResult string
		if err := workflow.ExecuteChildWorkflow(ctx, dynamicProbeChildType).Get(ctx, &childResult); err != nil {
			return nil, err
		}

		return activityResult + childResult, nil
	}

	harness := newDynamicWorkflowTestHarness(t, handler)
	harness.env.RegisterActivityWithOptions(
		func() (string, error) { return "activity", nil },
		activity.RegisterOptions{Name: dynamicProbeActivityType},
	)
	harness.env.RegisterWorkflowWithOptions(
		func(workflow.Context) (string, error) { return "child", nil },
		workflow.RegisterOptions{Name: dynamicProbeChildType},
	)
	harness.execute("probe-scheduled-types")

	requireWorkflowResult(t, harness.env, "activitychild")
	requireScheduledActivityNames(t, harness, dynamicProbeActivityType)
	requireScheduledChildWorkflowNames(t, harness, dynamicProbeChildType)
}

func TestDynamicWorkflowFixturesHaveExpectedValidationShape(t *testing.T) {
	validFixtures := []string{
		"set-switch.yaml",
		"continue-as-new.yaml",
		"for.yaml",
		"fork.yaml",
		"try.yaml",
		"built-in-http.yaml",
	}

	for _, fixture := range validFixtures {
		t.Run(fixture, func(t *testing.T) {
			require.NoError(t, zigflow.ValidateBytes(loadDynamicWorkflowFixture(t, fixture)))
		})
	}

	t.Run("invalid schema", func(t *testing.T) {
		err := zigflow.ValidateBytes(loadDynamicWorkflowFixture(t, "invalid-schema.yaml"))
		require.ErrorIs(t, err, zigflow.ErrSchemaValidation)
	})

	t.Run("unsupported task shape", func(t *testing.T) {
		err := zigflow.ValidateBytes(loadDynamicWorkflowFixture(t, "unsupported-task.yaml"))
		require.Error(t, err)
		require.False(t, errors.Is(err, zigflow.ErrSchemaValidation))
		require.ErrorContains(t, err, "multiple switch statements without when")
	})
}
