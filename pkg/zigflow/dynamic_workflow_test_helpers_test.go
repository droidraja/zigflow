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
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

// dynamicWorkflowTestHarness runs one dynamic workflow fallback entirely in
// Temporal's in-memory testsuite. It also records the activity and child
// workflow type names scheduled by the execution for later assertions.
type dynamicWorkflowTestHarness struct {
	testing.TB
	env                *testsuite.TestWorkflowEnvironment
	activityNames      []string
	childWorkflowNames []string
}

// newDynamicWorkflowTestHarness registers handler as the environment's one
// dynamic workflow fallback. Callers may register named activities or child
// workflows on the returned environment before executing the root workflow.
func newDynamicWorkflowTestHarness(t testing.TB, handler any) *dynamicWorkflowTestHarness {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	harness := &dynamicWorkflowTestHarness{
		TB:  t,
		env: suite.NewTestWorkflowEnvironment(),
	}
	harness.env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		harness.activityNames = append(harness.activityNames, info.ActivityType.Name)
	})
	harness.env.SetOnChildWorkflowStartedListener(func(info *workflow.Info, _ workflow.Context, _ converter.EncodedValues) {
		harness.childWorkflowNames = append(harness.childWorkflowNames, info.WorkflowType.Name)
	})
	harness.env.RegisterDynamicWorkflow(handler, workflow.DynamicRegisterOptions{})

	return harness
}

// execute runs workflowType through the registered dynamic fallback. The type
// is deliberately a string so tests are not limited to statically registered
// Go workflow functions.
func (h *dynamicWorkflowTestHarness) execute(workflowType string, args ...any) {
	h.Helper()
	h.env.ExecuteWorkflow(workflowType, args...)
}

// requireWorkflowResult decodes and compares the completed workflow result.
func requireWorkflowResult[T any](t testing.TB, env *testsuite.TestWorkflowEnvironment, expected T) {
	t.Helper()
	require.NoError(t, env.GetWorkflowError())

	var actual T
	require.NoError(t, env.GetWorkflowResult(&actual))
	require.Equal(t, expected, actual)
}

// requireNonRetryableApplicationError returns the application error produced
// by a failed workflow after asserting that Temporal marked it non-retryable.
func requireNonRetryableApplicationError(
	t testing.TB,
	env *testsuite.TestWorkflowEnvironment,
) *temporal.ApplicationError {
	t.Helper()

	err := env.GetWorkflowError()
	require.Error(t, err)

	var applicationError *temporal.ApplicationError
	require.True(t, errors.As(err, &applicationError), "workflow error must contain a Temporal ApplicationError")
	require.True(t, applicationError.NonRetryable(), "application error must be non-retryable")

	return applicationError
}

func requireScheduledActivityNames(t testing.TB, harness *dynamicWorkflowTestHarness, expected ...string) {
	t.Helper()
	require.Equal(t, expected, harness.activityNames)
}

func requireScheduledChildWorkflowNames(t testing.TB, harness *dynamicWorkflowTestHarness, expected ...string) {
	t.Helper()
	require.Equal(t, expected, harness.childWorkflowNames)
}

func loadDynamicWorkflowFixture(t testing.TB, name string) []byte {
	t.Helper()

	definition, err := os.ReadFile(filepath.Join("testdata", "dynamic", name))
	require.NoError(t, err)
	require.NotEmpty(t, definition)

	return definition
}
