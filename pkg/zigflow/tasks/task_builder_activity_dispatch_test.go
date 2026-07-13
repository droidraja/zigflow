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
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"github.com/zigflow/zigflow/pkg/zigflow/activities"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const testDispatchShellCommand = "echo hello"

type builtInActivityDispatchCase struct {
	name      string
	fixedName string
	build     func(testing.TB, worker.Worker, *TaskOpts) TemporalWorkflowFunc
	register  func(*testsuite.TestWorkflowEnvironment, string)
}

func builtInActivityDispatchCases() []builtInActivityDispatchCase {
	return []builtInActivityDispatchCase{
		{
			name:      "HTTP",
			fixedName: legacyCallHTTPActivityName,
			build: func(t testing.TB, temporalWorker worker.Worker, opts *TaskOpts) TemporalWorkflowFunc {
				t.Helper()
				builder, err := NewCallHTTPTaskBuilder(
					temporalWorker, newTestHTTPTask(), "step", dispatchTestWorkflow(), testEvents, opts,
				)
				require.NoError(t, err)
				fn, err := builder.Build()
				require.NoError(t, err)
				return fn
			},
			register: registerCallHTTPActivity,
		},
		{
			name:      "gRPC",
			fixedName: legacyCallGRPCActivityName,
			build: func(t testing.TB, temporalWorker worker.Worker, opts *TaskOpts) TemporalWorkflowFunc {
				t.Helper()
				builder, err := NewCallGRPCTaskBuilder(
					temporalWorker, newTestGRPCTask(), "step", dispatchTestWorkflow(), testEvents, opts,
				)
				require.NoError(t, err)
				fn, err := builder.Build()
				require.NoError(t, err)
				return fn
			},
			register: registerCallGRPCActivity,
		},
		{
			name:      "container",
			fixedName: legacyCallContainerActivityName,
			build:     buildContainerDispatch,
			register:  registerCallContainerActivity,
		},
		{
			name:      "script",
			fixedName: legacyCallScriptActivityName,
			build:     buildScriptDispatch,
			register:  registerCallRunActivity,
		},
		{
			name:      "shell",
			fixedName: legacyCallShellActivityName,
			build:     buildShellDispatch,
			register:  registerCallRunActivity,
		},
	}
}

func TestDynamicBuiltInActivityDispatchUsesFixedRegisteredNames(t *testing.T) {
	registeredMethodNames := activityRegistryMethodNames()

	for _, tc := range builtInActivityDispatchCases() {
		t.Run(tc.name, func(t *testing.T) {
			require.Contains(t, registeredMethodNames, tc.fixedName)

			workerMock := new(WorkflowRegistryMock)
			fn := tc.build(t, workerMock, &TaskOpts{ActivityDispatchPolicy: ActivityDispatchDynamic})
			workerMock.AssertNotCalled(t, "RegisterActivityWithOptions", mock.Anything, mock.Anything)

			activityNames := executeDispatchWorkflow(t, fn, tc.fixedName, tc.register)
			require.Equal(t, []string{tc.fixedName}, activityNames)
		})
	}
}

func TestStaticBuiltInActivityDispatchUsesPerTaskAliases(t *testing.T) {
	const perTaskName = "dispatch-test.step"

	for _, tc := range builtInActivityDispatchCases() {
		t.Run(tc.name, func(t *testing.T) {
			fn := tc.build(t, nil, nil)
			activityNames := executeDispatchWorkflow(t, fn, perTaskName, tc.register)
			require.Equal(t, []string{perTaskName}, activityNames)
		})
	}
}

func dispatchTestWorkflow() *model.Workflow {
	return &model.Workflow{Document: model.Document{Name: "dispatch-test"}}
}

func buildContainerDispatch(t testing.TB, temporalWorker worker.Worker, opts *TaskOpts) TemporalWorkflowFunc {
	t.Helper()
	return buildRunDispatch(t, temporalWorker, opts, &model.RunTask{
		Run: model.RunTaskConfiguration{
			Await:     utils.Ptr(true),
			Container: &model.Container{Image: "busybox:latest"},
		},
	})
}

func buildScriptDispatch(t testing.TB, temporalWorker worker.Worker, opts *TaskOpts) TemporalWorkflowFunc {
	t.Helper()
	return buildRunDispatch(t, temporalWorker, opts, &model.RunTask{
		Run: model.RunTaskConfiguration{
			Await: utils.Ptr(true),
			Script: &model.Script{
				Language:   constScriptLanguagePython,
				InlineCode: utils.Ptr("print(1)"),
			},
		},
	})
}

func buildShellDispatch(t testing.TB, temporalWorker worker.Worker, opts *TaskOpts) TemporalWorkflowFunc {
	t.Helper()
	return buildRunDispatch(t, temporalWorker, opts, &model.RunTask{
		Run: model.RunTaskConfiguration{
			Await: utils.Ptr(true),
			Shell: &model.Shell{Command: testDispatchShellCommand},
		},
	})
}

func buildRunDispatch(
	t testing.TB,
	temporalWorker worker.Worker,
	opts *TaskOpts,
	task *model.RunTask,
) TemporalWorkflowFunc {
	t.Helper()
	builder, err := NewRunTaskBuilder(temporalWorker, task, "step", dispatchTestWorkflow(), testEvents, opts)
	require.NoError(t, err)
	fn, err := builder.Build()
	require.NoError(t, err)
	return fn
}

func executeDispatchWorkflow(
	t testing.TB,
	fn TemporalWorkflowFunc,
	activityName string,
	register func(*testsuite.TestWorkflowEnvironment, string),
) []string {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var activityNames []string
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		activityNames = append(activityNames, info.ActivityType.Name)
	})
	register(env, activityName)
	env.ExecuteWorkflow(func(ctx workflow.Context) (any, error) {
		ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{StartToCloseTimeout: time.Minute})
		return fn(ctx, map[string]any{}, utils.NewState())
	})
	require.NoError(t, env.GetWorkflowError())
	return activityNames
}

func registerCallHTTPActivity(env *testsuite.TestWorkflowEnvironment, name string) {
	env.RegisterActivityWithOptions(
		func(context.Context, *model.CallHTTP, any, *utils.State) (any, error) {
			return map[string]any{testConstOK: true}, nil
		},
		activity.RegisterOptions{Name: name},
	)
}

func registerCallGRPCActivity(env *testsuite.TestWorkflowEnvironment, name string) {
	env.RegisterActivityWithOptions(
		func(context.Context, *model.CallGRPC, any, *utils.State) (any, error) {
			return map[string]any{testConstOK: true}, nil
		},
		activity.RegisterOptions{Name: name},
	)
}

func registerCallRunActivity(env *testsuite.TestWorkflowEnvironment, name string) {
	env.RegisterActivityWithOptions(
		func(context.Context, *model.RunTask, any, *utils.State) (any, error) {
			return map[string]any{testConstOK: true}, nil
		},
		activity.RegisterOptions{Name: name},
	)
}

func registerCallContainerActivity(env *testsuite.TestWorkflowEnvironment, name string) {
	env.RegisterActivityWithOptions(
		func(
			context.Context,
			*model.RunTask,
			any,
			*utils.State,
			string,
			activities.ContainerRuntime,
			string,
		) (any, error) {
			return map[string]any{testConstOK: true}, nil
		},
		activity.RegisterOptions{Name: name},
	)
}

func activityRegistryMethodNames() map[string]struct{} {
	names := make(map[string]struct{})
	for _, registered := range ActivitiesList() {
		registeredType := reflect.TypeOf(registered)
		for i := 0; i < registeredType.NumMethod(); i++ {
			names[registeredType.Method(i).Name] = struct{}{}
		}
	}
	return names
}
