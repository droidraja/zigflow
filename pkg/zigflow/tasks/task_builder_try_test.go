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
	"errors"
	"testing"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"github.com/zigflow/zigflow/pkg/zigflow/flow"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const dynamicTryOriginalInput = "original"

func TestTryTaskBuilderGetTasks(t *testing.T) {
	task := &model.TryTask{
		Try: &model.TaskList{
			&model.TaskItem{Key: "task", Task: &model.SetTask{}},
		},
		Catch: &model.TryTaskCatch{
			Do: &model.TaskList{
				&model.TaskItem{Key: "catch", Task: &model.SetTask{}},
			},
		},
	}

	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			task: task,
		},
	}

	got := builder.getTasks()
	require.Len(t, got, 2)
	assert.Equal(t, tryBodyPathSegment, got[0].name)
	assert.Equal(t, task.Try, got[0].tasks)
	assert.Equal(t, catchBodyPathSegment, got[1].name)
	assert.Equal(t, task.Catch.Do, got[1].tasks)
}

func TestTryTaskBuilderTraversalReportsTryErrorFirst(t *testing.T) {
	task := &model.TryTask{
		Try: &model.TaskList{},
		Catch: &model.TryTaskCatch{
			Do: &model.TaskList{},
		},
	}
	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "try-task",
			task: task,
		},
	}

	checks := []struct {
		name string
		run  func() error
	}{
		{name: "build", run: func() error { _, err := builder.Build(); return err }},
		{name: "post load", run: builder.PostLoad},
		{name: "validate", run: builder.Validate},
	}

	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			for range 20 {
				err := check.run()
				require.Error(t, err)
				assert.Contains(t, err.Error(), "no tasks detected for try in try-task")
				assert.NotContains(t, err.Error(), "no tasks detected for catch")
			}
		})
	}
}

func TestTryTaskBuilderExecRunsCatchOnError(t *testing.T) {
	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "try-task",
			task: &model.TryTask{
				Try: &model.TaskList{},
				Catch: &model.TryTaskCatch{
					Do: &model.TaskList{},
				},
			},
		},
		tryChildWorkflowName:   "try-child",
		catchChildWorkflowName: "catch-child",
	}

	fn, err := builder.exec()
	assert.NoError(t, err)

	state := utils.NewState()

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return nil, errors.New("boom")
	}, workflow.RegisterOptions{Name: builder.tryChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return map[string]any{
			testConstHandledKey: true,
		}, nil
	}, workflow.RegisterOptions{Name: builder.catchChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, state)
	}, workflow.RegisterOptions{Name: "try-exec"})

	env.ExecuteWorkflow("try-exec")
	assert.NoError(t, env.GetWorkflowError())

	var result map[string]any
	assert.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, map[string]any{testConstHandledKey: true}, result)
}

func TestTryTaskBuilderExecUsesDynamicInvocationForTryAndCatch(t *testing.T) {
	definition := []byte("recorded-try-definition")
	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "dynamic-try-task",
			task: &model.TryTask{
				Try: &model.TaskList{},
				Catch: &model.TryTaskCatch{
					As: "failure",
					Do: &model.TaskList{},
				},
			},
			taskOpts: &TaskOpts{DynamicExecution: NewDynamicExecutionOptions(definition)},
		},
		tryChildWorkflowName:   "dynamic-try-child",
		catchChildWorkflowName: "dynamic-catch-child",
	}

	fn, err := builder.exec()
	require.NoError(t, err)

	input := map[string]any{testConstRequest: dynamicTryOriginalInput}
	state := utils.NewState()
	state.Input = input
	state.Env = map[string]any{"DEPLOYMENT": "recorded"}
	state.Data = map[string]any{"parent": "preserved"}

	var tryInvocation InternalWorkflowInvocation
	var catchInvocation InternalWorkflowInvocation

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(
		func(_ workflow.Context, invocation InternalWorkflowInvocation) (map[string]any, error) {
			tryInvocation = invocation
			return nil, temporal.NewApplicationError("kaboom", "MyAppError")
		},
		workflow.RegisterOptions{Name: builder.tryChildWorkflowName},
	)
	env.RegisterWorkflowWithOptions(
		func(_ workflow.Context, invocation InternalWorkflowInvocation) (map[string]any, error) {
			catchInvocation = invocation
			return map[string]any{testConstHandledKey: true}, nil
		},
		workflow.RegisterOptions{Name: builder.catchChildWorkflowName},
	)
	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, state)
	}, workflow.RegisterOptions{Name: "dynamic-try-exec"})

	env.ExecuteWorkflow("dynamic-try-exec")
	require.NoError(t, env.GetWorkflowError())

	for _, invocation := range []InternalWorkflowInvocation{tryInvocation, catchInvocation} {
		require.Equal(t, InternalWorkflowInvocationVersion, invocation.Version)
		require.Equal(t, definition, invocation.Definition)
		require.Equal(t, input, invocation.OriginalInput)
		require.Equal(t, state.Env, invocation.RecordedEnv)
		require.Equal(t, input, invocation.State.Input)
	}
	require.Equal(t, "preserved", tryInvocation.State.Data["parent"])
	require.NotContains(t, tryInvocation.State.Data, "failure")
	require.Equal(t, "preserved", catchInvocation.State.Data["parent"])
	caughtError, ok := catchInvocation.State.Data["failure"].(map[string]any)
	require.True(t, ok)
	require.Equal(t, "MyAppError", caughtError["type"])
	require.Equal(t, "kaboom", caughtError["message"])
	require.Contains(t, caughtError, "childWorkflow")
}

// TestTryTaskBuilderExecPropagatesEndFromTryChild proves that a
// `then: end` directive inside the try child workflow is NOT treated
// as a catchable failure. The carried output must survive the boundary
// and exec must surface flow.ErrEnd to the do-task pipeline so the
// overall workflow ends cleanly, not run the catch handler.
func TestTryTaskBuilderExecPropagatesEndFromTryChild(t *testing.T) {
	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "try-task-end",
			task: &model.TryTask{
				Try: &model.TaskList{},
				Catch: &model.TryTaskCatch{
					Do: &model.TaskList{},
				},
			},
		},
		tryChildWorkflowName:   "try-child-end",
		catchChildWorkflowName: "catch-child-end",
	}

	fn, err := builder.exec()
	require.NoError(t, err)

	state := utils.NewState()
	childOutput := map[string]any{testConstValue: "end-time-output"}
	catchRan := false

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return nil, flow.NewEndApplicationError(childOutput)
	}, workflow.RegisterOptions{Name: builder.tryChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		catchRan = true
		return map[string]any{testConstHandledKey: true}, nil
	}, workflow.RegisterOptions{Name: builder.catchChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, state)
	}, workflow.RegisterOptions{Name: "try-exec-end"})

	env.ExecuteWorkflow("try-exec-end")

	// The try task surfaces flow.ErrEnd through the Temporal envelope.
	wErr := env.GetWorkflowError()
	require.Error(t, wErr)
	assert.Contains(t, wErr.Error(), flow.ErrEnd.Error())
	assert.False(t, catchRan, "catch handler must not run when the try child workflow signalled end")
}

// TestTryTaskBuilderExecPropagatesEndFromCatchChild is the symmetric
// case: when the try child fails for a real reason and the catch
// handler itself emits `then: end`, that end must propagate as
// flow.ErrEnd rather than being wrapped as a generic catch-workflow
// failure.
func TestTryTaskBuilderExecPropagatesEndFromCatchChild(t *testing.T) {
	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "try-task-catch-end",
			task: &model.TryTask{
				Try: &model.TaskList{},
				Catch: &model.TryTaskCatch{
					Do: &model.TaskList{},
				},
			},
		},
		tryChildWorkflowName:   "try-child-real-fail",
		catchChildWorkflowName: "catch-child-end",
	}

	fn, err := builder.exec()
	require.NoError(t, err)

	state := utils.NewState()
	catchOutput := map[string]any{testConstValue: "catch-end-output"}

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return nil, errors.New("boom from try")
	}, workflow.RegisterOptions{Name: builder.tryChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return nil, flow.NewEndApplicationError(catchOutput)
	}, workflow.RegisterOptions{Name: builder.catchChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, state)
	}, workflow.RegisterOptions{Name: "try-exec-catch-end"})

	env.ExecuteWorkflow("try-exec-catch-end")

	wErr := env.GetWorkflowError()
	require.Error(t, wErr)
	assert.Contains(t, wErr.Error(), flow.ErrEnd.Error())
	assert.NotContains(t, wErr.Error(), "error calling catch workflow",
		"catch-emitted end must not be wrapped as a catch-workflow failure")
}

// runCatchAndCaptureState executes a try task whose try child fails, then
// returns the $data the catch child workflow actually observed alongside the
// parent state the exec function was handed. The catch child records the data
// it receives into a closure-captured map so the test can assert on the exact
// caught-error contract exposed under $data.
func runCatchAndCaptureState(t *testing.T, catchAs string, tryErr error) (caughtData map[string]any, parentState *utils.State) {
	t.Helper()

	builder := &TryTaskBuilder{
		builder: builder[*model.TryTask]{
			name: "try-task-capture",
			task: &model.TryTask{
				Try: &model.TaskList{},
				Catch: &model.TryTaskCatch{
					As: catchAs,
					Do: &model.TaskList{},
				},
			},
		},
		tryChildWorkflowName:   "try-child-capture",
		catchChildWorkflowName: "catch-child-capture",
	}

	fn, err := builder.exec()
	require.NoError(t, err)

	parentState = utils.NewState()

	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		return nil, tryErr
	}, workflow.RegisterOptions{Name: builder.tryChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context, input any, st *utils.State) (map[string]any, error) {
		caughtData = st.Data
		return map[string]any{testConstHandledKey: true}, nil
	}, workflow.RegisterOptions{Name: builder.catchChildWorkflowName})

	env.RegisterWorkflowWithOptions(func(ctx workflow.Context) (any, error) {
		return fn(ctx, nil, parentState)
	}, workflow.RegisterOptions{Name: "try-exec-capture"})

	env.ExecuteWorkflow("try-exec-capture")
	require.NoError(t, env.GetWorkflowError())

	return caughtData, parentState
}

// TestTryTaskBuilderExecExposesErrorUnderDefaultKey proves the catch child
// workflow sees the caught error under $data.error when catch.as is unset.
func TestTryTaskBuilderExecExposesErrorUnderDefaultKey(t *testing.T) {
	caughtData, _ := runCatchAndCaptureState(t, "", temporal.NewApplicationError("kaboom", "MyAppError"))

	caughtErr, ok := caughtData["error"].(map[string]any)
	require.True(t, ok, "catch child must see the caught error under $data.error")

	// The error crosses a real child workflow boundary, so it carries both the
	// child workflow metadata and the unwrapped ApplicationError fields.
	assert.Equal(t, "MyAppError", caughtErr["type"])
	assert.Equal(t, "kaboom", caughtErr["message"])
	assert.Contains(t, caughtErr, "childWorkflow")
	childMeta, ok := caughtErr["childWorkflow"].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, childMeta["workflowType"])
	assert.NotEmpty(t, childMeta["workflowID"])
}

// TestTryTaskBuilderExecExposesErrorUnderCustomKey proves the catch child
// workflow sees the caught error under $data.<catch.as> when it is configured,
// and that the default "error" key is not used in that case.
func TestTryTaskBuilderExecExposesErrorUnderCustomKey(t *testing.T) {
	const customKey = "failure"

	caughtData, _ := runCatchAndCaptureState(t, customKey, temporal.NewApplicationError("kaboom", "MyAppError"))

	caughtErr, ok := caughtData[customKey].(map[string]any)
	require.True(t, ok, "catch child must see the caught error under the custom $data key")
	assert.Equal(t, "MyAppError", caughtErr["type"])
	assert.Equal(t, "kaboom", caughtErr["message"])

	assert.NotContains(t, caughtData, "error", "default error key must not be set when catch.as is configured")
}

// TestTryTaskBuilderExecDoesNotLeakErrorIntoParentState proves the injected
// caught error lives only on the cloned catch state and never mutates the
// parent state that later tasks observe. This guards Zigflow's explicit state
// propagation model: the error is only carried forward if the catch tasks
// output it.
func TestTryTaskBuilderExecDoesNotLeakErrorIntoParentState(t *testing.T) {
	_, parentState := runCatchAndCaptureState(t, "", temporal.NewApplicationError("kaboom", "MyAppError"))

	assert.NotContains(t, parentState.Data, "error",
		"caught error must not leak back into the parent state after catch completes")
	assert.Empty(t, parentState.Data, "parent state data must be untouched by catch error injection")
}

// TestBuildCatchError gives direct, deterministic coverage of the Temporal
// error enrichment without round-tripping every error shape through a workflow.
func TestBuildCatchError(t *testing.T) {
	tb := &TryTaskBuilder{}

	t.Run("application error fields", func(t *testing.T) {
		details := map[string]any{"reason": "quota exceeded"}
		appErr := temporal.NewNonRetryableApplicationError(
			"boom message", "BoomError", errors.New("root cause"), details,
		)

		out := tb.buildCatchError(appErr)

		assert.Equal(t, "BoomError", out["type"])
		assert.Equal(t, "boom message", out["message"])
		assert.Equal(t, true, out["nonRetryable"])
		assert.Equal(t, "root cause", out["cause"])
		assert.Equal(t, details, out["details"])
	})

	t.Run("retryable application error without details", func(t *testing.T) {
		appErr := temporal.NewApplicationError("transient", "TransientError")

		out := tb.buildCatchError(appErr)

		assert.Equal(t, "TransientError", out["type"])
		assert.Equal(t, "transient", out["message"])
		assert.Equal(t, false, out["nonRetryable"])
		assert.NotContains(t, out, "details")
	})

	t.Run("non-application error yields empty map", func(t *testing.T) {
		out := tb.buildCatchError(errors.New("plain"))

		assert.Empty(t, out)
	})
}
