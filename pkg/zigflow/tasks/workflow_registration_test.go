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

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

const (
	testContainerRootWorkflow = "container-root"
	testForChildWorkflow      = "workflow_for_iterate"
)

func TestLocalWorkflowRegistryRejectsDuplicateNames(t *testing.T) {
	registry := NewLocalWorkflowRegistry()
	fn := func(workflow.Context, any, *utils.State) (any, error) { return nil, nil }

	require.NoError(t, registry.RegisterWorkflow("example", fn))
	err := registry.RegisterWorkflow("example", fn)

	require.Error(t, err)
	assert.EqualError(t, err, `workflow type "example" is already registered`)
	registered, ok := registry.Workflow("example")
	assert.True(t, ok)
	assert.NotNil(t, registered)
	assert.Equal(t, []string{"example"}, registry.Names())
}

func TestWorkerWorkflowRegistrarPreservesStaticRegistration(t *testing.T) {
	temporalWorker := new(WorkflowRegistryMock)
	fn := TemporalWorkflowFunc(func(workflow.Context, any, *utils.State) (any, error) { return nil, nil })
	temporalWorker.
		On("RegisterWorkflowWithOptions", mock.Anything, workflow.RegisterOptions{Name: "static"}).
		Once()

	registrar := NewWorkerWorkflowRegistrar(temporalWorker)
	require.NoError(t, registrar.RegisterWorkflow("static", fn))
	err := registrar.RegisterWorkflow("static", fn)

	require.Error(t, err)
	assert.EqualError(t, err, `workflow type "static" is already registered`)
	temporalWorker.AssertExpectations(t)
}

func TestContainerBuildUsesExecutionLocalWorkflowRegistry(t *testing.T) {
	registry := NewLocalWorkflowRegistry()
	doc := &model.Workflow{Document: model.Document{Name: testContainerRootWorkflow}}
	doBuilder, err := NewDoTaskBuilder(
		nil,
		containerRegistrationTask(),
		doc.Document.Name,
		doc,
		testEvents,
		&TaskOpts{WorkflowRegistrar: registry},
	)
	require.NoError(t, err)

	_, err = doBuilder.Build()
	require.NoError(t, err)
	assert.Equal(t, []string{
		testContainerRootWorkflow,
		"workflow_catch_guarded",
		testForChildWorkflow,
		"workflow_fork_parallel_left",
		"workflow_fork_parallel_right",
		"workflow_try_guarded",
	}, registry.Names())
}

func TestStaticContainerWorkflowRegistrationNamesRemainUnchanged(t *testing.T) {
	temporalWorker := new(WorkflowRegistryMock)
	expectedNames := []string{
		testContainerRootWorkflow,
		"workflow_catch_guarded",
		testForChildWorkflow,
		"workflow_fork_parallel_left",
		"workflow_fork_parallel_right",
		"workflow_try_guarded",
	}
	for _, name := range expectedNames {
		temporalWorker.
			On("RegisterWorkflowWithOptions", mock.Anything, workflow.RegisterOptions{Name: name}).
			Once()
	}

	doc := &model.Workflow{Document: model.Document{Name: testContainerRootWorkflow}}
	doBuilder, err := NewDoTaskBuilder(
		temporalWorker,
		containerRegistrationTask(),
		doc.Document.Name,
		doc,
		testEvents,
		nil,
	)
	require.NoError(t, err)

	_, err = doBuilder.Build()
	require.NoError(t, err)
	temporalWorker.AssertExpectations(t)
}

func TestMultipleRootDoRegistrationNamesRemainUnchanged(t *testing.T) {
	registry := NewLocalWorkflowRegistry()
	doc := &model.Workflow{Document: model.Document{Name: "ignored-document-name"}}
	task := &model.DoTask{Do: &model.TaskList{
		{Key: "first-root", Task: doWithSet("first-step")},
		{Key: "second-root", Task: doWithSet("second-step")},
	}}
	doBuilder, err := NewDoTaskBuilder(
		nil,
		task,
		doc.Document.Name,
		doc,
		testEvents,
		&TaskOpts{WorkflowRegistrar: registry},
	)
	require.NoError(t, err)

	_, err = doBuilder.Build()
	require.NoError(t, err)
	assert.Equal(t, []string{"first-root", "second-root"}, registry.Names())
}

func TestDuplicateGeneratedWorkflowNameReturnsBuildError(t *testing.T) {
	registry := NewLocalWorkflowRegistry()
	doc := &model.Workflow{Document: model.Document{Name: "duplicate-root"}}
	task := &model.DoTask{Do: &model.TaskList{
		{Key: "repeat", Task: forWithSet("first")},
		{Key: "repeat", Task: forWithSet("second")},
	}}
	doBuilder, err := NewDoTaskBuilder(
		nil,
		task,
		doc.Document.Name,
		doc,
		testEvents,
		&TaskOpts{WorkflowRegistrar: registry},
	)
	require.NoError(t, err)

	_, err = doBuilder.Build()
	require.Error(t, err)
	assert.ErrorContains(t, err, `workflow type "workflow_for_repeat" is already registered`)
}

func TestForWrapperKeepsForChildResultValue(t *testing.T) {
	registry := NewLocalWorkflowRegistry()
	doc := &model.Workflow{Document: model.Document{Name: "for-result"}}
	builder, err := NewForTaskBuilder(
		nil,
		forWithSet("result"),
		"iterate",
		doc,
		testEvents,
		&TaskOpts{WorkflowRegistrar: registry},
	)
	require.NoError(t, err)

	_, err = builder.Build()
	require.NoError(t, err)
	fn, ok := registry.Workflow(testForChildWorkflow)
	require.True(t, ok)

	state := utils.NewState()
	state.Context = map[string]any{"kept": true}
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: testForChildWorkflow})
	env.ExecuteWorkflow(testForChildWorkflow, nil, state)
	require.NoError(t, env.GetWorkflowError())

	var result forChildResult
	require.NoError(t, env.GetWorkflowResult(&result))
	assert.Equal(t, map[string]any{"result": true}, result.Output)
	assert.Equal(t, map[string]any{"kept": true}, result.Context)
}

func containerRegistrationTask() *model.DoTask {
	return &model.DoTask{Do: &model.TaskList{
		{Key: "initialise", Task: setTask("initialised")},
		{Key: "iterate", Task: forWithSet("iteration")},
		{
			Key: "parallel",
			Task: &model.ForkTask{Fork: model.ForkTaskConfiguration{Branches: &model.TaskList{
				{Key: "left", Task: setTask("left")},
				{Key: "right", Task: setTask("right")},
			}}},
		},
		{
			Key: "guarded",
			Task: &model.TryTask{
				Try: &model.TaskList{{Key: "try-step", Task: setTask("try")}},
				Catch: &model.TryTaskCatch{
					Do: &model.TaskList{{Key: "catch-step", Task: setTask("catch")}},
				},
			},
		},
	}}
}

func doWithSet(name string) *model.DoTask {
	return &model.DoTask{Do: &model.TaskList{{Key: name, Task: setTask(name)}}}
}

func forWithSet(name string) *model.ForTask {
	return &model.ForTask{
		For: model.ForTaskConfiguration{In: "${ [1] }"},
		Do:  &model.TaskList{{Key: name, Task: setTask(name)}},
	}
}

func setTask(name string) *model.SetTask {
	return &model.SetTask{Set: model.NewObjectOrRuntimeExpr(map[string]any{name: true})}
}
