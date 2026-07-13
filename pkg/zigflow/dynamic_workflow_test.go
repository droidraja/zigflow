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
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/zigflow"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/client"
)

const (
	dynamicTestTaskQueue       = "dynamic-tests"
	dynamicSetSwitchType       = "dynamic-set-switch"
	dynamicInputApplicationErr = "zigflow.dynamic.input"
)

func TestDynamicWorkflowExecutesFlatDefinitionWithEnvelopeInput(t *testing.T) {
	handler := zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{})
	harness := newDynamicWorkflowTestHarness(t, handler)
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute(dynamicSetSwitchType, zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: loadDynamicWorkflowFixture(t, "set-switch.yaml"),
		Input:      map[string]any{"selected": "match"},
	})

	requireWorkflowResult(t, harness.env, map[string]any{
		"complete": true,
		"selected": "match",
	})
	requireScheduledActivityNames(t, harness)
	requireScheduledChildWorkflowNames(t, harness)
}

func TestDynamicWorkflowCapturesImmutableEnvironmentOptions(t *testing.T) {
	envvars := map[string]any{"DEPLOYMENT": "captured"}
	handler := zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{Envvars: envvars})
	envvars["DEPLOYMENT"] = "mutated"

	definition := flatDynamicDefinition("dynamic-env", `
  - expose:
      output:
        as:
          input: ${ $input.value }
          environment: ${ $env.DEPLOYMENT }
      set:
        input: ${ $input.value }
        environment: ${ $env.DEPLOYMENT }`)
	harness := newDynamicWorkflowTestHarness(t, handler)
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-env", zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: definition,
		Input:      map[string]any{"value": "user-input"},
	})

	requireWorkflowResult(t, harness.env, map[string]any{
		"environment": "captured",
		"input":       "user-input",
	})
}

func TestDynamicWorkflowExecutesOtherFlatTaskTypes(t *testing.T) {
	t.Run("wait", func(t *testing.T) {
		definition := flatDynamicDefinition("dynamic-wait", `
  - pause:
      wait:
        milliseconds: 1
  - finish:
      output:
        as:
          waited: true
      set:
        waited: true`)
		harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
		harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
		harness.execute("dynamic-wait", dynamicInput(definition))

		requireWorkflowResult(t, harness.env, map[string]any{"waited": true})
	})

	t.Run("listen query", func(t *testing.T) {
		definition := flatDynamicDefinition("dynamic-listen", `
  - query:
      listen:
        to:
          one:
            with:
              id: current_state
              type: query
              data:
                ready: true
  - finish:
      output:
        as:
          listening: true
      set:
        listening: true`)
		harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
		harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
		harness.execute("dynamic-listen", dynamicInput(definition))

		requireWorkflowResult(t, harness.env, map[string]any{"listening": true})
	})

	t.Run("raise", func(t *testing.T) {
		definition := flatDynamicDefinition("dynamic-raise", `
  - fail:
      raise:
        error:
          type: https://serverlessworkflow.io/spec/1.0.0/errors/communication
          status: 400`)
		harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
		harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
		harness.execute("dynamic-raise", dynamicInput(definition))

		require.Error(t, harness.env.GetWorkflowError())
	})
}

func TestDynamicWorkflowRejectsInvalidDefinitionBeforeScheduling(t *testing.T) {
	for _, fixture := range []string{"invalid-schema.yaml", "unsupported-task.yaml"} {
		t.Run(fixture, func(t *testing.T) {
			harness := newDynamicWorkflowTestHarness(
				t,
				zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}),
			)
			harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
			var timers int
			harness.env.SetOnTimerScheduledListener(func(string, time.Duration) { timers++ })
			harness.execute("invalid-definition", dynamicInput(loadDynamicWorkflowFixture(t, fixture)))

			applicationError := requireNonRetryableApplicationError(t, harness.env)
			assert.Equal(t, "zigflow.dynamic.preparation", applicationError.Type())
			assert.Contains(t, applicationError.Error(), "prepare dynamic workflow definition")
			assert.Zero(t, timers)
			requireScheduledActivityNames(t, harness)
			requireScheduledChildWorkflowNames(t, harness)
		})
	}
}

func TestDynamicWorkflowRejectsInputAndDispatchMismatches(t *testing.T) {
	definition := loadDynamicWorkflowFixture(t, "set-switch.yaml")
	tests := []struct {
		name         string
		workflowType string
		taskQueue    string
		input        any
		errorType    string
		message      string
	}{
		{
			name:         "input decode",
			workflowType: dynamicSetSwitchType,
			taskQueue:    dynamicTestTaskQueue,
			input:        42,
			errorType:    dynamicInputApplicationErr,
			message:      "decode dynamic workflow input",
		},
		{
			name:         "unsupported envelope version",
			workflowType: dynamicSetSwitchType,
			taskQueue:    dynamicTestTaskQueue,
			input: zigflow.DynamicWorkflowInput{
				Version:    zigflow.DynamicWorkflowInputVersion + 1,
				Definition: definition,
			},
			errorType: dynamicInputApplicationErr,
			message:   "unsupported dynamic workflow input version",
		},
		{
			name:         "empty definition",
			workflowType: dynamicSetSwitchType,
			taskQueue:    dynamicTestTaskQueue,
			input: zigflow.DynamicWorkflowInput{
				Version:    zigflow.DynamicWorkflowInputVersion,
				Definition: []byte(" \n\t"),
			},
			errorType: dynamicInputApplicationErr,
			message:   "dynamic workflow definition must not be empty",
		},
		{
			name:         "task queue mismatch",
			workflowType: dynamicSetSwitchType,
			taskQueue:    "another-queue",
			input:        dynamicInput(definition),
			errorType:    "zigflow.dynamic.dispatch",
			message:      `workflow definition task queue "dynamic-tests" does not match execution task queue "another-queue"`,
		},
		{
			name:         "workflow type mismatch",
			workflowType: "unknown-workflow-type",
			taskQueue:    dynamicTestTaskQueue,
			input:        dynamicInput(definition),
			errorType:    "zigflow.dynamic.dispatch",
			message:      `workflow type "unknown-workflow-type" is not executable from this definition`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newDynamicWorkflowTestHarness(
				t,
				zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}),
			)
			harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: test.taskQueue})
			harness.execute(test.workflowType, test.input)

			applicationError := requireNonRetryableApplicationError(t, harness.env)
			assert.Equal(t, test.errorType, applicationError.Type())
			assert.Contains(t, applicationError.Error(), test.message)
			requireScheduledActivityNames(t, harness)
			requireScheduledChildWorkflowNames(t, harness)
		})
	}
}

func TestDynamicWorkflowBuildFailureIsNonRetryable(t *testing.T) {
	definition := flatDynamicDefinition("ignored-document-type", `
  - duplicate:
      do:
        - first:
            set:
              value: first
  - duplicate:
      do:
        - second:
            set:
              value: second`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("duplicate", dynamicInput(definition))

	applicationError := requireNonRetryableApplicationError(t, harness.env)
	assert.Equal(t, "zigflow.dynamic.build", applicationError.Type())
	assert.Contains(t, applicationError.Error(), `workflow type "duplicate" is already registered`)
	requireScheduledActivityNames(t, harness)
	requireScheduledChildWorkflowNames(t, harness)
}

func TestPrepareAndBuildWorkflowUseMandatoryLocalPath(t *testing.T) {
	doc, err := zigflow.PrepareWorkflow(loadDynamicWorkflowFixture(t, "set-switch.yaml"))
	require.NoError(t, err)

	taskOpts := &tasks.TaskOpts{ActivityDispatchPolicy: tasks.ActivityDispatchDynamic}
	registry, err := zigflow.BuildPreparedWorkflow(doc, zigflow.WorkflowBuildOptions{TaskOpts: taskOpts})
	require.NoError(t, err)
	assert.Equal(t, []string{"dynamic-set-switch"}, registry.Names())
	assert.Nil(t, taskOpts.WorkflowRegistrar, "local build must not mutate caller options")

	_, err = zigflow.PrepareWorkflow([]byte(`document:
  dsl: 0.1.0
  workflowType: invalid
do:
  - choose:
      switch:
        - one:
            then: end
        - two:
            then: end`))
	assert.ErrorIs(t, err, zigflow.ErrSchemaValidation, "schema validation must run before later preparation stages")
}

func dynamicInput(definition []byte) zigflow.DynamicWorkflowInput {
	return zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: definition,
	}
}

func flatDynamicDefinition(workflowType, taskDefinitions string) []byte {
	return []byte(fmt.Sprintf(`document:
  dsl: 1.0.0
  taskQueue: dynamic-tests
  workflowType: %s
  version: 0.0.1
do:%s
`, workflowType, taskDefinitions))
}
