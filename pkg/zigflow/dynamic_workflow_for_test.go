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
	dynamicForAlpha         = "alpha"
	dynamicForBeta          = "beta"
	dynamicForFirst         = "first"
	dynamicForIndexKey      = "index"
	dynamicForItemKey       = "item"
	dynamicForItemsKey      = "items"
	dynamicForMarkerKey     = "marker"
	dynamicForOriginalInput = "original-input"
	dynamicForPositionKey   = "position"
	dynamicForSecond        = "second"
	dynamicForValueKey      = "value"
)

func TestDynamicWorkflowForArrayResolvesNestedTasks(t *testing.T) {
	definition := loadDynamicWorkflowFixture(t, "for.yaml")
	input := map[string]any{
		dynamicForItemsKey: []any{dynamicForAlpha, dynamicForBeta},
	}
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})

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
	harness.execute("dynamic-for", zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: definition,
		Input:      input,
	})

	requireWorkflowResult(t, harness.env, []any{
		map[string]any{dynamicForIndexKey: float64(0), dynamicForItemKey: dynamicForAlpha},
		map[string]any{dynamicForIndexKey: float64(1), dynamicForItemKey: dynamicForBeta},
	})
	require.Empty(t, decodeErrors)
	require.Equal(t, []string{"workflow_for_iterate", "workflow_for_iterate"}, childNames)
	require.Len(t, invocations, 2)
	for index, invocation := range invocations {
		require.Equal(t, tasks.InternalWorkflowInvocationVersion, invocation.Version)
		require.Equal(t, definition, invocation.Definition)
		require.Equal(t, input, invocation.OriginalInput)
		require.Equal(t, input, invocation.State.Input)
		require.Equal(t, float64(index), invocation.State.Data[dynamicForIndexKey])
		require.Equal(t, input[dynamicForItemsKey].([]any)[index], invocation.State.Data[dynamicForItemKey])
	}
}

func TestDynamicWorkflowForObjectMatchesStaticResultShape(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-for-object", `
  - iterate:
      for:
        each: value
        in: ${ $input.items }
        at: key
      do:
        - capture:
            output:
              as:
                key: ${ $data.key }
                marker: ${ $input.marker }
                value: ${ $data.value }
            set:
              key: ${ $data.key }
              value: ${ $data.value }`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-for-object", dynamicInputWithUserInput(definition, map[string]any{
		dynamicForItemsKey: map[string]any{
			dynamicForFirst:  dynamicForAlpha,
			dynamicForSecond: dynamicForBeta,
		},
		dynamicForMarkerKey: dynamicForOriginalInput,
	}))

	requireWorkflowResult(t, harness.env, map[string]any{
		dynamicForFirst: map[string]any{
			"key":               dynamicForFirst,
			dynamicForMarkerKey: dynamicForOriginalInput,
			dynamicForValueKey:  dynamicForAlpha,
		},
		dynamicForSecond: map[string]any{
			"key":               dynamicForSecond,
			dynamicForMarkerKey: dynamicForOriginalInput,
			dynamicForValueKey:  dynamicForBeta,
		},
	})
}

func TestDynamicWorkflowForPreservesInterIterationStateAndParentIsolation(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-for-isolation", `
  - initialise:
      export:
        as:
          root: preserved
      set:
        initialised: true
  - iterate:
      for:
        each: item
        in: ${ $input.items }
        at: position
      while: ${ ($context.iterations // 0) < 2 }
      do:
        - capture:
            output:
              as:
                item: ${ $data.item }
                position: ${ $data.position }
                previousIterations: ${ $context.iterations // 0 }
            export:
              as:
                iterations: ${ ($context.iterations // 0) + 1 }
                root: ${ $context.root }
            set:
              childOnly: ${ $data.item }
  - inspect:
      output:
        as:
          aggregate: ${ $output }
          childOnly: ${ $data.childOnly }
          item: ${ $data.item }
          parentContext: ${ $context }
          position: ${ $data.position }
      set:
        inspected: true`)
	harness := newDynamicWorkflowTestHarness(t, zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{}))
	harness.env.SetStartWorkflowOptions(client.StartWorkflowOptions{TaskQueue: dynamicTestTaskQueue})
	harness.execute("dynamic-for-isolation", dynamicInputWithUserInput(definition, map[string]any{
		dynamicForItemsKey: []any{dynamicForAlpha, dynamicForBeta, "not-run"},
	}))

	requireWorkflowResult(t, harness.env, map[string]any{
		"aggregate": []any{
			map[string]any{
				dynamicForItemKey:     dynamicForAlpha,
				dynamicForPositionKey: float64(0),
				"previousIterations":  float64(0),
			},
			map[string]any{
				dynamicForItemKey:     dynamicForBeta,
				dynamicForPositionKey: float64(1),
				"previousIterations":  float64(1),
			},
		},
		"childOnly":       nil,
		dynamicForItemKey: nil,
		"parentContext": map[string]any{
			"root": "preserved",
		},
		dynamicForPositionKey: nil,
	})
}

func TestDynamicWorkflowForNestedEndReachesRootWithEffectiveOutput(t *testing.T) {
	definition := flatDynamicDefinition("dynamic-for-end", `
  - iterate:
      for:
        each: item
        in: ${ $input.items }
        at: index
      do:
        - finish:
            output:
              as:
                ended: true
                index: ${ $data.index }
                item: ${ $data.item }
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
	harness.execute("dynamic-for-end", dynamicInputWithUserInput(definition, map[string]any{
		dynamicForItemsKey: []any{dynamicForFirst, dynamicForSecond},
	}))

	requireWorkflowResult(t, harness.env, map[string]any{
		"ended":            true,
		dynamicForIndexKey: float64(0),
		dynamicForItemKey:  dynamicForFirst,
	})
	requireScheduledChildWorkflowNames(t, harness, "workflow_for_iterate")
}

func dynamicInputWithUserInput(definition []byte, input any) zigflow.DynamicWorkflowInput {
	return zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: definition,
		Input:      input,
	}
}
