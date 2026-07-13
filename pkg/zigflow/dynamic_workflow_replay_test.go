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
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	dynamicReplayCompleteKey = "complete"
	dynamicReplayDefinition  = `document:
  dsl: 1.0.0
  taskQueue: dynamic-replay
  workflowType: arbitrary-recorded-workflow
  version: 0.0.1
  metadata:
    activityOptions:
      startToCloseTimeout:
        seconds: 5
do:
  - choose:
      switch:
        - recorded:
            when: ${ $env.RELEASE == "recorded" }
            then: continue
        - default:
            then: end
  - request:
      call: http
      with:
        method: get
        endpoint: http://recorded.invalid/dynamic
  - finish:
      set:
        complete: true
`
)

func TestDynamicWorkflowReplaysRecordedDefinitionAndEnvironment(t *testing.T) {
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterDynamicWorkflow(
		zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{
			Envvars: map[string]any{"RELEASE": "changed"},
		}),
		workflow.DynamicRegisterOptions{},
	)

	require.NoError(t, replayer.ReplayWorkflowHistory(nil, dynamicReplayHistory(t)),
		"changed worker environment must not change recorded dynamic commands")
}

func dynamicReplayHistory(t *testing.T) *historypb.History {
	t.Helper()

	dc := converter.GetDefaultDataConverter()
	startedInput, err := dc.ToPayloads(zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: []byte(dynamicReplayDefinition),
	})
	require.NoError(t, err)

	sideEffectID, err := dc.ToPayloads(int64(1))
	require.NoError(t, err)
	sideEffectData, err := dc.ToPayloads(map[string]any{"RELEASE": "recorded"})
	require.NoError(t, err)
	activityResult, err := dc.ToPayloads(map[string]any{"statusCode": 200})
	require.NoError(t, err)
	workflowResult, err := dc.ToPayloads(map[string]any{dynamicReplayCompleteKey: true})
	require.NoError(t, err)

	taskQueue := &taskqueuepb.TaskQueue{Name: "dynamic-replay"}
	events := []*historypb.HistoryEvent{
		{
			EventId:   1,
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
				WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
					WorkflowType: &commonpb.WorkflowType{Name: "arbitrary-recorded-workflow"},
					TaskQueue:    taskQueue,
					Input:        startedInput,
				},
			},
		},
		{
			EventId:   2,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue: taskQueue,
				},
			},
		},
		{
			EventId:   3,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
			},
		},
		{
			EventId:   4,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 2,
					StartedEventId:   3,
				},
			},
		},
		{
			EventId:   5,
			EventType: enums.EVENT_TYPE_MARKER_RECORDED,
			Attributes: &historypb.HistoryEvent_MarkerRecordedEventAttributes{
				MarkerRecordedEventAttributes: &historypb.MarkerRecordedEventAttributes{
					MarkerName: "SideEffect",
					Details: map[string]*commonpb.Payloads{
						"side-effect-id": sideEffectID,
						"data":           sideEffectData,
					},
					WorkflowTaskCompletedEventId: 4,
				},
			},
		},
		{
			EventId:   6,
			EventType: enums.EVENT_TYPE_ACTIVITY_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_ActivityTaskScheduledEventAttributes{
				ActivityTaskScheduledEventAttributes: &historypb.ActivityTaskScheduledEventAttributes{
					ActivityId:                   "6",
					ActivityType:                 &commonpb.ActivityType{Name: "CallHTTPActivity"},
					TaskQueue:                    taskQueue,
					WorkflowTaskCompletedEventId: 4,
				},
			},
		},
		{
			EventId:   7,
			EventType: enums.EVENT_TYPE_ACTIVITY_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_ActivityTaskStartedEventAttributes{
				ActivityTaskStartedEventAttributes: &historypb.ActivityTaskStartedEventAttributes{
					ScheduledEventId: 6,
				},
			},
		},
		{
			EventId:   8,
			EventType: enums.EVENT_TYPE_ACTIVITY_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_ActivityTaskCompletedEventAttributes{
				ActivityTaskCompletedEventAttributes: &historypb.ActivityTaskCompletedEventAttributes{
					ScheduledEventId: 6,
					StartedEventId:   7,
					Result:           activityResult,
				},
			},
		},
		{
			EventId:   9,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskScheduledEventAttributes{
				WorkflowTaskScheduledEventAttributes: &historypb.WorkflowTaskScheduledEventAttributes{
					TaskQueue: taskQueue,
				},
			},
		},
		{
			EventId:   10,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_STARTED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskStartedEventAttributes{
				WorkflowTaskStartedEventAttributes: &historypb.WorkflowTaskStartedEventAttributes{},
			},
		},
		{
			EventId:   11,
			EventType: enums.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &historypb.WorkflowTaskCompletedEventAttributes{
					ScheduledEventId: 9,
					StartedEventId:   10,
				},
			},
		},
		{
			EventId:   12,
			EventType: enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
			Attributes: &historypb.HistoryEvent_WorkflowExecutionCompletedEventAttributes{
				WorkflowExecutionCompletedEventAttributes: &historypb.WorkflowExecutionCompletedEventAttributes{
					Result:                       workflowResult,
					WorkflowTaskCompletedEventId: 11,
				},
			},
		},
	}

	return &historypb.History{Events: events}
}
