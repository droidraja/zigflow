//go:build e2e

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

package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/internal/e2etest"
	"github.com/zigflow/zigflow/pkg/zigflow"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

const (
	dynamicTaskQueue    = "dynamic-e2e"
	dynamicWorkflowType = "arbitrary-dynamic-workflow"
	recordedRelease     = "recorded"
	changedRelease      = "changed"
)

func TestDynamicWorkflowE2E(t *testing.T) {
	ctx := t.Context()
	temporal := e2etest.StartTemporal(ctx, t)
	recorder := e2etest.StartHTTPRecorder(t, `{"ok":true}`)

	definition, err := os.ReadFile("workflow.yaml")
	require.NoError(t, err)
	definition = []byte(strings.ReplaceAll(
		string(definition),
		"http://127.0.0.1:1/dynamic",
		recorder.URL+"/dynamic",
	))

	process := e2etest.StartDynamicWorkerWithEnv(
		ctx,
		t,
		[]string{"ZIGGY_RELEASE=" + recordedRelease},
		temporal.Address,
		dynamicTaskQueue,
	)

	c, err := client.Dial(client.Options{HostPort: temporal.Address})
	require.NoError(t, err, "dial Temporal")
	defer c.Close()

	runCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	workflowID := "dynamic-e2e-" + time.Now().Format("20060102150405.000000000")
	we, err := c.ExecuteWorkflow(runCtx, client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: dynamicTaskQueue,
	}, dynamicWorkflowType, zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: definition,
		Input: map[string]any{
			"items":    []any{"first", "second"},
			"marker":   "original-input",
			"selected": "run",
		},
	})
	require.NoError(t, err, "execute dynamic workflow")
	initialRunID := we.GetRunID()

	var result map[string]any
	require.NoError(t, we.Get(runCtx, &result), "get dynamic workflow result")
	require.Equal(t, map[string]any{
		"complete":    true,
		"environment": recordedRelease,
		"input":       "original-input",
	}, result)
	require.Len(t, recorder.RequestsForPath("/dynamic"), 1)

	description, err := c.DescribeWorkflowExecution(runCtx, workflowID, "")
	require.NoError(t, err, "describe completed dynamic workflow")
	finalRunID := description.GetWorkflowExecutionInfo().GetExecution().GetRunId()
	require.NotEqual(t, initialRunID, finalRunID, "workflow must force Continue-As-New")

	initialHistory := loadHistory(t, runCtx, c, workflowID, initialRunID)
	finalHistory := loadHistory(t, runCtx, c, workflowID, finalRunID)
	require.Equal(t, enums.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
		finalHistory.Events[len(finalHistory.Events)-1].GetEventType())

	assertPublicSnapshot(t, initialHistory, definition)
	assertInternalSnapshot(t, finalHistory, definition)

	requestsBeforeInvalidInput := len(recorder.Requests())
	invalidRun, err := c.ExecuteWorkflow(runCtx, client.StartWorkflowOptions{
		TaskQueue: dynamicTaskQueue,
	}, "arbitrary-invalid-input", zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion + 1,
		Definition: definition,
	})
	require.NoError(t, err, "start invalid dynamic workflow")
	require.Error(t, invalidRun.Get(runCtx, nil), "invalid envelope must fail")
	require.Len(t, recorder.Requests(), requestsBeforeInvalidInput,
		"invalid input must fail before the HTTP activity is called")

	process.Stop()
	changedProcess := e2etest.StartDynamicWorkerWithEnv(
		ctx,
		t,
		[]string{"ZIGGY_RELEASE=" + changedRelease},
		temporal.Address,
		dynamicTaskQueue,
	)

	changedDefinition := []byte(`document:
  dsl: 1.0.0
  taskQueue: dynamic-e2e
  workflowType: changed-environment-probe
  version: 0.0.1
do:
  - expose:
      output:
        as:
          environment: ${ $env.RELEASE }
      set:
        complete: true
`)
	changedRun, err := c.ExecuteWorkflow(runCtx, client.StartWorkflowOptions{
		TaskQueue: dynamicTaskQueue,
	}, "changed-environment-probe", zigflow.DynamicWorkflowInput{
		Version:    zigflow.DynamicWorkflowInputVersion,
		Definition: changedDefinition,
	})
	require.NoError(t, err, "execute workflow after worker restart")
	var changedResult map[string]any
	require.NoError(t, changedRun.Get(runCtx, &changedResult))
	require.Equal(t, map[string]any{"environment": changedRelease}, changedResult)
	changedProcess.Stop()

	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterDynamicWorkflow(
		zigflow.NewDynamicWorkflowHandler(zigflow.DynamicWorkflowOptions{
			Envvars: map[string]any{"RELEASE": changedRelease},
		}),
		workflow.DynamicRegisterOptions{},
	)
	require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(
		nil,
		initialHistory,
		worker.ReplayWorkflowHistoryOptions{
			OriginalExecution: workflow.Execution{ID: workflowID, RunID: initialRunID},
		},
	),
		"replay must use the definition and environment recorded at start")
	require.NoError(t, replayer.ReplayWorkflowHistoryWithOptions(
		nil,
		finalHistory,
		worker.ReplayWorkflowHistoryOptions{
			OriginalExecution: workflow.Execution{ID: workflowID, RunID: finalRunID},
		},
	),
		"continued replay must use the recorded internal invocation")
}

func loadHistory(
	t *testing.T,
	ctx context.Context,
	c client.Client,
	workflowID string,
	runID string,
) *historypb.History {
	t.Helper()

	history := &historypb.History{}
	iter := c.GetWorkflowHistory(ctx, workflowID, runID, false, enums.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT)
	for iter.HasNext() {
		event, err := iter.Next()
		require.NoError(t, err, "read workflow history")
		history.Events = append(history.Events, event)
	}
	require.NotEmpty(t, history.Events)
	return history
}

func assertPublicSnapshot(t *testing.T, history *historypb.History, definition []byte) {
	t.Helper()

	started := history.Events[0].GetWorkflowExecutionStartedEventAttributes()
	require.NotNil(t, started)
	var input zigflow.DynamicWorkflowInput
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &input))
	require.Equal(t, definition, input.Definition)
}

func assertInternalSnapshot(t *testing.T, history *historypb.History, definition []byte) {
	t.Helper()

	started := history.Events[0].GetWorkflowExecutionStartedEventAttributes()
	require.NotNil(t, started)
	var invocation tasks.InternalWorkflowInvocation
	require.NoError(t, converter.GetDefaultDataConverter().FromPayloads(started.GetInput(), &invocation))
	require.Equal(t, definition, invocation.Definition)
	require.Equal(t, map[string]any{"RELEASE": recordedRelease}, invocation.RecordedEnv)
}
