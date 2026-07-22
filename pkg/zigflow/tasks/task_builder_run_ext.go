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
	"fmt"
	"regexp"

	"github.com/serverlessworkflow/sdk-go/v3/model"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/utils"
	"github.com/zigflow/zigflow/pkg/zigflow/flow"
	"github.com/zigflow/zigflow/pkg/zigflow/models"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
)

var nativeWorkflowTaskQueuePattern = regexp.MustCompile(`^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$`)

// NewRunWorkflowExtTaskBuilder constructs the builder for Zigflow's explicit
// cross-task-queue native child workflow extension (run.workflow.taskQueue).
func NewRunWorkflowExtTaskBuilder(
	temporalWorker worker.Worker,
	task *models.RunWorkflowExtTask,
	taskName string,
	doc *model.Workflow,
	emitter *cloudevents.Events,
	taskOpts *TaskOpts,
) (*RunWorkflowExtTaskBuilder, error) {
	return &RunWorkflowExtTaskBuilder{
		builder: builder[*models.RunWorkflowExtTask]{
			doc:            doc,
			eventEmitter:   emitter,
			name:           taskName,
			task:           task,
			taskOpts:       taskOpts,
			temporalWorker: temporalWorker,
		},
	}, nil
}

// RunWorkflowExtTaskBuilder dispatches a native Temporal child workflow on an
// explicit task queue using the one-argument child contract, unlike
// RunTaskBuilder's internal (input, state) Zigflow child contract.
type RunWorkflowExtTaskBuilder struct {
	builder[*models.RunWorkflowExtTask]
}

// PostLoad defaults optional fields on the shared extension model. Builders
// are reconstructed for each of the PostLoad, Validate, and Build phases, so
// defaults must land on the parsed task itself to be visible at Build time.
func (t *RunWorkflowExtTaskBuilder) PostLoad() error {
	if t.task.Run.Await == nil {
		t.task.Run.Await = utils.Ptr(true)
	}
	return nil
}

func (t *RunWorkflowExtTaskBuilder) Validate() error {
	if t.task.Run.Workflow.Type == "" {
		return fmt.Errorf("native child workflow type must be set: %s", t.GetTaskName())
	}
	if !nativeWorkflowTaskQueuePattern.MatchString(t.task.Run.Workflow.TaskQueue) {
		return fmt.Errorf("native child workflow task queue must be a valid RFC 1123 DNS label: %s", t.GetTaskName())
	}
	return nil
}

func (t *RunWorkflowExtTaskBuilder) Build() (TemporalWorkflowFunc, error) {
	return func(ctx workflow.Context, _ any, state *utils.State) (any, error) {
		logger := workflow.GetLogger(ctx)
		logger.Debug("Run await status", "await", *t.task.Run.Await, "task", t.GetTaskName())

		res, err := t.runNativeWorkflow(ctx, state)
		if err != nil {
			// Flow-control directives (notably an `end` propagated from the
			// child) carry a meaningful payload that the do-task pipeline
			// needs to see. Preserve res so the directive can be dispatched
			// with the right output.
			if flow.IsControlError(err) {
				logger.Debug("Run task signalled a flow directive; preserving carried output",
					"task", t.GetTaskName(), "directive", err.Error())
				state.AddData(map[string]any{
					t.name: res,
				})
				return res, err
			}
			return nil, err
		}

		// Add the result to the state's data
		logger.Debug("Setting data to the state", "key", t.name)
		state.AddData(map[string]any{
			t.name: res,
		})

		return res, nil
	}, nil
}

func (t *RunWorkflowExtTaskBuilder) runNativeWorkflow(ctx workflow.Context, state *utils.State) (any, error) {
	logger := workflow.GetLogger(ctx)
	logger.Debug("Running a native child workflow", "task", t.GetTaskName())

	await := *t.task.Run.Await

	opts := workflow.ChildWorkflowOptions{TaskQueue: t.task.Run.Workflow.TaskQueue}
	if !await {
		opts.ParentClosePolicy = enums.PARENT_CLOSE_POLICY_ABANDON
	}

	ctx = workflow.WithChildOptions(ctx, opts)

	childInput, err := t.resolveNativeWorkflowInput(state)
	if err != nil {
		return nil, fmt.Errorf("resolve native child workflow input: %w", err)
	}
	future := workflow.ExecuteChildWorkflow(ctx, t.task.Run.Workflow.Type, childInput)

	if !await {
		if err := future.GetChildWorkflowExecution().Get(ctx, nil); err != nil {
			logger.Error("Error starting child workflow", "task", t.GetTaskName(), "error", err)
			return nil, fmt.Errorf("error starting child workflow: %w", err)
		}
		logger.Warn("Not waiting for child workspace response", "task", t.GetTaskName())
		return nil, nil
	}

	var res any
	if err := future.Get(ctx, &res); err != nil {
		// A `then: end` directive inside the child workflow crosses the
		// child workflow boundary as a typed Temporal ApplicationError.
		// Treat it as a deliberate workflow termination rather than a
		// child-workflow failure: surface it as flow.ErrEnd and carry the
		// child's effective output upward so the surrounding do-task
		// pipeline can keep propagating end with the right payload.
		if payload, isEnd := flow.DecodeEndApplicationError(err); isEnd {
			logger.Info("Run child workflow signalled end; propagating",
				"task", t.GetTaskName(), "workflow", t.task.Run.Workflow.Type, "carriedOutput", payload.Output)
			return payload.Output, flow.ErrEnd
		}

		logger.Error("Error executiing child workflow", "error", err)
		return nil, fmt.Errorf("error executiing child workflow: %w", err)
	}
	logger.Debug("Child workflow completed", "task", t.GetTaskName())

	return res, nil
}

func (t *RunWorkflowExtTaskBuilder) resolveNativeWorkflowInput(state *utils.State) (any, error) {
	input := t.task.Run.Workflow.Input
	if input == nil {
		input = map[string]any{}
	}
	return utils.TraverseAndEvaluateObj(model.NewObjectOrRuntimeExpr(input), nil, state)
}
