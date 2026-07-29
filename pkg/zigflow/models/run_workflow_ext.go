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

package models

import (
	"github.com/open-workflow-specification/sdk-go/v4/model"
	"github.com/zigflow/zigflow/pkg/zigflow/extensions"
)

const keyRun = "run"

// RunWorkflowExtTask is Zigflow's explicit cross-task-queue child workflow
// extension. A run.workflow without taskQueue remains the Serverless Workflow
// SDK model and retains Zigflow's historical two-argument child contract.
type RunWorkflowExtTask struct {
	model.TaskBase `json:",inline"`
	Run            RunWorkflowExtConfiguration `json:"__zigflow_ext_run" validate:"required"`
}

// GetBase implements model.Task.
func (r *RunWorkflowExtTask) GetBase() *model.TaskBase {
	return &r.TaskBase
}

// RunWorkflowExtConfiguration contains only the cross-task-queue workflow
// variant claimed by this extension.
type RunWorkflowExtConfiguration struct {
	Await    *bool          `json:"await,omitempty"`
	Workflow RunWorkflowExt `json:"workflow" validate:"required"`
}

// RunWorkflowExt describes a native Temporal child workflow call. Input is an
// object because that is the authored Serverless Workflow input shape.
type RunWorkflowExt struct {
	Type      string         `json:"type"`
	TaskQueue string         `json:"taskQueue"`
	Input     map[string]any `json:"input,omitempty"`
}

type runWorkflowExtension struct{}

func (runWorkflowExtension) TaskType() string { return keyRun }

func (runWorkflowExtension) Claims(body any) bool {
	run, ok := body.(map[string]any)
	if !ok {
		return false
	}
	workflow, ok := run["workflow"].(map[string]any)
	if !ok {
		return false
	}
	_, hasTaskQueue := workflow["taskQueue"]
	return hasTaskQueue
}

func init() {
	extensions.RegisterExtension(runWorkflowExtension{}, func() model.Task {
		return &RunWorkflowExtTask{}
	})
}
