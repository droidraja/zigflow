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

package zigflow

import (
	"bytes"
	"fmt"
	"strings"

	swUtils "github.com/serverlessworkflow/sdk-go/v3/impl/utils"
	"github.com/zigflow/zigflow/pkg/cloudevents"
	"github.com/zigflow/zigflow/pkg/zigflow/tasks"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const (
	// DynamicWorkflowInputVersion is the public dynamic input contract version.
	DynamicWorkflowInputVersion = 1

	dynamicWorkflowInputErrorType       = "zigflow.dynamic.input"
	dynamicWorkflowPreparationErrorType = "zigflow.dynamic.preparation"
	dynamicWorkflowBuildErrorType       = "zigflow.dynamic.build"
	dynamicWorkflowDispatchErrorType    = "zigflow.dynamic.dispatch"
)

// DynamicWorkflowInput is the versioned public input recorded in the Temporal
// workflow start event. Definition contains the complete YAML or JSON snapshot.
type DynamicWorkflowInput struct {
	Version    int    `json:"version"`
	Definition []byte `json:"definition"`
	Input      any    `json:"input,omitempty"`
}

// DynamicWorkflowOptions contains immutable worker runtime options captured by
// a dynamic workflow handler factory.
type DynamicWorkflowOptions struct {
	Envvars  map[string]any
	TaskOpts *tasks.TaskOpts
}

// NewDynamicWorkflowHandler creates a Temporal dynamic workflow fallback. Each
// invocation prepares its recorded definition and builds a new local registry.
// It does not access a mutable definition catalogue or register workflow types
// on the SDK worker.
func NewDynamicWorkflowHandler(
	opts DynamicWorkflowOptions,
) func(workflow.Context, converter.EncodedValues) (any, error) {
	capturedEnv := swUtils.DeepClone(opts.Envvars)
	capturedTaskOpts := cloneTaskOpts(opts.TaskOpts)
	capturedTaskOpts.WorkflowRegistrar = nil
	capturedTaskOpts.ActivityDispatchPolicy = tasks.ActivityDispatchDynamic

	return func(ctx workflow.Context, args converter.EncodedValues) (any, error) {
		var input DynamicWorkflowInput
		if err := args.Get(&input); err != nil {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowInputErrorType,
				fmt.Errorf("decode dynamic workflow input: %w", err),
			)
		}
		if input.Version != DynamicWorkflowInputVersion {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowInputErrorType,
				fmt.Errorf(
					"unsupported dynamic workflow input version %d, supported version is %d",
					input.Version,
					DynamicWorkflowInputVersion,
				),
			)
		}
		if len(bytes.TrimSpace(input.Definition)) == 0 {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowInputErrorType,
				fmt.Errorf("dynamic workflow definition must not be empty"),
			)
		}

		doc, err := PrepareWorkflow(input.Definition)
		if err != nil {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowPreparationErrorType,
				fmt.Errorf("prepare dynamic workflow definition: %w", err),
			)
		}

		info := workflow.GetInfo(ctx)
		if doc.Document.Namespace != info.TaskQueueName {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowDispatchErrorType,
				fmt.Errorf(
					"workflow definition task queue %q does not match execution task queue %q",
					doc.Document.Namespace,
					info.TaskQueueName,
				),
			)
		}

		var recordedEnv map[string]any
		if err := workflow.SideEffect(ctx, func(workflow.Context) any {
			return swUtils.DeepClone(capturedEnv)
		}).Get(&recordedEnv); err != nil {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowPreparationErrorType,
				fmt.Errorf("record dynamic workflow environment snapshot: %w", err),
			)
		}
		if recordedEnv == nil {
			recordedEnv = map[string]any{}
		}

		registry, err := BuildPreparedWorkflow(doc, WorkflowBuildOptions{
			Envvars:  recordedEnv,
			Emitter:  cloudevents.NewNoOpEvents(),
			TaskOpts: capturedTaskOpts,
		})
		if err != nil {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowBuildErrorType,
				fmt.Errorf("build dynamic workflow definition: %w", err),
			)
		}

		workflowType := info.WorkflowType.Name
		fn, ok := registry.Workflow(workflowType)
		if !ok {
			return nil, newDynamicWorkflowError(
				dynamicWorkflowDispatchErrorType,
				fmt.Errorf(
					"workflow type %q is not executable from this definition, executable types: %s",
					workflowType,
					strings.Join(registry.Names(), ", "),
				),
			)
		}

		return fn(ctx, input.Input, nil)
	}
}

func newDynamicWorkflowError(errorType string, err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), errorType, err)
}
