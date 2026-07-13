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
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/zigflow/zigflow/pkg/utils"
)

func TestInternalInvocationArgsPreserveStaticContract(t *testing.T) {
	builder := newTestDoTaskBuilder("static-invocation")
	input := map[string]any{"request": "123"}
	state := utils.NewState()

	args := builder.internalInvocationArgs(input, state)

	require.Len(t, args, 2)
	require.Same(t, state, args[1])
	require.Equal(t, input, args[0])
}

func TestInternalInvocationArgsCaptureDynamicContractOutsideState(t *testing.T) {
	const output = "output"

	definition := []byte("definition-snapshot-not-in-state")
	builder := newTestDoTaskBuilder("dynamic-invocation")
	builder.taskOpts = &TaskOpts{DynamicExecution: NewDynamicExecutionOptions(definition)}
	state := utils.NewState()
	state.Env = map[string]any{"DEPLOYMENT": "recorded"}
	state.Input = map[string]any{"request": "123"}
	state.Context = map[string]any{"exported": true}
	state.Data = map[string]any{"value": "data"}
	state.Output = map[string]any{"value": output}

	definition[0] = 'X'
	args := builder.internalInvocationArgs(state.Input, state)

	require.Len(t, args, 1)
	invocation, ok := args[0].(InternalWorkflowInvocation)
	require.True(t, ok)
	require.Equal(t, InternalWorkflowInvocationVersion, invocation.Version)
	require.Equal(t, []byte("definition-snapshot-not-in-state"), invocation.Definition)
	require.Equal(t, state.Env, invocation.RecordedEnv)
	require.Equal(t, state.Input, invocation.OriginalInput)
	require.Same(t, state, invocation.State)

	statePayload, err := json.Marshal(invocation.State)
	require.NoError(t, err)
	require.False(t, bytes.Contains(statePayload, invocation.Definition),
		"the definition snapshot must not be copied into built-in activity state")
}
