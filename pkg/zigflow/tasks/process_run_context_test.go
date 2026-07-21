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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcessRunContextRequiresExactNonEmptyFields(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		message string
	}{
		{name: "missing tenant", payload: `{"runId":"run","initiatorSubjectId":"subject"}`, message: "tenantId must be a non-empty string"},
		{name: "missing run", payload: `{"tenantId":"tenant","initiatorSubjectId":"subject"}`, message: "runId must be a non-empty string"},
		{name: "missing initiator", payload: `{"tenantId":"tenant","runId":"run"}`, message: "initiatorSubjectId must be a non-empty string"},
		{
			name:    "whitespace",
			payload: `{"tenantId":" ","runId":"run","initiatorSubjectId":"subject"}`,
			message: "tenantId must be a non-empty string",
		},
		{name: "wrong type", payload: `{"tenantId":1,"runId":"run","initiatorSubjectId":"subject"}`, message: "cannot unmarshal number"},
		{
			name:    "extra field",
			payload: `{"tenantId":"tenant","runId":"run","initiatorSubjectId":"subject","extra":true}`,
			message: `unknown field "extra"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var context ProcessRunContext
			err := json.Unmarshal([]byte(test.payload), &context)
			require.ErrorContains(t, err, test.message)
		})
	}

	var context ProcessRunContext
	require.NoError(t, json.Unmarshal([]byte(
		`{"tenantId":"tenant","runId":"run","initiatorSubjectId":"subject"}`,
	), &context))
	require.Equal(t, ProcessRunContext{
		TenantID:           "tenant",
		RunID:              "run",
		InitiatorSubjectID: "subject",
	}, context)
}
