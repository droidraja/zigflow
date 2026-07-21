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
	"fmt"
	"io"
	"strings"
)

const (
	processRunContextTenantIDField           = "tenantId"
	processRunContextRunIDField              = "runId"
	processRunContextInitiatorSubjectIDField = "initiatorSubjectId"
)

// UnmarshalJSON rejects fields outside the v1 contract. Temporal's default
// JSON data converter invokes this method when decoding both public and
// internal dynamic invocation envelopes.
func (c *ProcessRunContext) UnmarshalJSON(data []byte) error {
	type processRunContext ProcessRunContext

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded processRunContext
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("decode processRunContext: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return fmt.Errorf("decode processRunContext: %w", err)
	}

	*c = ProcessRunContext(decoded)
	return c.Validate()
}

// Validate checks the required v1 identity fields.
func (c ProcessRunContext) Validate() error {
	required := []struct {
		name  string
		value string
	}{
		{name: processRunContextTenantIDField, value: c.TenantID},
		{name: processRunContextRunIDField, value: c.RunID},
		{name: processRunContextInitiatorSubjectIDField, value: c.InitiatorSubjectID},
	}
	for _, field := range required {
		if strings.TrimSpace(field.value) == "" {
			return fmt.Errorf("processRunContext.%s must be a non-empty string", field.name)
		}
	}
	return nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
