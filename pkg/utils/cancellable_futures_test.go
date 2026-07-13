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

package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCancellableFuturesPreservesInsertionOrder(t *testing.T) {
	futures := &CancellableFutures{}
	futures.Add("first", CancellableFuture{})
	futures.Add("second", CancellableFuture{})
	futures.Add("third", CancellableFuture{})

	entries := futures.List()
	require.Len(t, entries, 3)
	assert.Equal(t, "first", entries[0].Key)
	assert.Equal(t, "second", entries[1].Key)
	assert.Equal(t, "third", entries[2].Key)
	assert.Equal(t, 3, futures.Length())
}

func TestCancellableFuturesReplacesDuplicateInPlace(t *testing.T) {
	futures := &CancellableFutures{}
	futures.Add("first", CancellableFuture{})
	futures.Add("second", CancellableFuture{})
	futures.Add("first", CancellableFuture{Cancel: func() {}})

	entries := futures.List()
	require.Len(t, entries, 2)
	assert.Equal(t, "first", entries[0].Key)
	assert.NotNil(t, entries[0].Future.Cancel)
	assert.Equal(t, "second", entries[1].Key)
	assert.Equal(t, 2, futures.Length())
}
