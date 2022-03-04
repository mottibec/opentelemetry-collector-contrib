// Copyright  OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package batch_test

import (
	"testing"

	"math/rand"

	"github.com/stretchr/testify/assert"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/awskinesisexporter/internal/batch"
)

func TestBatchingMessages(t *testing.T) {
	t.Parallel()

	b := batch.New()
	for i := 0; i < 948; i++ {
		assert.NoError(t, b.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}

	chunk, err := b.Chunk()
	assert.NoError(t, err, "Must not have return an error processing data")
	for _, records := range chunk {
		for _, record := range records {
			assert.Equal(t, []byte("foobar"), record.Data, "Must have the expected record value")
			assert.Equal(t, "fixed-string", *record.PartitionKey, "Must have the expected partition key")
		}
	}

	assert.Len(t, chunk, 2, "Must have split the batch into two chunks")
	assert.Len(t, chunk, 2, "Must not modify the stored data within the batch")

	assert.Error(t, b.AddRecord(nil, "fixed-string"), "Must error when invalid record provided")
	assert.Error(t, b.AddRecord([]byte("some data that is very important"), ""), "Must error when invalid partition key provided")
}

func TestCustomBatchSizeConstraints(t *testing.T) {
	t.Parallel()

	b := batch.New(
		batch.WithMaxRecordsPerBatch(1),
	)
	const records = 203
	for i := 0; i < records; i++ {
		assert.NoError(t, b.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}
	chunks, err := b.Chunk()
	assert.NoError(t, err, "Must not have return an error processing data")
	assert.Len(t, chunks, records, "Must have one batch per record added")
}

func TestBatchWithAggregation(t *testing.T) {
	b := batch.New(batch.WithAggregation(true))

	recordCount := 948
	for i := 0; i < recordCount; i++ {
		randomBytes := make([]byte, 1<<20/2)
		rand.Read(randomBytes)
		assert.NoError(t, b.AddRecord(randomBytes, "fixed-string"), "Must not error when adding elements into the batch")
	}

	chunk, err := b.Chunk()
	assert.NoError(t, err, "Must not have return an error processing data")
	assert.Len(t, chunk, 1, "Must have split the batch into two chunks")
	assert.Error(t, b.AddRecord(nil, "fixed-string"), "Must error when invalid record provided")
	assert.Error(t, b.AddRecord([]byte("some data that is very important"), ""), "Must error when invalid partition key provided")
}

func TestBatchWithAggregation_maxRecordSize(t *testing.T) {
	b := batch.New(batch.WithAggregation(true))

	recordCount := 948
	for i := 0; i < recordCount; i++ {
		randomBytes := make([]byte, 1<<20)
		rand.Read(randomBytes)
		assert.NoError(t, b.AddRecord(randomBytes, "fixed-string"), "Must not error when adding elements into the batch")
	}

	chunk, err := b.Chunk()
	assert.NoError(t, err, "Must not have return an error processing data")
	assert.Len(t, chunk, 2, "Must have split the batch into two chunks")
	assert.Error(t, b.AddRecord(nil, "fixed-string"), "Must error when invalid record provided")
	assert.Error(t, b.AddRecord([]byte("some data that is very important"), ""), "Must error when invalid partition key provided")
}

func BenchmarkChunkingRecords(b *testing.B) {
	bt := batch.New()
	for i := 0; i < 948; i++ {
		assert.NoError(b, bt.AddRecord([]byte("foobar"), "fixed-string"), "Must not error when adding elements into the batch")
	}
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		chunk, err := bt.Chunk()
		assert.NoError(b, err, "Must not have return an error processing data")
		assert.Len(b, chunk, 2, "Must have exactly two chunks")
	}
}
