// Copyright  The OpenTelemetry Authors
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

package sampling

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
	"go.uber.org/zap"
)

func TestCombinedEvaluatorNotSampled(t *testing.T) {
	n1 := NewStringAttributeFilter(zap.NewNop(), "name", []string{"value"}, false, 0, false)
	n2, err := NewStatusCodeFilter(zap.NewNop(), []string{"ERROR"})
	if err != nil {
		t.FailNow()
	}

	combined := NewCombined(zap.NewNop(), []PolicyEvaluator{n1, n2})

	traces := pdata.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ils := rs.InstrumentationLibrarySpans().AppendEmpty()

	span := ils.Spans().AppendEmpty()
	span.Status().SetCode(pdata.StatusCodeError)
	span.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	trace := &TraceData{
		ReceivedBatches: []pdata.Traces{traces},
	}
	decision, err := combined.Evaluate(traceID, trace)
	require.NoError(t, err, "Failed to evaluate combined policy: %v", err)
	assert.Equal(t, decision, NotSampled)

}

func TestCombinedEvaluatorSampled(t *testing.T) {
	n1 := NewStringAttributeFilter(zap.NewNop(), "attribute_name", []string{"attribute_value"}, false, 0, false)
	n2, err := NewStatusCodeFilter(zap.NewNop(), []string{"ERROR"})
	require.NoError(t, err, "Failed to evaluate combined policy: %v", err)

	combined := NewCombined(zap.NewNop(), []PolicyEvaluator{n1, n2})

	traces := pdata.NewTraces()
	rs := traces.ResourceSpans().AppendEmpty()
	ils := rs.InstrumentationLibrarySpans().AppendEmpty()

	span := ils.Spans().AppendEmpty()
	span.Attributes().InsertString("attribute_name", "attribute_value")
	span.Status().SetCode(pdata.StatusCodeError)
	span.SetTraceID(pdata.NewTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}))
	span.SetSpanID(pdata.NewSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8}))

	trace := &TraceData{
		ReceivedBatches: []pdata.Traces{traces},
	}
	decision, err := combined.Evaluate(traceID, trace)
	require.NoError(t, err, "Failed to evaluate combined policy: %v", err)
	assert.Equal(t, decision, Sampled)

}
