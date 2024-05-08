// Copyright (c) 2021  Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package promremote

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteQueryConverter(t *testing.T) {
	now := xtime.Now()
	dp := ts.Datapoint{
		Timestamp: now,
		Value:     42,
	}
	dp1 := ts.Datapoint{
		Timestamp: now.Add(-time.Minute),
		Value:     3,
	}
	dp2 := ts.Datapoint{
		Timestamp: now.Add(time.Minute),
		Value:     55,
	}
	tag := models.Tag{
		Name:  []byte("test_tag_name"),
		Value: []byte("test_tag_value"),
	}
	convertedToLabel := prompb.Label{
		Name:  "test_tag_name",
		Value: "test_tag_value",
	}
	covertedToSample := prompb.Sample{
		Timestamp: now.ToNormalizedTime(time.Millisecond),
		Value:     42,
	}

	tcs := []struct {
		name     string
		input    storage.WriteQueryOptions
		expected *prompb.WriteRequest
		samples  int
	}{
		{
			name: "single datapoint",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions(),
					Tags: []models.Tag{tag},
				},
				Datapoints: ts.Datapoints{dp},
				Unit:       xtime.Millisecond,
			},
			expected: promWriteRequest(prompb.TimeSeries{
				Labels:  []prompb.Label{convertedToLabel},
				Samples: []prompb.Sample{covertedToSample},
			}),
			samples: 1,
		},
		{
			name: "duplicate tags and samples",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions().SetAllowTagNameDuplicates(true),
					Tags: []models.Tag{tag, tag},
				},
				Datapoints: ts.Datapoints{dp, dp},
				Unit:       xtime.Millisecond,
			},
			expected: promWriteRequest(prompb.TimeSeries{
				Labels:  []prompb.Label{convertedToLabel, convertedToLabel},
				Samples: []prompb.Sample{covertedToSample, covertedToSample},
			}),
			samples: 2,
		},
		{
			name: "out of order samples",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions(),
					Tags: []models.Tag{tag},
				},
				Datapoints: ts.Datapoints{dp, dp1, dp2}, // out of order in timestamp
				Unit:       xtime.Millisecond,
			},
			expected: promWriteRequest(prompb.TimeSeries{
				Labels: []prompb.Label{convertedToLabel},
				Samples: []prompb.Sample{
					{Timestamp: dp1.Timestamp.ToNormalizedTime(time.Millisecond), Value: dp1.Value},
					{Timestamp: dp.Timestamp.ToNormalizedTime(time.Millisecond), Value: dp.Value},
					{Timestamp: dp2.Timestamp.ToNormalizedTime(time.Millisecond), Value: dp2.Value},
				},
			}),
			samples: 3,
		},
		{
			name: "overrides metric name tag",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions().SetMetricName(tag.Name),
					Tags: []models.Tag{tag},
				},
				Datapoints: ts.Datapoints{dp},
				Unit:       xtime.Millisecond,
			},
			expected: promWriteRequest(prompb.TimeSeries{
				Labels: []prompb.Label{{
					Name:  "__name__",
					Value: convertedToLabel.Value,
				}},
				Samples: []prompb.Sample{covertedToSample},
			}),
			samples: 1,
		},
		{
			name: "overrides bucket name name tag",
			input: storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions().SetBucketName(tag.Name),
					Tags: []models.Tag{tag},
				},
				Datapoints: ts.Datapoints{dp},
				Unit:       xtime.Millisecond,
			},
			expected: promWriteRequest(prompb.TimeSeries{
				Labels: []prompb.Label{{
					Name:  "le",
					Value: convertedToLabel.Value,
				}},
				Samples: []prompb.Sample{covertedToSample},
			}),
			samples: 1,
		},
	}

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			q, err := storage.NewWriteQuery(tc.input)
			require.NoError(t, err)
			r, samples := convertWriteQuery([]*storage.WriteQuery{q})
			assert.Equal(t, tc.expected, r)
			assert.Equal(t, tc.samples, samples)
		})
	}
}

func TestConvertQueryNil(t *testing.T) {
	r, samples := convertWriteQuery(nil)
	assert.Nil(t, r)
	assert.Equal(t, 0, samples)
}

func TestEncodeWriteQuery(t *testing.T) {
	data, samples, err := convertAndEncodeWriteQuery(nil)
	require.Error(t, err)
	assert.Len(t, data, 0)
	assert.Equal(t, 0, samples)
	assert.Contains(t, err.Error(), "received nil query")
}

func promWriteRequest(ts prompb.TimeSeries) *prompb.WriteRequest {
	return &prompb.WriteRequest{Timeseries: []prompb.TimeSeries{ts}}
}
