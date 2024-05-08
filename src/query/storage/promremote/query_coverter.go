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
	"time"

	"github.com/m3db/m3/src/query/storage"

	"github.com/golang/snappy"
	"github.com/pkg/errors"
	"github.com/prometheus/prometheus/prompb"
)

var errNilQuery = errors.New("received nil query or no samples in query")

func convertAndEncodeWriteQuery(queries []*storage.WriteQuery) ([]byte, int, error) {
	promQuery, samples := convertWriteQuery(queries)
	if promQuery == nil || len(promQuery.Timeseries) == 0 {
		return []byte{}, samples, errNilQuery
	}
	data, err := promQuery.Marshal()
	if err != nil {
		return nil, samples, err
	}
	return snappy.Encode(nil, data), samples, nil
}

func convertWriteQuery(queries []*storage.WriteQuery) (*prompb.WriteRequest, int) {
	if queries == nil || len(queries) == 0 {
		return nil, 0
	}
	ts := make([]prompb.TimeSeries, 0, len(queries))
	sampleCount := 0
	for _, query := range queries {
		if query == nil || len(query.Datapoints()) == 0 {
			continue
		}
		ourLabels := storage.TagsToPromLabels(query.Tags())
		labels := make([]prompb.Label, 0, len(ourLabels))
		for _, tag := range ourLabels {
			labels = append(labels, prompb.Label{
				Name:  string(tag.Name),
				Value: string(tag.Value),
			})
		}
		sampleCount += len(query.Datapoints())
		samples := make([]prompb.Sample, 0, len(query.Datapoints()))
		for _, dp := range query.Datapoints() {
			samples = append(samples, prompb.Sample{
				Value:     dp.Value,
				Timestamp: dp.Timestamp.ToNormalizedTime(time.Millisecond),
			})
		}
		ts = append(ts, prompb.TimeSeries{
			Labels:  labels,
			Samples: samples,
		})
	}

	return &prompb.WriteRequest{
		Timeseries: ts,
	}, sampleCount
}
