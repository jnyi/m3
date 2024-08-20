// Copyright (c) 2020 Uber Technologies, Inc.
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

package ingest

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLatencyBuckets(t *testing.T) {
	buckets, err := NewLatencyBuckets()
	require.NoError(t, err)

	// NB(r): Bucket length is tested just to sanity check how many buckets we are creating
	require.Equal(t, 14, len(buckets.WriteLatencyBuckets.AsDurations()))

	// NB(r): Bucket values are tested to sanity check they look right
	// nolint: lll
	expected := "[0s 200ms 400ms 600ms 800ms 1s 4s 7s 10s 35s 1m0s 5m0s 10m0s 15m0s]"
	actual := fmt.Sprintf("%v", buckets.WriteLatencyBuckets.AsDurations())
	require.Equal(t, expected, actual)

	// NB(r): Bucket length is tested just to sanity check how many buckets we are creating
	require.Equal(t, 18, len(buckets.IngestLatencyBuckets.AsDurations()))

	// NB(r): Bucket values are tested to sanity check they look right
	// nolint: lll
	expected = "[0s 200ms 400ms 600ms 800ms 1s 4s 7s 10s 35s 1m0s 5m0s 10m0s 15m0s 1h0m0s 1h30m0s 2h0m0s 2h30m0s]"
	actual = fmt.Sprintf("%v", buckets.IngestLatencyBuckets.AsDurations())
	require.Equal(t, expected, actual)
}
