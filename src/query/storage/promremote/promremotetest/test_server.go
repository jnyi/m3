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

// Package promremotetest provides test utilities.
package promremotetest

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"
	"github.com/prometheus/prometheus/prompb"
	"github.com/prometheus/prometheus/storage/remote"
	"github.com/stretchr/testify/assert"
)

// TestPromServer is a fake http server handling prometheus remote write. Intended for test usage.
type TestPromServer struct {
	mu               sync.Mutex
	totalSamples     int
	lastWriteRequest *prompb.WriteRequest
	respErr          *respErr
	t                *testing.T
	svr              *httptest.Server
	jitter           bool
}

type respErr struct {
	error  string
	status int
}

// NewServer creates new instance of a fake server.
func NewServer(t *testing.T, jitter bool) *TestPromServer {
	testPromServer := &TestPromServer{t: t, jitter: jitter}

	mux := http.NewServeMux()
	mux.HandleFunc("/write", testPromServer.handleWrite)

	testPromServer.svr = httptest.NewServer(mux)

	return testPromServer
}

func (s *TestPromServer) handleWrite(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	assert.Equal(s.t, r.Header.Get("content-encoding"), "snappy")
	assert.Equal(s.t, r.Header.Get("content-type"), "application/x-protobuf")

	req, err := remote.DecodeWriteRequest(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	now := xtime.Now()
	if s.jitter {
		r := rand.Intn(1200)
		time.Sleep(time.Duration(r) * time.Millisecond)
		if xtime.Now().After(now.Add(1 * time.Second)) {
			http.Error(w, "timeout", http.StatusRequestTimeout)
			return
		}
	}
	s.lastWriteRequest = req
	for _, ts := range req.Timeseries {
		s.totalSamples += len(ts.Samples)
	}
	if s.respErr != nil {
		http.Error(w, s.respErr.error, s.respErr.status)
		return
	}
}

// GetTotalSamples returns total number of samples received.
func (s *TestPromServer) GetTotalSamples() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalSamples
}

// GetLastWriteRequest returns the last recorded write request.
func (s *TestPromServer) GetLastWriteRequest() *prompb.WriteRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastWriteRequest
}

// WriteAddr returns http address of a write endpoint.
func (s *TestPromServer) WriteAddr() string {
	return fmt.Sprintf("%s/write", s.svr.URL)
}

// SetError sets error that will be returned for all incoming requests.
func (s *TestPromServer) SetError(body string, status int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = &respErr{error: body, status: status}
}

// Reset resets state to default.
func (s *TestPromServer) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.respErr = nil
	s.lastWriteRequest = nil
}

// Close stops underlying http server.
func (s *TestPromServer) Close() {
	s.svr.Close()
}
