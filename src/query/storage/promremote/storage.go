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
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/pkg/errors"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

const metricsScope = "prom_remote_storage"

var errorReadingBody = []byte("error reading body")

var errNoEndpoints = errors.New("write did not match any of known endpoints")

// WriteQueue This is used by a single thread go routine
type WriteQueue struct {
	t        tenant
	capacity int
	queries  []*storage.WriteQuery
}

func NewWriteQueue(t tenant, capacity int) *WriteQueue {
	return &WriteQueue{
		t:        t,
		capacity: capacity,
		queries:  make([]*storage.WriteQuery, 0, capacity),
	}
}

func (wq *WriteQueue) pop() []*storage.WriteQuery {
	res := wq.queries
	wq.queries = make([]*storage.WriteQuery, 0, wq.capacity)
	return res
}

func (wq *WriteQueue) Add(query *storage.WriteQuery) ([]*storage.WriteQuery, error) {
	if len(wq.queries) >= wq.capacity {
		return wq.pop(), errors.New(wq.t.name + " write queue is Full")
	}
	wq.queries = append(wq.queries, query)
	return nil, nil
}

func (wq *WriteQueue) Flush(ctx context.Context, p *promStorage, force bool) {
	size := len(wq.queries)
	if size < 10 {
		if size == 0 || !force {
			p.logger.Warn("don't do tick flush for small batch with less than 10 samples, "+
				"samples will be evently written to remote storage as it accumulates.",
				zap.String("tenant", wq.t.name), zap.Int("size", size))
			return
		}
	}
	data := wq.pop()
	p.tickWrite.Inc(int64(len(data)))
	p.workerPool.Go(func() {
		if err := p.writeBatch(ctx, wq.t, data); err != nil {
			p.logger.Error("error writing async batch",
				zap.String("tenant", wq.t.name),
				zap.Error(err))
		}
	})
}

// NewStorage returns new Prometheus remote write compatible storage
func NewStorage(opts Options) (storage.Storage, error) {
	client := xhttp.NewHTTPClient(opts.httpOptions)
	scope := opts.scope.SubScope(metricsScope)
	if opts.poolSize < 1 {
		return nil, errors.New("poolSize must be greater than 0")
	}
	if opts.queueSize < 1 {
		return nil, errors.New("queueSize must be greater than 0 to batch writes")
	}
	// Use fixed
	queriesWithFixedTenants := make(map[tenant]*WriteQueue, len(opts.tenantRules)+1)
	for _, endpoint := range opts.endpoints {
		defaultTenant := tenant{
			name: opts.tenantDefault,
			attr: endpoint.attributes,
		}
		queriesWithFixedTenants[defaultTenant] = NewWriteQueue(defaultTenant, opts.queueSize)
		for _, rule := range opts.tenantRules {
			t := tenant{
				name: rule.Tenant,
				attr: endpoint.attributes,
			}
			queriesWithFixedTenants[t] = NewWriteQueue(t, opts.queueSize)
		}
	}
	s := &promStorage{
		opts:            opts,
		client:          client,
		endpointMetrics: initEndpointMetrics(opts.endpoints, scope),
		scope:           scope,
		droppedWrites:   scope.Counter("dropped_writes"),
		enqueued:        scope.Counter("enqueued"),
		batchWrite:      scope.Counter("batch_write"),
		tickWrite:       scope.Counter("tick_write"),
		logger:          opts.logger,
		queryQueue:      make(chan *storage.WriteQuery, opts.queueSize),
		workerPool:      xsync.NewWorkerPool(opts.poolSize),
		pendingQuery:    queriesWithFixedTenants,
		noTenantFound:   scope.Counter("no_tenant_found"),
	}
	s.startAsync()
	return s, nil
}

type promStorage struct {
	unimplementedPromStorageMethods
	opts            Options
	client          *http.Client
	endpointMetrics map[string]instrument.MethodMetrics
	scope           tally.Scope
	droppedWrites   tally.Counter
	enqueued        tally.Counter
	batchWrite      tally.Counter
	tickWrite       tally.Counter
	logger          *zap.Logger
	queryQueue      chan *storage.WriteQuery
	workerPool      xsync.WorkerPool
	pendingQuery    map[tenant]*WriteQueue
	noTenantFound   tally.Counter
}

type tenant struct {
	name string
	attr storagemetadata.Attributes
}

func (p *promStorage) getTenant(query *storage.WriteQuery) tenant {
	for _, rule := range p.opts.tenantRules {
		if ok := rule.Filter.MatchTags(query.Tags()); ok {
			return tenant{
				name: rule.Tenant,
				attr: query.Attributes(),
			}
		}
	}
	return tenant{
		name: p.opts.tenantDefault,
		attr: query.Attributes(),
	}
}

func (p *promStorage) startAsync() {
	p.logger.Info("Start prometheus remote write storage async job",
		zap.Int("poolSize", p.opts.poolSize))
	p.workerPool.Init()
	ticker := time.NewTicker(*p.opts.tickDuration)
	go func() {
		for {
			ctx := context.Background()
			select {
			case query := <-p.queryQueue:
				t := p.getTenant(query)
				if _, ok := p.pendingQuery[t]; !ok {
					p.noTenantFound.Inc(1)
					p.logger.Error("no pre-defined tenant found",
						zap.String("tenant", t.name),
						zap.String("timeseries", query.String()))
					// use default tenant
					t = tenant{
						name: p.opts.tenantDefault,
						attr: query.Attributes(),
					}
				}
				if data, err := p.pendingQuery[t].Add(query); err != nil {
					if data == nil {
						p.scope.Tagged(map[string]string{"tenant": t.name}).Counter("write_queue_error").Inc(1)
						p.logger.Error("Empty write queue",
							zap.String("tenant", t.name),
							zap.String("timeseries", query.String()),
							zap.Error(err))
					} else {
						p.batchWrite.Inc(int64(len(data)))
						p.workerPool.Go(func() {
							if err := p.writeBatch(ctx, t, data); err != nil {
								p.logger.Error("error writing async batch",
									zap.String("tenant", t.name),
									zap.Error(err))
							}
						})
					}
				}
			case <-ticker.C:
				for _, queue := range p.pendingQuery {
					queue.Flush(ctx, p, false)
				}
			}
		}
	}()
}

func (p *promStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	if query != nil {
		p.queryQueue <- query
		p.enqueued.Inc(1)
	}
	return nil
}

func (p *promStorage) writeBatch(ctx context.Context, tenant tenant, queries []*storage.WriteQuery) error {
	p.logger.Debug("async write batch",
		zap.String("tenant", tenant.name),
		zap.Duration("retention", tenant.attr.Retention),
		zap.Duration("resolution", tenant.attr.Resolution),
		zap.Int("size", len(queries)))
	encoded, err := convertAndEncodeWriteQuery(queries)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	multiErr := xerrors.NewMultiError()
	var errLock sync.Mutex
	atLeastOneEndpointMatched := false
	for _, endpoint := range p.opts.endpoints {
		endpoint := endpoint
		if endpoint.attributes.Resolution != tenant.attr.Resolution ||
			endpoint.attributes.Retention != tenant.attr.Retention {
			continue
		}

		metrics := p.endpointMetrics[endpoint.name]
		atLeastOneEndpointMatched = true

		wg.Add(1)
		go func() {
			defer wg.Done()
			err := p.writeSingle(ctx, metrics, endpoint, tenant, bytes.NewReader(encoded))
			if err != nil {
				errLock.Lock()
				multiErr = multiErr.Add(err)
				errLock.Unlock()
				return
			}
		}()
	}

	wg.Wait()

	if !atLeastOneEndpointMatched {
		p.droppedWrites.Inc(1)
		multiErr = multiErr.Add(errNoEndpoints)
		p.logger.Warn(
			"write did not match any of known endpoints",
			zap.Duration("retention", tenant.attr.Retention),
			zap.Duration("resolution", tenant.attr.Resolution),
		)
	}
	return multiErr.FinalError()
}

func (p *promStorage) Type() storage.Type {
	return storage.TypeRemoteDC
}

func (p *promStorage) Close() error {
	close(p.queryQueue)
	ctx := context.Background()
	var wg sync.WaitGroup
	for _, queries := range p.pendingQuery {
		wg.Add(1)
		queries.Flush(ctx, p, true)
	}
	wg.Wait()
	p.client.CloseIdleConnections()
	return nil
}

func (p *promStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorFail
}

func (p *promStorage) Name() string {
	return "prom-remote"
}

func (p *promStorage) writeSingle(
	ctx context.Context,
	metrics instrument.MethodMetrics,
	endpoint EndpointOptions,
	tenant tenant,
	encoded io.Reader,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.address, encoded)
	if err != nil {
		return err
	}
	req.Header.Set("content-encoding", "snappy")
	req.Header.Set(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)
	if endpoint.apiToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("Basic %s",
			base64.StdEncoding.EncodeToString([]byte(
				fmt.Sprintf("%s:%s", tenant.name, endpoint.apiToken),
			)),
		))
	}
	if len(endpoint.otherHeaders) > 0 {
		for k, v := range endpoint.otherHeaders {
			// set headers defined in remote endpoint options
			req.Header.Set(k, v)
		}
	}
	req.Header.Set(endpoint.tenantHeader, tenant.name)

	start := time.Now()
	resp, err := p.client.Do(req)
	methodDuration := time.Since(start)
	if err != nil {
		metrics.ReportError(methodDuration)
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode/100 != 2 {
		metrics.ReportError(methodDuration)
		response, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			p.logger.Error("error reading body", zap.Error(err))
			response = errorReadingBody
		}
		genericError := fmt.Errorf(
			"expected status code 2XX: actual=%v, address=%v, resp=%s",
			resp.StatusCode, endpoint.address, response,
		)
		if resp.StatusCode < 500 && resp.StatusCode != http.StatusTooManyRequests {
			return xerrors.NewInvalidParamsError(genericError)
		}
		return genericError
	}
	metrics.ReportSuccess(methodDuration)
	return nil
}

func initEndpointMetrics(endpoints []EndpointOptions, scope tally.Scope) map[string]instrument.MethodMetrics {
	metrics := make(map[string]instrument.MethodMetrics, len(endpoints))
	for _, endpoint := range endpoints {
		endpointScope := scope.Tagged(map[string]string{"endpoint_name": endpoint.name})
		methodMetrics := instrument.NewMethodMetrics(endpointScope, "writeSingle", instrument.TimerOptions{
			Type:             instrument.HistogramTimerType,
			HistogramBuckets: tally.DefaultBuckets,
		})
		metrics[endpoint.name] = methodMetrics
	}
	return metrics
}

var _ storage.Storage = &promStorage{}

type unimplementedPromStorageMethods struct{}

func (p *unimplementedPromStorageMethods) FetchProm(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (storage.PromResult, error) {
	return storage.PromResult{}, unimplementedError("FetchProm")
}

func (p *unimplementedPromStorageMethods) FetchBlocks(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (block.Result, error) {
	return block.Result{}, unimplementedError("FetchBlocks")
}

func (p *unimplementedPromStorageMethods) FetchCompressed(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	return nil, unimplementedError("FetchCompressed")
}

func (p *unimplementedPromStorageMethods) SearchSeries(
	_ context.Context,
	_ *storage.FetchQuery,
	_ *storage.FetchOptions,
) (*storage.SearchResults, error) {
	return nil, unimplementedError("SearchSeries")
}

func (p *unimplementedPromStorageMethods) CompleteTags(
	_ context.Context,
	_ *storage.CompleteTagsQuery,
	_ *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	return nil, unimplementedError("CompleteTags")
}

func (p *unimplementedPromStorageMethods) QueryStorageMetadataAttributes(
	_ context.Context,
	_, _ time.Time,
	_ *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	return nil, unimplementedError("QueryStorageMetadataAttributes")
}

func unimplementedError(name string) error {
	return fmt.Errorf("promStorage: %s method is not supported", name)
}
