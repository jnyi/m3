package composite

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"go.uber.org/zap"
)

type compositeStorage struct {
	name   string
	stores []storage.Storage
	logger *zap.Logger
}

func Compose(logger *zap.Logger, stores ...storage.Storage) storage.Storage {
	var name strings.Builder
	name.WriteString("compositedStorage")
	logger.Info("construct a composite storage",
		zap.String("name", name.String()),
		zap.Int("store_size", len(stores)+1))
	return &compositeStorage{
		name:   name.String(),
		stores: stores,
		logger: logger,
	}
}

func (s *compositeStorage) Write(ctx context.Context, query *storage.WriteQuery) error {
	for _, store := range s.stores {
		if err := store.Write(ctx, query); err != nil {
			s.logger.Error("composite storage write error",
				zap.String("store", store.Name()),
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *compositeStorage) ErrorBehavior() storage.ErrorBehavior {
	return storage.BehaviorWarn
}

func (s *compositeStorage) Type() storage.Type {
	return storage.TypeLocalDC
}

func (s *compositeStorage) Name() string {
	return s.name
}

func (s *compositeStorage) Close() error {
	for _, store := range s.stores {
		if err := store.Close(); err != nil {
			s.logger.Error("composite storage close error",
				zap.String("store", store.Name()),
				zap.Error(err))
			return err
		}
	}
	return nil
}

func (s *compositeStorage) FetchProm(
	c context.Context,
	q *storage.FetchQuery,
	opts *storage.FetchOptions,
) (storage.PromResult, error) {
	for _, store := range s.stores {
		r, err := store.FetchProm(c, q, opts)
		if err == nil {
			return r, err
		}
	}
	return storage.PromResult{}, unimplementedError(s.name, "FetchProm")
}

func (s *compositeStorage) FetchBlocks(
	c context.Context,
	q *storage.FetchQuery,
	opts *storage.FetchOptions,
) (block.Result, error) {
	for _, store := range s.stores {
		r, err := store.FetchBlocks(c, q, opts)
		if err == nil {
			return r, err
		}
	}
	return block.Result{}, unimplementedError(s.name, "FetchBlocks")
}

func (s *compositeStorage) FetchCompressed(
	c context.Context,
	q *storage.FetchQuery,
	opts *storage.FetchOptions,
) (consolidators.MultiFetchResult, error) {
	for _, store := range s.stores {
		r, err := store.FetchCompressed(c, q, opts)
		if err == nil {
			return r, err
		}
	}
	return nil, unimplementedError(s.name, "FetchCompressed")
}

func (s *compositeStorage) SearchSeries(
	c context.Context,
	q *storage.FetchQuery,
	opts *storage.FetchOptions,
) (*storage.SearchResults, error) {
	for _, store := range s.stores {
		r, err := store.SearchSeries(c, q, opts)
		if err == nil {
			return r, err
		}
	}
	return nil, unimplementedError(s.name, "SearchSeries")
}

func (s *compositeStorage) CompleteTags(
	c context.Context,
	q *storage.CompleteTagsQuery,
	opts *storage.FetchOptions,
) (*consolidators.CompleteTagsResult, error) {
	for _, store := range s.stores {
		r, err := store.CompleteTags(c, q, opts)
		if err == nil {
			return r, err
		}
	}
	return nil, unimplementedError(s.name, "CompleteTags")
}

func (s *compositeStorage) QueryStorageMetadataAttributes(
	c context.Context,
	start, end time.Time,
	opts *storage.FetchOptions,
) ([]storagemetadata.Attributes, error) {
	for _, store := range s.stores {
		r, err := store.QueryStorageMetadataAttributes(c, start, end, opts)
		if err == nil {
			return r, err
		}
	}
	return nil, unimplementedError(s.name, "QueryStorageMetadataAttributes")
}

func unimplementedError(storeName, name string) error {
	return fmt.Errorf("%s: %s method is not supported", storeName, name)
}
