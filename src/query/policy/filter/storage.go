// Copyright (c) 2018 Uber Technologies, Inc.
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

package filter

import (
	"regexp"
	"strings"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
)
const (
	// NB: This is specific to Databricks!
	storageNameLabelKey = "shardName"
	localStorageName = "local_store"
	remoteStorePrefix = "remote_store_"
)
// Storage determines whether storage can fulfil the query
type Storage func(query storage.Query, store storage.Storage) bool

// LocalOnly filters out all remote storages
func LocalOnly(_ storage.Query, store storage.Storage) bool {
	return store.Type() == storage.TypeLocalDC
}

// RemoteOnly filters out any non-remote storages
func RemoteOnly(_ storage.Query, store storage.Storage) bool {
	return store.Type() == storage.TypeRemoteDC
}

// AllowAll does not filter any storages
func AllowAll(_ storage.Query, _ storage.Storage) bool {
	return true
}

// AllowNone filters all storages
func AllowNone(_ storage.Query, _ storage.Storage) bool {
	return false
}

// Allow only storages which meet the relevant filters in the query.
func ReadOptimizedFilter(query storage.Query, store storage.Storage) bool {
	if store.Name() == localStorageName {
		return true
	}
	fetchQuery, ok := query.(*storage.FetchQuery)
	if !ok {
		// This filter only applies to fetch queries. The configration is wrong!
		return true
	}	
	getShardName := func(storeName string) string {
		// The format of store name is "remote_store_<env>-<cloud>-<shardName>", e.g. "remote_store_prod-aws-nvirginia-prod".
		// "remote_store_" prefix is added by M3: https://github.com/databricks/m3/blob/8d4161053f6f951beeeb6689472c450b5c05994d/src/query/storage/remote/storage.go#L115-L116
		// The other parts are specific to Databricks. See: https://github.com/databricks/universe/blob/master/kubernetes/config/m3/params/m3-shard-flags.jsonnet.TEMPLATE#L2837
		if !strings.HasPrefix(storeName, remoteStorePrefix) {
			return storeName
		}
		parts := strings.Split(storeName[len(remoteStorePrefix):], "-")
		if len(parts) < 3 {
			return storeName[len(remoteStorePrefix):]
		}
		return strings.Join(parts[2:], "-")
	}
	shardName := getShardName(store.Name())
	for _, tagMatcher := range fetchQuery.TagMatchers {
		if string(tagMatcher.Name) == storageNameLabelKey {
			switch tagMatcher.Type {
			case models.MatchEqual:
				if shardName != string(tagMatcher.Value) {
					return false
				}
			case models.MatchRegexp:
				matched, err := regexp.MatchString("^(" + string(tagMatcher.Value) + ")$", shardName)
				if err == nil && !matched {
					return false
				}
			case models.MatchNotEqual:
				if shardName == string(tagMatcher.Value) {
					return false
				}
			case models.MatchNotRegexp:
				matched, err := regexp.MatchString("^(" + string(tagMatcher.Value) + ")$", shardName)
				if err == nil && matched {
					return false
				}
			}
		}
	}
	return true
}

// StorageCompleteTags determines whether storage can fulfil the complete tag query
type StorageCompleteTags func(query storage.CompleteTagsQuery, store storage.Storage) bool

// CompleteTagsLocalOnly filters out all remote storages
func CompleteTagsLocalOnly(_ storage.CompleteTagsQuery, store storage.Storage) bool {
	return store.Type() == storage.TypeLocalDC
}

// CompleteTagsRemoteOnly filters out any non-remote storages
func CompleteTagsRemoteOnly(_ storage.CompleteTagsQuery, store storage.Storage) bool {
	return store.Type() == storage.TypeRemoteDC
}

// CompleteTagsAllowAll does not filter any storages
func CompleteTagsAllowAll(_ storage.CompleteTagsQuery, _ storage.Storage) bool {
	return true
}

// CompleteTagsAllowNone filters all storages
func CompleteTagsAllowNone(_ storage.CompleteTagsQuery, _ storage.Storage) bool {
	return false
}
