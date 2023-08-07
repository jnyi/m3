package etcd

import (
	"fmt"
	etcd "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/generated/proto/kvpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"go.uber.org/zap"
)

func getEctdClient(endpoint string) (kv.Store, error) {
	cluster := etcd.NewCluster().
		SetZone("embedded").
		SetEndpoints([]string{endpoint})
	opts := etcd.NewOptions().
		SetService("m3db").
		SetEnv("m3/m3db").
		SetZone("embedded").
		SetClusters([]etcd.Cluster{cluster})
	client, err := etcd.NewConfigServiceClient(opts)
	if err != nil {
		return nil, err
	}
	kvOpts := kv.NewOverrideOptions()
	kvOpts.SetZone("embedded")
	kvOpts.SetEnvironment("m3db")
	return client.Store(kvOpts)
}

func DoGet(
	endpoint string,
	logger *zap.Logger,
) ([]byte, error) {
	store, err := getEctdClient(endpoint)
	if err != nil {
		logger.Error("Can't access kv store", zap.Error(err))
		return nil, err
	}
	value, err := store.Get(kvconfig.QueryLimits)
	if err != nil {
		logger.Warn("error resolving query limit", zap.Error(err))
		return nil, err
	}

	dynamicLimits := &kvpb.QueryLimits{}
	err = value.Unmarshal(dynamicLimits)
	if err != nil {
		return nil, err
	}
	logger.Info("output", zap.Any("proto", dynamicLimits))
	s := fmt.Sprintf("%d", dynamicLimits.MaxRecentlyQueriedSeriesDiskBytesRead.Limit)
	return []byte(s), nil
}

func DoUpdate(endpoint string, limits int64, logger *zap.Logger) ([]byte, error) {
	if limits <= 0 {
		logger.Error("Can't set limits to be non positive", zap.Int64("limits", limits))
		return nil, nil
	}
	store, err := getEctdClient(endpoint)
	if err != nil {
		logger.Error("Can't access kv store", zap.Error(err))
		return nil, err
	}
	ql := kvpb.QueryLimit{
		Limit:           limits,
		LookbackSeconds: 15,
	}
	queryLimits := kvpb.QueryLimits{
		MaxRecentlyQueriedSeriesDiskBytesRead: &ql,
	}
	version, err := store.Set(kvconfig.QueryLimits, &queryLimits)
	if err != nil {
		logger.Warn("error set new query limit", zap.Error(err))
		return nil, err
	}
	s := fmt.Sprintf("new query limit version %d", version)
	return []byte(s), nil
}
