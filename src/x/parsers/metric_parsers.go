package parsers

import (
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	"bytes"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/checked"
)

func getMetricTagsIterator() serialize.MetricTagsIterator {
	// todo: creating a tag decoder in an ad-hoc manner, replace with our own decoder without the need of a pool
	poolOpts := pool.NewObjectPoolOptions().SetSize(1)
	size := 1
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
			CheckBytesWrapperPoolSize: &size,
		}), poolOpts)
	tagDecoderPool.Init()
	metricIteratorPool := serialize.NewMetricTagsIteratorPool(tagDecoderPool, poolOpts)
	metricIteratorPool.Init()

	return metricIteratorPool.Get()
}

func getNewID(tags []ident.Tag) (id.RawID, bool) {
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()

	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(ident.NewTagsIterator(ident.NewTags(tags...)))
	if err != nil {
		return nil, false
	}

	data, ok := tagEncoder.Data()
	if !ok {
		return nil, false
	}

	return data.Bytes(), true
}

// GetMetricIDWithoutLe returns a metric id (with sorted tag pairs) without the le tag
func GetMetricIDWithoutLe(metricID id.RawID) (id.RawID, bool) {
	it := getMetricTagsIterator()
	it.Reset(metricID)
	leTagName := []byte("le")
	if _, ok := it.TagValue(leTagName); !ok {
		// metricID does not have the le tag
		return metricID, false
	}

	var tagsWithoutLe []ident.Tag
	for it.Next() {
		tagName, tagValue := it.Current()

		if !bytes.Equal(tagName, leTagName) {
			tagsWithoutLe = append(tagsWithoutLe, ident.BinaryTag(checked.NewBytes(tagName, nil), checked.NewBytes(tagValue, nil)))
		}
	}
	idWithoutLe, ok := getNewID(tagsWithoutLe)
	if !ok {
		return metricID, false
	}
	return idWithoutLe, true
}
