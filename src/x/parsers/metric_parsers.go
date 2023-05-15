package parsers

import (
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/serialize"
	"bytes"
)

// GetMetricIDWithoutLe returns a metric id (with sorted tag pairs) without the le tag
func GetMetricIDWithoutLe(metricID id.RawID) (id.RawID, bool) {
	it := serialize.NewUncheckedMetricTagsIterator(serialize.NewTagSerializationLimits())
	it.Reset(metricID)
	leTagName := []byte("le")
	if _, ok := it.TagValue(leTagName); !ok {
		// metricID does not have the le tag
		return metricID, false
	}

	var idWithoutLe []byte
	for it.Next() {
		tagName, tagValue := it.Current()

		if !bytes.Equal(tagName, leTagName) {
			idWithoutLe = append(idWithoutLe, tagName...)
			idWithoutLe = append(idWithoutLe, tagValue...)
		}
	}
	return idWithoutLe, true
}
