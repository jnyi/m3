package parsers

import (
	"bytes"

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/serialize"
)

// GetMetricIDForHistogramAgg returns a metric id (with sorted tag pairs) without the le tag (if is a histogram) and
// strips histogram suffixes ("_bucket", "_sum". "_count")
func GetMetricIDForHistogramAgg(metricID id.RawID) (id.RawID, bool) {
	it := serialize.NewUncheckedMetricTagsIterator(serialize.NewTagSerializationLimits())
	it.Reset(metricID)
	leTagName := []byte("le")
	nameTagName := []byte("__name__")

	bucketSuffix := []byte("_bucket")
	countSuffix := []byte("_count")
	sumSuffix := []byte("_sum")

	isHistogram := false

	var idForHistogramAgg []byte
	for it.Next() {
		tagName, tagValue := it.Current()

		if bytes.Equal(tagName, nameTagName) {
			// if the __name__ contains a histogram suffix, then we strip the suffix
			isHistogram = true
			switch {
			case bytes.HasSuffix(tagValue, bucketSuffix):
				tagValue = tagValue[:len(tagValue)-len(bucketSuffix)]
			case bytes.HasSuffix(tagValue, countSuffix):
				tagValue = tagValue[:len(tagValue)-len(countSuffix)]
			case bytes.HasSuffix(tagValue, sumSuffix):
				tagValue = tagValue[:len(tagValue)-len(sumSuffix)]
			default:
				isHistogram = false
			}

			if !isHistogram {
				// if the __name__ does not contain a histogram suffix, then return original metricID
				// since the metric is not a histogram
				return metricID, false
			}
		} else if bytes.Equal(tagName, leTagName) {
			isHistogram = true
			continue
		}

		idForHistogramAgg = append(idForHistogramAgg, tagName...)
		idForHistogramAgg = append(idForHistogramAgg, tagValue...)
	}
	return idForHistogramAgg, isHistogram
}
