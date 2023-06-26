package parsers

import (
	"sort"
	"testing"

	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/stretchr/testify/require"
)

func newTestID(t *testing.T, tags map[string]string) id.ID {
	tagEncoderPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(),
		pool.NewObjectPoolOptions().SetSize(1))
	tagEncoderPool.Init()

	var stringTags []ident.Tag
	tagNames := make([]string, 0, len(tags))
	for name, _ := range tags {
		tagNames = append(tagNames, name)
	}
	sort.Strings(tagNames)
	for _, name := range tagNames {
		value := tags[name]
		stringTags = append(stringTags, ident.StringTag(name, value))
	}

	tagEncoder := tagEncoderPool.Get()
	err := tagEncoder.Encode(ident.NewTagsIterator(ident.NewTags(stringTags...)))
	require.NoError(t, err)

	data, ok := tagEncoder.Data()
	require.True(t, ok)

	size := 1
	tagDecoderPool := serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{
			CheckBytesWrapperPoolSize: &size,
		}),
		pool.NewObjectPoolOptions().SetSize(size))
	tagDecoderPool.Init()

	tagDecoder := tagDecoderPool.Get()

	iter := serialize.NewMetricTagsIterator(tagDecoder, nil)
	iter.Reset(data.Bytes())
	return iter
}

func copyTags(tags map[string]string) map[string]string {
	copy := make(map[string]string)
	for k, v := range tags {
		copy[k] = v
	}
	return copy
}
func TestGetMetricIDWithoutLe(t *testing.T) {
	tagsWithoutLe := make(map[string]string)
	tagsWithoutLe["__name__"] = "metric"
	tagsWithoutLe["foo"] = "bar"
	tagsWithoutLe["k1"] = "k2"
	tagsWithLe := copyTags(tagsWithoutLe)
	tagsWithLe["le"] = "0.0"
	idWithoutLe := newTestID(t, tagsWithoutLe)
	// idWithLe := newTestID(t, tagsWithLe)
	// idWithBucketSuffix := newTestID(t, map[string]string{"__name__": "metric_bucket"})
	// idWithSumSuffix := newTestID(t, map[string]string{"__name__": "metric_sum"})
	// idWithCountSuffix := newTestID(t, map[string]string{"__name__": "metric_count"})
	// idWithCountSuffixWithOtherLabel := newTestID(t, map[string]string{"FOO": "foo", "__name__": "metric_count"})

	type testCase struct {
		originalMetricID    id.RawID
		expectedMetricID    id.RawID
		expectedIsHistogram bool
		testName            string
	}
	testCases := []testCase{
		{
			testName:            "test original metric id without le returns the original metric id",
			originalMetricID:    idWithoutLe.Bytes(),
			expectedMetricID:    idWithoutLe.Bytes(),
			expectedIsHistogram: false,
		},
		// {
		// 	testName:            "test original metric id with le returns a modified id without le",
		// 	originalMetricID:    idWithLe.Bytes(),
		// 	expectedMetricID:    []byte("foobark1k2"),
		// 	expectedIsHistogram: true,
		// },
		// {
		// 	testName:            "test original metric id with _bucket suffix returns a modified id where __name__ does not have _bucket suffix",
		// 	originalMetricID:    idWithBucketSuffix.Bytes(),
		// 	expectedMetricID:    []byte("__name__metric"),
		// 	expectedIsHistogram: true,
		// },
		// {
		// 	testName:            "test original metric id with _sum suffix returns a modified id where __name__ does not have _sum suffix",
		// 	originalMetricID:    idWithSumSuffix.Bytes(),
		// 	expectedMetricID:    []byte("__name__metric"),
		// 	expectedIsHistogram: true,
		// },
		// {
		// 	testName:            "test original metric id with _count suffix returns a modified id where __name__ does not have _count suffix",
		// 	originalMetricID:    idWithCountSuffix.Bytes(),
		// 	expectedMetricID:    []byte("__name__metric"),
		// 	expectedIsHistogram: true,
		// },
		// {
		// 	testName:            "test original metric id with _count suffix and another label returns a modified id where __name__ does not have _count suffix",
		// 	originalMetricID:    idWithCountSuffixWithOtherLabel.Bytes(),
		// 	expectedMetricID:    []byte("FOOfoo__name__metric"),
		// 	expectedIsHistogram: true,
		// },
	}

	for _, testCase := range testCases {
		actualMetricID, actualLeResult := GetMetricIDForHistogramAgg(testCase.originalMetricID)
		require.Equal(t, testCase.expectedIsHistogram, actualLeResult, testCase.testName)
		require.Equal(t, testCase.expectedMetricID, actualMetricID, testCase.testName)
	}
}
