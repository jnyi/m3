package parsers

import (
	"testing"
	"github.com/stretchr/testify/require"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/ident"
	"sort"
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
	tagsWithoutLe["foo"] = "bar"
	tagsWithoutLe["k1"] = "k2"
	tagsWithLe := copyTags(tagsWithoutLe)
	tagsWithLe["le"] = "0.0"
	idWithoutLe := newTestID(t, tagsWithoutLe)
	idWithLe := newTestID(t, tagsWithLe)

	type testCase struct {
		originalMetricID id.RawID
		expectedMetricID id.RawID
		expectedLeResult bool
		testName         string
	}
	testCases := []testCase{
		{
			testName:         "test original metric id without le returns the original metric id",
			originalMetricID: idWithoutLe.Bytes(),
			expectedMetricID: idWithoutLe.Bytes(),
			expectedLeResult: false,
		},
		{
			testName:         "test original metric id with le returns a modified id without le",
			originalMetricID: idWithLe.Bytes(),
			expectedMetricID: []byte("foobark1k2"),
			expectedLeResult: true,
		},
	}

	for _, testCase := range testCases {
		actualMetricID, actualLeResult := GetMetricIDWithoutLe(testCase.originalMetricID)
		require.Equal(t, testCase.expectedLeResult, actualLeResult, testCase.testName)
		require.Equal(t, testCase.expectedMetricID, actualMetricID, testCase.testName)
	}
}
