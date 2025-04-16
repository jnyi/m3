// Copyright (c) 2017 Uber Technologies, Inc.
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

package filters

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/query/models"
)

const (
	// tagFilterListSeparator splits key:value pairs in a tag filter list.
	tagFilterListSeparator = " "
)

var (
	unknownFilterSeparator tagFilterSeparator

	// defaultFilterSeparator represents the default filter separator with no negation.
	defaultFilterSeparator = tagFilterSeparator{
		str: ":", negate: false,
	}
	databricksFilterSeparator = tagFilterSeparator{
		str: "#", negate: false,
	}

	// validFilterSeparators represent a list of valid filter separators.
	// NB: the separators here are tried in order during parsing.
	validFilterSeparators = []tagFilterSeparator{
		// The separators are sorted by priorities here. If one applies successfully, the ones below it are not considered.
		databricksFilterSeparator,
		defaultFilterSeparator,
	}
)

type tagFilterSeparator struct {
	str    string
	negate bool
}

type TagFilterValueMapKey struct {
	Name    string
	Exclude bool
}

func NewTagFilterValueMapKey(name string, exclude bool) TagFilterValueMapKey {
	return TagFilterValueMapKey{Name: name, Exclude: exclude}
}

// TagFilterValueMap is a map containing mappings from tag names to filter values.
type TagFilterValueMap map[TagFilterValueMapKey]FilterValue

// ParseTagFilterValueMap parses the input string and creates a tag filter value map.
func ParseTagFilterValueMap(str string) (TagFilterValueMap, error) {
	trimmed := strings.TrimSpace(str)
	tagPairs := strings.Split(trimmed, tagFilterListSeparator)
	res := make(map[TagFilterValueMapKey]FilterValue, len(tagPairs))
	for _, p := range tagPairs {
		sanitized := strings.TrimSpace(p)
		if sanitized == "" {
			continue
		}
		parts, separator, err := parseTagFilter(sanitized)
		if err != nil {
			return nil, err
		}
		// NB: we do not allow duplicate tags at the moment.
		exclude := parts[0][0] == negationChar
		name := parts[0]
		if exclude {
			name = parts[0][1:]
		}
		key := TagFilterValueMapKey{Name: name, Exclude: exclude}
		_, exists := res[key]
		if exists {
			return nil, fmt.Errorf("invalid filter %s: duplicate tag %s found", str, parts[0])
		}
		if key.Exclude {
			// For exclude rules, it doesn't make sense to have a value filter pattern since we are simply checking for the absence of the tag.
			// To avoid confusion, we enforce that the value filter pattern is a wildcard.
			if parts[1] != string(wildcardChar) {
				return nil, fmt.Errorf("invalid filter %s: negation only supported for wildcard patterns", str)
			}
		}
		res[key] = FilterValue{Pattern: parts[1], Negate: separator.negate}
	}
	return res, nil
}

func parseTagFilter(str string) ([]string, tagFilterSeparator, error) {
	parseByOneSeparator := func(separator tagFilterSeparator) ([]string, tagFilterSeparator, error) {
		items := strings.Split(str, separator.str)
		if len(items) == 2 {
			if items[0] == "" {
				return nil, unknownFilterSeparator, fmt.Errorf("invalid filter %s: empty tag name", str)
			}
			if items[1] == "" {
				return nil, unknownFilterSeparator, fmt.Errorf("invalid filter %s: empty filter pattern", str)
			}
			return items, separator, nil
		}
		return nil, unknownFilterSeparator, fmt.Errorf("invalid filter %s: expecting tag pattern pairs", str)
	}
	var returnedErr error
	// TODO(xichen): support negation of glob patterns.
	for _, separator := range validFilterSeparators {
		parts, _, err := parseByOneSeparator(separator)
		if err == nil {
			return parts, separator, nil
		} else {
			returnedErr = err
		}
	}
	return nil, unknownFilterSeparator, returnedErr
}

// tagFilter is a filter associated with a given tag.
type tagFilter struct {
	name        []byte
	valueFilter Filter
	// exclude indicates whether we want to check for the absence of the tag.
	exclude bool
}

func (f *tagFilter) String() string {
	excludePrefix := ""
	if f.exclude {
		excludePrefix = string(negationChar)
	}
	return fmt.Sprintf("%s%s:%s", excludePrefix, string(f.name), f.valueFilter.String())
}

type tagFiltersByNameAsc []tagFilter

func (tn tagFiltersByNameAsc) Len() int           { return len(tn) }
func (tn tagFiltersByNameAsc) Swap(i, j int)      { tn[i], tn[j] = tn[j], tn[i] }
func (tn tagFiltersByNameAsc) Less(i, j int) bool { return bytes.Compare(tn[i].name, tn[j].name) < 0 }

// TagsFilterOptions provide a set of tag filter options.
type TagsFilterOptions struct {
	// Name of the name tag.
	NameTagKey []byte

	// Function to extract name and tags from an id.
	NameAndTagsFn id.NameAndTagsFn

	// Function to create a new sorted tag iterator from id tags.
	SortedTagIteratorFn id.SortedTagIteratorFn
}

// tagsFilter contains a list of tag filters.
type tagsFilter struct {
	nameFilter Filter
	tagFilters []tagFilter
	op         LogicalOp
	opts       TagsFilterOptions
	// The value of the metric name tag filter
	nameFilterValue FilterValue
}

// NewTagsFilter creates a new tags filter.
func NewTagsFilter(
	filters TagFilterValueMap,
	op LogicalOp,
	opts TagsFilterOptions,
) (TagsFilter, error) {
	var (
		nameFilter      Filter
		tagFilters      = make([]tagFilter, 0, len(filters))
		nameFilterValue FilterValue
	)
	for entry, value := range filters {
		// We disallow OR support for exclude rules for simplicity.
		if entry.Exclude && op == Disjunction {
			return nil, fmt.Errorf("Invalid filter %s: exclude not supported for disjunction", entry.Name)
		}
		valFilter, err := NewFilterFromFilterValue(value)
		if err != nil {
			return nil, err
		}
		bName := []byte(entry.Name)
		if bytes.Equal(opts.NameTagKey, bName) {
			nameFilter = valFilter
			nameFilterValue = value
		} else {
			tagFilters = append(tagFilters, tagFilter{
				name:        bName,
				valueFilter: valFilter,
				exclude:     entry.Exclude,
			})
		}
	}
	sort.Sort(tagFiltersByNameAsc(tagFilters))
	return &tagsFilter{
		nameFilter:      nameFilter,
		tagFilters:      tagFilters,
		op:              op,
		opts:            opts,
		nameFilterValue: nameFilterValue,
	}, nil
}

func (f *tagsFilter) String() string {
	separator := " " + string(f.op) + " "
	var buf bytes.Buffer
	numTagFilters := len(f.tagFilters)
	if f.nameFilter != nil {
		buf.WriteString(fmt.Sprintf("%s:%s", f.opts.NameTagKey, f.nameFilter.String()))
		if numTagFilters > 0 {
			buf.WriteString(separator)
		}
	}
	for i := 0; i < numTagFilters; i++ {
		buf.WriteString(f.tagFilters[i].String())
		if i < numTagFilters-1 {
			buf.WriteString(separator)
		}
	}
	return buf.String()
}

func (f *tagsFilter) NameFilterValue() *FilterValue {
	if f.nameFilter == nil {
		return nil
	}
	return &f.nameFilterValue
}

func (f *tagsFilter) Matches(id []byte, opts TagMatchOptions) (bool, error) {
	if f.nameFilter == nil && len(f.tagFilters) == 0 {
		return true, nil
	}
	name, tags, err := opts.NameAndTagsFn(id)
	if err != nil {
		return false, err
	}
	return f.MatchesWithNameAndTags(name, tags, opts)
}

func (f *tagsFilter) MatchesWithNameAndTags(name, tags []byte, opts TagMatchOptions) (bool, error) {
	if f.nameFilter != nil {
		match := f.nameFilter.Matches(name)
		if match && f.op == Disjunction {
			return true, nil
		}
		if !match && f.op == Conjunction {
			return false, nil
		}
	}

	iter := opts.SortedTagIteratorFn(tags)

	currIdx := 0

	// Iterate over each of the metric's tags and rule's tag filters. They're both in sorted order.
	for iter.Next() && currIdx < len(f.tagFilters) {
		name, value := iter.Current()

		// Check if the current metric tag matches the current tag filter
		comparison := bytes.Compare(name, f.tagFilters[currIdx].name)
		if comparison == 0 && f.tagFilters[currIdx].exclude {
			// We have a tagFilter that is looking for the absence of a tag.
			return false, nil
		}

		// If the tags don't match, we move onto the next metric tag.
		// This is correct because not every one of the metric tag's need be represented in the rule.
		if comparison < 0 {
			continue
		}

		if comparison > 0 && f.tagFilters[currIdx].exclude {
			// We have a tagFilter that is looking for the absence of a tag.
			// Iterate through the remaining exclude tagFilters if any
			currIdx++
			for currIdx < len(f.tagFilters) && bytes.Compare(name, f.tagFilters[currIdx].name) > 0 && f.tagFilters[currIdx].exclude {
				currIdx++
			}

			if currIdx == len(f.tagFilters) {
				// We have reached the end after considering all exclude tag filters. Everything is good
				continue
			}
			// We have reached the first tagFilter that is not an exclude tag filter.
			newComparison := bytes.Compare(name, f.tagFilters[currIdx].name)
			if newComparison < 0 {
				// Not a problem
				continue
			} else if newComparison == 0 {
				// Time to do a value match. Pass through
			} else {
				// We have a tagFilter that is looking for the presence of a tag and it's missing
				return false, nil
			}
		} else if comparison > 0 {
			if f.op == Conjunction {
				// For AND, if the current filter tag doesn't exist, bail immediately.
				return false, nil
			}

			// Iterate tagFilters for the OR case.
			currIdx++
			for currIdx < len(f.tagFilters) && bytes.Compare(name, f.tagFilters[currIdx].name) > 0 {
				currIdx++
			}

			if currIdx == len(f.tagFilters) {
				// Past all tagFilters without covering all of the metric's tags
				return false, nil
			}

			if bytes.Compare(name, f.tagFilters[currIdx].name) < 0 {
				continue
			}
		}

		// Now check that the metric's underlying tag value passes the corresponding filter's value
		match := f.tagFilters[currIdx].valueFilter.Matches(value)
		if match && f.op == Disjunction {
			return true, nil
		}

		if !match && f.op == Conjunction {
			return false, nil
		}

		currIdx++
	}

	if iter.Err() != nil {
		for currIdx < len(f.tagFilters) && f.tagFilters[currIdx].exclude {
			currIdx++
		}
		if currIdx != len(f.tagFilters) {
			return false, iter.Err()
		}
	}

	if f.op == Disjunction {
		return false, nil
	}
	for currIdx < len(f.tagFilters) && f.tagFilters[currIdx].exclude {
		currIdx++
	}

	return currIdx == len(f.tagFilters), nil
}

func (f *tagsFilter) MatchTags(tags models.Tags) bool {
	if f.nameFilter == nil && len(f.tagFilters) == 0 {
		return true
	}
	curr := 0
	// Iterate over each of the metric's tags and rule's tag filters. They're both in sorted order.
	for i := 0; i < len(tags.Tags) && curr < len(f.tagFilters); i++ {
		tag := tags.Tags[i]
		comparison := bytes.Compare(tag.Name, f.tagFilters[curr].name)
		if comparison == 0 && f.tagFilters[curr].exclude {
			// We have a tagFilter that is looking for the absence of a tag.
			return false
		}
		if comparison < 0 {
			// If the tags don't match, we move onto the next metric tag.
			// This is correct because not every one of the metric tag's need be represented in the rule.
			continue
		}
		if comparison > 0 && !f.tagFilters[curr].exclude {
			if f.op == Conjunction {
				// For AND, if the current filter tag doesn't exist, bail immediately.
				return false
			}
			// Iterate tagFilters for the OR case, i-- to stay on the same tag.
			curr++
			i--
			continue
		}
		if comparison == 0 && f.tagFilters[curr].exclude {
			// We have a tagFilter that is looking for the absence of a tag.
			return false
		}
		match := f.tagFilters[curr].valueFilter.Matches(tag.Value)
		if match && f.op == Disjunction {
			return true
		}

		if !match && f.op == Conjunction {
			return false
		}
		curr++
	}
	for curr < len(f.tagFilters) && f.tagFilters[curr].exclude {
		curr++
	}

	return curr == len(f.tagFilters)
}

// ValidateTagsFilter validates whether a given string is a valid tags filter,
// returning the filter values if the string is a valid tags filter expression,
// and the error otherwise.
func ValidateTagsFilter(str string) (TagFilterValueMap, error) {
	filterValues, err := ParseTagFilterValueMap(str)
	if err != nil {
		return nil, errors.NewValidationError(fmt.Sprintf("tags filter %s is malformed: %v", str, err))
	}
	for entry, value := range filterValues {
		// Validating the filter value by actually constructing the filter.
		if _, err := NewFilterFromFilterValue(value); err != nil {
			return nil, errors.NewValidationError(fmt.Sprintf("tags filter %s contains invalid filter pattern %s for tag %s: %v", str, value.Pattern, entry.Name, err))
		}
	}
	return filterValues, nil
}
