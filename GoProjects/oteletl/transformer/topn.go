package transformer

// OtherBucket is the default value for non-top-N values.
const OtherBucket = "__other__"

// TopNFilter keeps top N values and buckets the rest.
type TopNFilter struct {
	topValues map[string]bool
	other     string
}

// NewTopNFilter creates a new TopNFilter.
func NewTopNFilter(topValues []string, other string) *TopNFilter {
	if other == "" {
		other = OtherBucket
	}
	m := make(map[string]bool, len(topValues))
	for _, v := range topValues {
		m[v] = true
	}
	return &TopNFilter{topValues: m, other: other}
}

// Filter returns the value if it's in the top-N set, otherwise the other bucket.
func (f *TopNFilter) Filter(value string) string {
	if f.topValues[value] {
		return value
	}
	return f.other
}
