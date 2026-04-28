package prometheus

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	"github.com/symetryml/godf"
)

// FetchMetricsRangeDF slices and resamples an in-memory metrics DataFrame.
//
// This is the Go equivalent of the Python fetch_metrics_range_df function.
// For one-shot use this is fine. For looping over many windows of the same
// DataFrame, use IterMetricsWindows instead.
//
// df must have columns: timestamp (String or DateTime), metric (String),
// labels (Any), value (Float64).
func FetchMetricsRangeDF(
	df *godf.DataFrame,
	metricNames []string,
	start, end *time.Time,
	step string,
) *godf.DataFrame {
	if df.Empty() {
		return godf.NewDataFrame(nil)
	}

	// Filter by metric names if provided
	source := df
	if len(metricNames) > 0 {
		nameSet := make(map[string]bool, len(metricNames))
		for _, n := range metricNames {
			nameSet[n] = true
		}
		metricCol := source.Col("metric")
		mask := make([]bool, source.NRows())
		for i := 0; i < source.NRows(); i++ {
			mask[i] = nameSet[metricCol.Str(i)]
		}
		source = source.Filter(mask)
		if source.Empty() {
			return godf.NewDataFrame(nil)
		}
	}

	// Prepare: parse timestamps, sort, floor, build series keys
	prepared := prepareMetricsDF(source, step)

	// Filter by time range
	if start != nil || end != nil {
		prepared = filterByTimeRange(prepared, start, end)
		if prepared.Empty() {
			return godf.NewDataFrame(nil)
		}
	}

	return dedupLast(prepared)
}

// MetricsWindow represents a windowed slice of metrics data.
type MetricsWindow struct {
	Start time.Time
	End   time.Time
	DF    *godf.DataFrame
}

// IterMetricsWindows yields (start, end, windowDF) over the full time span.
//
// Designed for large DataFrames: pre-processes once, partitions via binary
// search on sorted timestamps.
func IterMetricsWindows(
	df *godf.DataFrame,
	metricNames []string,
	windowMinutes int,
	step string,
) []MetricsWindow {
	if df.Empty() {
		return nil
	}

	source := df
	if len(metricNames) > 0 {
		nameSet := make(map[string]bool, len(metricNames))
		for _, n := range metricNames {
			nameSet[n] = true
		}
		metricCol := source.Col("metric")
		mask := make([]bool, source.NRows())
		for i := 0; i < source.NRows(); i++ {
			mask[i] = nameSet[metricCol.Str(i)]
		}
		source = source.Filter(mask)
		if source.Empty() {
			return nil
		}
	}

	prepared := prepareMetricsDF(source, step)
	tsSeries := prepared.Col("timestamp")
	nRows := prepared.NRows()

	// Get sorted timestamps as time.Time slice for binary search
	timestamps := make([]time.Time, nRows)
	for i := 0; i < nRows; i++ {
		timestamps[i] = tsSeries.Time(i)
	}

	totalStart := timestamps[0]
	totalEnd := timestamps[nRows-1]
	delta := time.Duration(windowMinutes) * time.Minute

	var windows []MetricsWindow
	current := totalStart

	for !current.After(totalEnd) {
		windowEnd := current.Add(delta)

		// Binary search for lo and hi
		lo := sort.Search(nRows, func(i int) bool {
			return !timestamps[i].Before(current)
		})
		hi := sort.Search(nRows, func(i int) bool {
			return !timestamps[i].Before(windowEnd)
		})

		if lo < hi {
			indices := make([]int, hi-lo)
			for j := range indices {
				indices[j] = lo + j
			}
			windowSlice := prepared.Take(indices)
			windows = append(windows, MetricsWindow{
				Start: current,
				End:   windowEnd,
				DF:    dedupLast(windowSlice),
			})
		}

		current = windowEnd
	}

	return windows
}

// --- Internal helpers ---

var reStep = regexp.MustCompile(`^(\d+)([smhd])$`)

func stepToDuration(step string) time.Duration {
	m := reStep.FindStringSubmatch(step)
	if m == nil {
		return time.Minute
	}
	val, _ := strconv.Atoi(m[1])
	switch m[2] {
	case "s":
		return time.Duration(val) * time.Second
	case "m":
		return time.Duration(val) * time.Minute
	case "h":
		return time.Duration(val) * time.Hour
	case "d":
		return time.Duration(val) * 24 * time.Hour
	}
	return time.Minute
}

func floorTime(t time.Time, d time.Duration) time.Time {
	return t.Truncate(d)
}

// prepareMetricsDF sorts, floors timestamps, and adds a series key column.
func prepareMetricsDF(df *godf.DataFrame, step string) *godf.DataFrame {
	nRows := df.NRows()
	dur := stepToDuration(step)

	// Parse/floor timestamps
	tsSeries := df.Col("timestamp")
	metricCol := df.Col("metric")
	labelCol := df.Col("labels")

	flooredTimes := make([]time.Time, nRows)
	seriesKeys := make([]string, nRows)
	nulls := make([]bool, nRows)

	for i := 0; i < nRows; i++ {
		var t time.Time
		if tsSeries.Dtype() == godf.DateTime {
			t = tsSeries.Time(i)
		} else {
			str := fmt.Sprintf("%v", tsSeries.Any(i))
			parsed, err := time.Parse(time.RFC3339, str)
			if err != nil {
				parsed, _ = time.Parse("2006-01-02T15:04:05", str)
			}
			t = parsed
		}
		flooredTimes[i] = floorTime(t, dur)

		// Build series key: metric||labels_string
		m := metricCol.Str(i)
		l := fmt.Sprintf("%v", labelCol.Any(i))
		seriesKeys[i] = m + "||" + l
	}

	result := df.Copy()
	result.SetCol("timestamp", godf.NewDateTimeSeries("timestamp", flooredTimes, nulls))
	result.SetCol("_sk", godf.NewStringSeries("_sk", seriesKeys...))

	// Sort by timestamp
	result = result.SortBy("timestamp")
	return result
}

func filterByTimeRange(df *godf.DataFrame, start, end *time.Time) *godf.DataFrame {
	tsSeries := df.Col("timestamp")
	mask := make([]bool, df.NRows())

	for i := 0; i < df.NRows(); i++ {
		t := tsSeries.Time(i)
		if start != nil && t.Before(*start) {
			continue
		}
		if end != nil && t.After(*end) {
			continue
		}
		mask[i] = true
	}

	return df.Filter(mask)
}

// dedupLast keeps the last sample per (timestamp, series_key) bucket.
func dedupLast(df *godf.DataFrame) *godf.DataFrame {
	if df.Empty() {
		return godf.NewDataFrame(nil)
	}

	tsSeries := df.Col("timestamp")
	skSeries := df.Col("_sk")
	nRows := df.NRows()

	// Walk backwards, keeping first seen per (ts, sk) key
	seen := make(map[string]bool)
	keepIndices := make([]int, 0, nRows)

	for i := nRows - 1; i >= 0; i-- {
		key := fmt.Sprintf("%v||%s", tsSeries.Any(i), skSeries.Str(i))
		if !seen[key] {
			seen[key] = true
			keepIndices = append(keepIndices, i)
		}
	}

	// Reverse to maintain original order
	for i, j := 0, len(keepIndices)-1; i < j; i, j = i+1, j-1 {
		keepIndices[i], keepIndices[j] = keepIndices[j], keepIndices[i]
	}

	result := df.Take(keepIndices)

	// Drop the _sk helper column and return only standard columns
	result.DropCol("_sk")
	return result
}

// GetMetricsDataframe2DF builds a metric catalog from an in-memory DataFrame.
//
// Drop-in replacement for Client.GetMetricsDataframe2 when data is already
// loaded (e.g. from CSV). Returns unique (service, metric) pairs with type info.
//
// Input df must have columns: timestamp, metric, labels (map[string]string), value.
func GetMetricsDataframe2DF(df *godf.DataFrame) []MetricInfo {
	if df.Empty() {
		return nil
	}

	metricCol := df.Col("metric")
	labelCol := df.Col("labels")
	nRows := df.NRows()

	seen := make(map[string]bool)
	var result []MetricInfo

	for i := 0; i < nRows; i++ {
		metric := metricCol.Str(i)
		labels, _ := labelCol.Any(i).(map[string]string)
		if labels == nil {
			labels = map[string]string{}
		}

		service := firstOf(labels, "service_name", "service", "job", "container")
		if service == "" {
			service = "unknown"
		}

		key := service + "|" + metric
		if seen[key] {
			continue
		}
		seen[key] = true

		mType, mSubtype := detectMetricType(metric, labels)
		result = append(result, MetricInfo{
			Service: service,
			Metric:  metric,
			Type:    mType,
			Subtype: mSubtype,
		})
	}

	// Sort by service, then metric
	sort.Slice(result, func(i, j int) bool {
		if result[i].Service != result[j].Service {
			return result[i].Service < result[j].Service
		}
		return result[i].Metric < result[j].Metric
	})

	return result
}
