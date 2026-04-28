// Package pipeline implements the denormalize_metrics pipeline in Go.
//
// It transforms raw OTel metrics (long-format) into ML-ready wide-format
// DataFrames using the godf library.
package pipeline

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/symetryml/godf"
	"github.com/symetryml/oteletl/aggregator"
	"github.com/symetryml/oteletl/classifier"
	"github.com/symetryml/oteletl/transformer"
)

// Config holds pipeline configuration.
type Config struct {
	// WindowSeconds is the aggregation window in seconds (default: 60).
	WindowSeconds float64
	// IncludeDeltas controls whether delta features are computed (default: true).
	IncludeDeltas bool
	// DeltaWindows lists the delta window sizes in row-shift units (default: [5, 60]).
	DeltaWindows []int
	// PctChangeWindows lists the pct-change window sizes (default: [60]).
	PctChangeWindows []int
	// EntityLabels overrides which labels form the entity key.
	EntityLabels []string
	// UniqueTimestamps if true pivots only by timestamp (entity in column names).
	UniqueTimestamps bool
	// SchemaConfig holds per-metric label overrides (optional).
	// Can be loaded from YAML with LoadSchemaFile() or generated with RunProfilerFromDataFrame().
	SchemaConfig map[string]MetricSchema
	// SchemaPath is a path to a schema YAML file. If set and SchemaConfig is nil,
	// the schema is loaded from this file automatically.
	SchemaPath string
	// ForceDropLabels are labels to always drop.
	ForceDropLabels []string
	// CountersWanted controls which counter aggregations to keep.
	// Possible values: "rate", "count". Default: ["count"].
	// Set to nil/empty to skip counters entirely.
	CountersWanted []string
	// GaugeWanted controls which gauge aggregations to keep.
	// Possible values: "last", "mean", "min", "max", "stddev". Default: ["mean"].
	// Set to nil/empty to skip gauges entirely.
	GaugeWanted []string
}

// MetricSchema holds per-metric label configuration.
type MetricSchema struct {
	Labels map[string]LabelSchema
}

// LabelSchema holds per-label configuration.
type LabelSchema struct {
	Action     string   // "keep", "drop", "bucket", "top_n"
	BucketType string   // "status_code", "http_method", "operation", "route"
	TopValues  []string // Values to keep for top_n action
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		WindowSeconds:    60,
		IncludeDeltas:    true,
		DeltaWindows:     []int{5, 60},
		PctChangeWindows: []int{60},
		CountersWanted:   []string{"count"},
		GaugeWanted:      []string{"mean"},
	}
}

// CoreStatusBuckets are the status variants that must always have columns.
var CoreStatusBuckets = []string{"success", "client_error", "server_error"}

// DenormalizeMetrics transforms raw metrics into ML-ready wide-format DataFrame.
//
// Input DataFrame must have columns: timestamp (DateTime or String), metric (String),
// labels (Any - map[string]string), value (Float64).
func DenormalizeMetrics(rawDF *godf.DataFrame, cfg Config) *godf.DataFrame {
	if rawDF.Empty() {
		return godf.NewDataFrame(nil)
	}

	// Auto-load schema from file if path is set and SchemaConfig is nil
	if cfg.SchemaConfig == nil && cfg.SchemaPath != "" {
		schema, err := LoadSchemaFile(cfg.SchemaPath)
		if err == nil {
			cfg.SchemaConfig = schema
		}
	}

	if cfg.WindowSeconds == 0 {
		cfg.WindowSeconds = 60
	}
	if cfg.DeltaWindows == nil {
		cfg.DeltaWindows = []int{5, 60}
	}
	if cfg.PctChangeWindows == nil {
		cfg.PctChangeWindows = []int{60}
	}
	if cfg.CountersWanted == nil {
		cfg.CountersWanted = []string{"count"}
	}
	if cfg.GaugeWanted == nil {
		cfg.GaugeWanted = []string{"mean"}
	}

	// Stage 1: Apply transformations (label bucketing, signal key extraction)
	transformed := applyTransformations(rawDF, cfg)

	// Stage 2: Add entity key column
	transformed = addEntityKeyColumn(transformed, cfg.EntityLabels)

	// Stage 3: Aggregate metrics by type
	aggregated := aggregateMetrics(transformed, cfg.WindowSeconds, cfg.CountersWanted, cfg.GaugeWanted)

	// Stage 4: Generate feature names
	featured := generateFeatures(aggregated, cfg.UniqueTimestamps)

	// Stage 5: Pivot to wide format
	wide := pivotToWide(featured, cfg.UniqueTimestamps)

	// Stage 6: Ensure status columns exist
	wide = ensureStatusColumns(wide)

	// Stage 7: Compute delta features
	if cfg.IncludeDeltas {
		entityCol := "entity_key"
		if cfg.UniqueTimestamps {
			entityCol = ""
		}
		wide = computeDeltas(wide, entityCol, cfg.DeltaWindows, cfg.PctChangeWindows)
	}

	return wide
}

// --- Stage 1: Apply Transformations ---

func applyTransformations(df *godf.DataFrame, cfg Config) *godf.DataFrame {
	nRows := df.NRows()
	metricCol := df.Col("metric")
	labelCol := df.Col("labels")

	newLabels := make([]any, nRows)
	signalKeys := make([]string, nRows)

	for i := 0; i < nRows; i++ {
		rawLabels := labelCol.Any(i)
		labels, ok := rawLabels.(map[string]string)
		if !ok {
			labels = make(map[string]string)
		}

		metric := metricCol.Str(i)
		metricFamily := classifier.ExtractMetricFamily(metric)

		transformed := transformLabels(labels, metricFamily, cfg)
		newLabels[i] = transformed
		signalKeys[i] = extractSignalKey(transformed)
	}

	result := df.Copy()
	result.SetCol("labels", godf.NewAnySeries("labels", newLabels))
	result.SetCol("signal_key", godf.NewStringSeries("signal_key", signalKeys...))
	return result
}

func transformLabels(labels map[string]string, metricFamily string, cfg Config) map[string]string {
	transformed := make(map[string]string)

	// Build force-drop set
	dropSet := make(map[string]bool, len(cfg.ForceDropLabels))
	for _, l := range cfg.ForceDropLabels {
		dropSet[l] = true
	}

	// Never drop histogram internal labels — aggregators need them
	histogramInternals := map[string]bool{"le": true, "quantile": true}

	for label, value := range labels {
		if dropSet[label] && !histogramInternals[label] {
			continue
		}

		action := "keep"
		bucketType := ""

		// Check schema config first
		if cfg.SchemaConfig != nil {
			if ms, ok := cfg.SchemaConfig[metricFamily]; ok {
				if ls, ok := ms.Labels[label]; ok {
					action = ls.Action
					bucketType = ls.BucketType

					if action == "drop" && !histogramInternals[label] {
						continue
					}
					if action == "top_n" && len(ls.TopValues) > 0 {
						f := transformer.NewTopNFilter(ls.TopValues, "")
						transformed[label] = f.Filter(value)
						continue
					}
				}
			}
		}

		// Fall back to semantic classification
		if bucketType == "" {
			c := classifier.ClassifyLabel(label)
			if c.Category == classifier.Correlation {
				continue
			}
			if c.Handling == "drop" {
				continue
			}
			if c.Category == classifier.Signal {
				bucketType = c.BucketType
			}
		}

		// Apply bucketing
		switch bucketType {
		case "status_code":
			transformed[label] = string(transformer.BucketStatusCode(value, label))
		case "http_method":
			transformed[label] = string(transformer.BucketHTTPMethod(value))
		case "operation":
			transformed[label] = string(transformer.BucketOperation(value))
		case "route":
			transformed[label] = transformer.ParameterizeRoute(value)
		default:
			if action != "drop" || histogramInternals[label] {
				transformed[label] = value
			}
		}
	}

	return transformed
}

func extractSignalKey(labels map[string]string) string {
	var parts []string
	// Sort label names for deterministic output
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		c := classifier.ClassifyLabel(k)
		if c.Category == classifier.Signal && c.BucketType != "" {
			parts = append(parts, labels[k])
		}
	}

	return strings.Join(parts, "__")
}

// --- Stage 2: Add Entity Key ---

func addEntityKeyColumn(df *godf.DataFrame, entityLabels []string) *godf.DataFrame {
	labelCol := df.Col("labels")
	nRows := df.NRows()
	keys := make([]string, nRows)

	for i := 0; i < nRows; i++ {
		rawLabels := labelCol.Any(i)
		labels, ok := rawLabels.(map[string]string)
		if !ok {
			keys[i] = "default"
			continue
		}
		keys[i] = computeEntityKey(labels, entityLabels)
	}

	result := df.Copy()
	result.SetCol("entity_key", godf.NewStringSeries("entity_key", keys...))
	return result
}

func computeEntityKey(labels map[string]string, entityLabels []string) string {
	if entityLabels == nil {
		// Auto-detect entity labels
		entityLabels = make([]string, 0)
		for k := range labels {
			if classifier.IsEntityLabel(k) {
				entityLabels = append(entityLabels, k)
			}
		}
	}

	if len(entityLabels) == 0 {
		defaults := []string{"service_name", "service", "job", "app", "instance"}
		for _, d := range defaults {
			if _, ok := labels[d]; ok {
				entityLabels = append(entityLabels, d)
			}
		}
	}

	sort.Strings(entityLabels)
	var parts []string
	for _, l := range entityLabels {
		if v, ok := labels[l]; ok {
			parts = append(parts, fmt.Sprintf("%s=%s", l, v))
		}
	}

	if len(parts) == 0 {
		return "default"
	}
	return strings.Join(parts, "::")
}

// --- Stage 3: Aggregate Metrics ---

type aggGroupKey struct {
	timestamp  string
	entityKey  string
	family     string
	signalKey  string
}

type rowInfo struct {
	timestamp  string
	entityKey  string
	family     string
	signalKey  string
	metricName string
	metricType string
	value      float64
	labels     map[string]string
	tsTime     time.Time
}

func aggregateMetrics(df *godf.DataFrame, windowSeconds float64, countersWanted, gaugeWanted []string) *godf.DataFrame {
	nRows := df.NRows()
	metricCol := df.Col("metric")
	valueCol := df.Col("value")
	labelCol := df.Col("labels")
	entityCol := df.Col("entity_key")
	signalCol := df.Col("signal_key")

	// Get timestamp as strings for grouping
	tsSeries := df.Col("timestamp")

	rows := make([]rowInfo, nRows)
	for i := 0; i < nRows; i++ {
		metric := metricCol.Str(i)
		var tsStr string
		if tsSeries.Dtype() == godf.DateTime {
			tsStr = tsSeries.Time(i).Format(time.RFC3339)
		} else {
			tsStr = fmt.Sprintf("%v", tsSeries.Any(i))
		}

		labels := make(map[string]string)
		if l, ok := labelCol.Any(i).(map[string]string); ok {
			labels = l
		}

		var tsTime time.Time
		if tsSeries.Dtype() == godf.DateTime {
			tsTime = tsSeries.Time(i)
		}

		rows[i] = rowInfo{
			timestamp:  tsStr,
			entityKey:  entityCol.Str(i),
			family:     classifier.ExtractMetricFamily(metric),
			signalKey:  signalCol.Str(i),
			metricName: metric,
			metricType: classifier.ClassifyMetricType(metric),
			value:      valueCol.Float(i),
			labels:     labels,
			tsTime:     tsTime,
		}
	}

	// Group by (timestamp, entity_key, metric_family, signal_key)
	type groupData struct {
		key  aggGroupKey
		rows []rowInfo
	}

	groupOrder := make([]string, 0)
	groups := make(map[string]*groupData)

	for _, r := range rows {
		k := aggGroupKey{r.timestamp, r.entityKey, r.family, r.signalKey}
		keyStr := fmt.Sprintf("%s|%s|%s|%s", k.timestamp, k.entityKey, k.family, k.signalKey)
		if _, ok := groups[keyStr]; !ok {
			groups[keyStr] = &groupData{key: k}
			groupOrder = append(groupOrder, keyStr)
		}
		groups[keyStr].rows = append(groups[keyStr].rows, r)
	}

	// Aggregate each group
	var results []map[string]any

	for _, keyStr := range groupOrder {
		gd := groups[keyStr]
		k := gd.key

		// Determine metric types in this group
		hasHistogram := false
		hasCounter := false
		for _, r := range gd.rows {
			if r.metricType == "histogram" || r.metricType == "histogram_component" {
				hasHistogram = true
			}
			if r.metricType == "counter" {
				hasCounter = true
			}
		}

		var aggResults map[string]float64
		if hasHistogram {
			aggResults = aggregateHistogramGroup(gd.rows)
		} else if hasCounter {
			if len(countersWanted) == 0 {
				continue
			}
			aggResults = aggregateCounterGroup(gd.rows, windowSeconds)
			aggResults = filterWanted(aggResults, countersWanted)
		} else {
			if len(gaugeWanted) == 0 {
				continue
			}
			aggResults = aggregateGaugeGroup(gd.rows)
			aggResults = filterWanted(aggResults, gaugeWanted)
		}

		if len(aggResults) == 0 {
			continue
		}

		// Get labels from first row (excluding le, quantile)
		var firstLabels map[string]string
		if len(gd.rows) > 0 {
			firstLabels = make(map[string]string)
			for lk, lv := range gd.rows[0].labels {
				if lk != "le" && lk != "quantile" {
					firstLabels[lk] = lv
				}
			}
		}

		for aggName, aggValue := range aggResults {
			results = append(results, map[string]any{
				"timestamp":     k.timestamp,
				"entity_key":    k.entityKey,
				"metric_family": k.family,
				"signal_key":    k.signalKey,
				"aggregation":   aggName,
				"value":         aggValue,
			})
		}
	}

	return godf.NewDataFrame(results)
}

// aggregateHistogramGroup aggregates histogram bucket data using percentile estimation.
// Matches Python _aggregate_histogram_group.
func aggregateHistogramGroup(rows []rowInfo) map[string]float64 {
	var leValues []string
	var countValues []float64
	var sumValue, countValue float64

	for _, r := range rows {
		switch {
		case strings.HasSuffix(r.metricName, "_bucket"):
			le, ok := r.labels["le"]
			if !ok {
				continue
			}
			leValues = append(leValues, le)
			countValues = append(countValues, r.value)
		case strings.HasSuffix(r.metricName, "_sum"):
			sumValue += r.value
		case strings.HasSuffix(r.metricName, "_count"):
			countValue += r.value
		}
	}

	if len(leValues) == 0 {
		return nil
	}

	h := aggregator.AggregateHistogram(leValues, countValues, sumValue, countValue)
	return map[string]float64{
		"p50":   h.P50,
		"p75":   h.P75,
		"p90":   h.P90,
		"p95":   h.P95,
		"p99":   h.P99,
		"mean":  h.Mean,
		"count": h.Count,
		"sum":   h.Sum,
	}
}

// aggregateHistogramAsCounters treats histogram _count and _sum as counters,
// ignoring _bucket rows. Percentile estimation from merged buckets across
// multiple instances is not meaningful.
//
// Multiple instances (routes, methods) are summed per timestamp before
// computing counter deltas.
func aggregateHistogramAsCounters(rows []rowInfo, windowSeconds float64) map[string]float64 {
	countByTS := make(map[time.Time]float64)
	sumByTS := make(map[time.Time]float64)

	for _, r := range rows {
		switch {
		case strings.HasSuffix(r.metricName, "_count"):
			countByTS[r.tsTime] += r.value
		case strings.HasSuffix(r.metricName, "_sum"):
			sumByTS[r.tsTime] += r.value
		}
	}

	result := make(map[string]float64)

	if len(countByTS) >= 2 {
		vals, ts := mapToSlices(countByTS)
		cr := aggregator.AggregateCounter(vals, ts, windowSeconds)
		result["rate"] = cr.RatePerSec
		result["count"] = cr.Count
	} else if len(countByTS) == 1 {
		result["rate"] = 0
		result["count"] = 0
	}

	if len(sumByTS) >= 2 {
		vals, ts := mapToSlices(sumByTS)
		sr := aggregator.AggregateCounter(vals, ts, windowSeconds)
		result["sum_rate"] = sr.RatePerSec
		result["sum"] = sr.Count
	} else if len(sumByTS) == 1 {
		result["sum_rate"] = 0
		result["sum"] = 0
	}

	if rate, ok := result["rate"]; ok && rate > 0 {
		if sumRate, ok := result["sum_rate"]; ok {
			result["mean"] = sumRate / rate
		}
	}

	return result
}

func filterWanted(results map[string]float64, wanted []string) map[string]float64 {
	filtered := make(map[string]float64, len(wanted))
	for _, k := range wanted {
		if v, ok := results[k]; ok {
			filtered[k] = v
		}
	}
	return filtered
}

func mapToSlices(m map[time.Time]float64) ([]float64, []time.Time) {
	vals := make([]float64, 0, len(m))
	ts := make([]time.Time, 0, len(m))
	for t, v := range m {
		ts = append(ts, t)
		vals = append(vals, v)
	}
	return vals, ts
}

func aggregateCounterGroup(rows []rowInfo, windowSeconds float64) map[string]float64 {
	// Sum _total values per timestamp across instances (error_type, etc.)
	byTS := make(map[time.Time]float64)
	for _, r := range rows {
		if strings.HasSuffix(r.metricName, "_total") {
			byTS[r.tsTime] += r.value
		}
	}

	if len(byTS) == 0 {
		return nil
	}

	if len(byTS) < 2 {
		return map[string]float64{"rate": 0, "count": 0}
	}

	vals, ts := mapToSlices(byTS)
	result := aggregator.AggregateCounter(vals, ts, windowSeconds)
	return map[string]float64{
		"rate":  result.RatePerSec,
		"count": result.Count,
	}
}

func aggregateGaugeGroup(rows []rowInfo) map[string]float64 {
	if len(rows) == 0 {
		return nil
	}

	// Average values per timestamp across instances
	sumByTS := make(map[time.Time]float64)
	countByTS := make(map[time.Time]int)
	for _, r := range rows {
		sumByTS[r.tsTime] += r.value
		countByTS[r.tsTime]++
	}

	values := make([]float64, 0, len(sumByTS))
	timestamps := make([]time.Time, 0, len(sumByTS))
	for t, s := range sumByTS {
		timestamps = append(timestamps, t)
		values = append(values, s/float64(countByTS[t]))
	}

	result := aggregator.AggregateGauge(values, timestamps)
	return map[string]float64{
		"last":   result.Last,
		"mean":   result.Mean,
		"min":    result.Min,
		"max":    result.Max,
		"stddev": result.Stddev,
	}
}

// --- Stage 4: Generate Feature Names ---

func generateFeatures(df *godf.DataFrame, uniqueTimestamps bool) *godf.DataFrame {
	if df.Empty() {
		return df
	}

	nRows := df.NRows()
	familyCol := df.Col("metric_family")
	aggCol := df.Col("aggregation")
	signalCol := df.Col("signal_key")

	features := make([]string, nRows)
	for i := 0; i < nRows; i++ {
		base := familyCol.Str(i) + "__" + aggCol.Str(i)

		sk := signalCol.Str(i)
		if sk != "" {
			base = base + "__" + sk
		}

		features[i] = base
	}

	result := df.Copy()
	result.SetCol("feature", godf.NewStringSeries("feature", features...))
	return result
}

// --- Stage 5: Pivot to Wide ---

func pivotToWide(df *godf.DataFrame, uniqueTimestamps bool) *godf.DataFrame {
	if df.Empty() {
		return godf.NewDataFrame(nil)
	}

	indexCols := []string{"timestamp", "entity_key"}
	if uniqueTimestamps {
		indexCols = []string{"timestamp"}
	}

	return df.PivotTable(indexCols, "feature", "value", "first")
}

// --- Stage 6: Ensure Status Columns ---

func ensureStatusColumns(df *godf.DataFrame) *godf.DataFrame {
	if df.Empty() {
		return df
	}

	cols := df.Columns()
	existing := make(map[string]bool, len(cols))
	for _, c := range cols {
		existing[c] = true
	}

	var newCols []string
	for _, col := range cols {
		for _, bucket := range CoreStatusBuckets {
			suffix := "__" + bucket
			if strings.HasSuffix(col, suffix) {
				base := col[:len(col)-len(suffix)]
				for _, other := range CoreStatusBuckets {
					variant := base + "__" + other
					if !existing[variant] {
						existing[variant] = true
						newCols = append(newCols, variant)
					}
				}
				break
			}
		}
	}

	if len(newCols) == 0 {
		return df
	}

	// Add NaN columns
	nRows := df.NRows()
	nanVals := make([]float64, nRows)
	for i := range nanVals {
		nanVals[i] = math.NaN()
	}

	for _, col := range newCols {
		df.SetCol(col, godf.NewFloat64Series(col, nanVals))
	}

	return df
}

// --- Stage 7: Compute Delta Features ---

func computeDeltas(df *godf.DataFrame, entityCol string, deltaWindows, pctWindows []int) *godf.DataFrame {
	if df.Empty() {
		return df
	}

	// Identify feature columns (not timestamp, entity_key)
	exclude := map[string]bool{"timestamp": true, "entity_key": true}
	var featureCols []string
	for _, col := range df.Columns() {
		if !exclude[col] && df.Col(col).Dtype() == godf.Float64 {
			featureCols = append(featureCols, col)
		}
	}

	if len(featureCols) == 0 {
		return df
	}

	// Sort by entity + timestamp
	if entityCol != "" {
		df = df.SortBy(entityCol, "timestamp")
	} else {
		df = df.SortBy("timestamp")
	}

	// Delta features
	for _, window := range deltaWindows {
		for _, col := range featureCols {
			deltaName := fmt.Sprintf("%s__delta_%dm", col, window)
			var shifted *godf.Series
			if entityCol != "" {
				shifted = df.GroupBy(entityCol).Shift(col, window)
			} else {
				shifted = df.Col(col).Shift(window)
			}
			delta := df.Col(col).Sub(shifted)
			df.SetCol(deltaName, delta)
		}
	}

	// Pct change features — applied to ALL float columns present at this point
	// (including delta columns just added), matching the Python behavior.
	exclude2 := map[string]bool{"timestamp": true, "entity_key": true}
	var allFeatureCols []string
	for _, col := range df.Columns() {
		if !exclude2[col] && df.Col(col).Dtype() == godf.Float64 {
			allFeatureCols = append(allFeatureCols, col)
		}
	}

	for _, window := range pctWindows {
		for _, col := range allFeatureCols {
			pctName := fmt.Sprintf("%s__pct_change_%dm", col, window)
			var shifted *godf.Series
			if entityCol != "" {
				shifted = df.GroupBy(entityCol).Shift(col, window)
			} else {
				shifted = df.Col(col).Shift(window)
			}
			diff := df.Col(col).Sub(shifted)
			absShifted := shifted.Abs()
			pctChange := diff.Div(absShifted).ReplaceInf(math.NaN())
			df.SetCol(pctName, pctChange)
		}
	}

	return df
}
