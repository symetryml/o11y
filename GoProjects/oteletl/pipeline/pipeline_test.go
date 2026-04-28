package pipeline

import (
	"math"
	"sort"
	"testing"

	"github.com/symetryml/godf"
)

const epsilon = 1e-6

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.IsNaN(want) {
		if !math.IsNaN(got) {
			t.Errorf("%s: got %f, want NaN", name, got)
		}
		return
	}
	if math.Abs(got-want) > epsilon {
		t.Errorf("%s: got %f, want %f", name, got, want)
	}
}

// buildGaugeData creates a simple gauge-only test dataset matching the Python test.
func buildGaugeData() *godf.DataFrame {
	records := []map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "api"}, "value": 50.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "api"}, "value": 60.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "api"}, "value": 70.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "api"}, "value": 75.0},
	}
	return godf.NewDataFrame(records)
}

func TestDenormalizeGaugesBasic(t *testing.T) {
	df := buildGaugeData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false
	cfg.GaugeWanted = []string{"last", "mean", "min", "max", "stddev"}

	result := DenormalizeMetrics(df, cfg)

	if result.Empty() {
		t.Fatal("Result is empty")
	}

	// Should have 2 rows (2 timestamps)
	if result.NRows() != 2 {
		t.Errorf("NRows: got %d, want 2", result.NRows())
	}

	// Check expected feature columns exist
	expectedCols := []string{
		"cpu_usage__last", "cpu_usage__mean", "cpu_usage__min", "cpu_usage__max", "cpu_usage__stddev",
		"memory_usage__last", "memory_usage__mean", "memory_usage__min", "memory_usage__max", "memory_usage__stddev",
	}
	for _, col := range expectedCols {
		if !result.HasColumn(col) {
			t.Errorf("Missing column: %s", col)
		}
	}

	// Verify values for cpu_usage__last (should be 50, 60 matching Python)
	if result.HasColumn("cpu_usage__last") {
		col := result.Col("cpu_usage__last")
		// Find rows with non-NaN values
		var vals []float64
		for i := 0; i < col.Len(); i++ {
			if !math.IsNaN(col.Float(i)) {
				vals = append(vals, col.Float(i))
			}
		}
		sort.Float64s(vals)
		if len(vals) >= 2 {
			assertFloat(t, "cpu_last[0]", vals[0], 50.0)
			assertFloat(t, "cpu_last[1]", vals[1], 60.0)
		}
	}

	if result.HasColumn("memory_usage__last") {
		col := result.Col("memory_usage__last")
		var vals []float64
		for i := 0; i < col.Len(); i++ {
			if !math.IsNaN(col.Float(i)) {
				vals = append(vals, col.Float(i))
			}
		}
		sort.Float64s(vals)
		if len(vals) >= 2 {
			assertFloat(t, "mem_last[0]", vals[0], 70.0)
			assertFloat(t, "mem_last[1]", vals[1], 75.0)
		}
	}
}

func TestDenormalizeGaugesStddev(t *testing.T) {
	df := buildGaugeData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false
	cfg.GaugeWanted = []string{"last", "mean", "min", "max", "stddev"}

	result := DenormalizeMetrics(df, cfg)

	// Single gauge per (timestamp, entity) → stddev should be 0
	if result.HasColumn("cpu_usage__stddev") {
		col := result.Col("cpu_usage__stddev")
		for i := 0; i < col.Len(); i++ {
			v := col.Float(i)
			if !math.IsNaN(v) {
				assertFloat(t, "stddev", v, 0.0)
			}
		}
	}
}

func TestDenormalizeWithDeltas(t *testing.T) {
	df := buildGaugeData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = true
	cfg.DeltaWindows = []int{1}
	cfg.PctChangeWindows = []int{1}

	result := DenormalizeMetrics(df, cfg)

	// Should have delta columns
	hasDelta := false
	for _, col := range result.Columns() {
		if len(col) > 10 {
			if idx := len(col) - 10; idx >= 0 && col[idx:] == "__delta_1m" {
				hasDelta = true
				break
			}
		}
	}
	if !hasDelta {
		t.Errorf("Expected delta columns to be present, got columns: %v", result.Columns())
	}
}

func TestDenormalizeEmpty(t *testing.T) {
	df := godf.NewDataFrame(nil)
	cfg := DefaultConfig()
	result := DenormalizeMetrics(df, cfg)
	if !result.Empty() {
		t.Error("Expected empty result for empty input")
	}
}

// buildHistogramData creates histogram test data.
func buildHistogramData() *godf.DataFrame {
	records := []map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "0.01"}, "value": 5.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "0.05"}, "value": 20.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "0.1"}, "value": 50.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "0.5"}, "value": 85.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "1.0"}, "value": 95.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_bucket", "labels": map[string]string{"service_name": "web", "le": "+Inf"}, "value": 100.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_sum", "labels": map[string]string{"service_name": "web"}, "value": 45.5},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_request_duration_count", "labels": map[string]string{"service_name": "web"}, "value": 100.0},
	}
	return godf.NewDataFrame(records)
}

func TestDenormalizeHistogram(t *testing.T) {
	df := buildHistogramData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	if result.Empty() {
		t.Fatal("Result is empty")
	}

	// Should have histogram feature columns
	histCols := []string{"http_request_duration__p50", "http_request_duration__p90",
		"http_request_duration__mean", "http_request_duration__count", "http_request_duration__sum"}
	for _, col := range histCols {
		if !result.HasColumn(col) {
			t.Errorf("Missing histogram column: %s (available: %v)", col, result.Columns())
		}
	}

	// Verify mean = 45.5/100 = 0.455
	if result.HasColumn("http_request_duration__mean") {
		col := result.Col("http_request_duration__mean")
		for i := 0; i < col.Len(); i++ {
			v := col.Float(i)
			if !math.IsNaN(v) {
				assertFloat(t, "hist_mean", v, 0.455)
			}
		}
	}

	// Verify count = 100
	if result.HasColumn("http_request_duration__count") {
		col := result.Col("http_request_duration__count")
		for i := 0; i < col.Len(); i++ {
			v := col.Float(i)
			if !math.IsNaN(v) {
				assertFloat(t, "hist_count", v, 100.0)
			}
		}
	}
}

// buildCounterData creates counter test data with status code labels.
func buildCounterData() *godf.DataFrame {
	records := []map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_requests_total", "labels": map[string]string{"service_name": "web", "status_code": "200"}, "value": 100.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "http_requests_total", "labels": map[string]string{"service_name": "web", "status_code": "200"}, "value": 150.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "http_requests_total", "labels": map[string]string{"service_name": "web", "status_code": "500"}, "value": 5.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "http_requests_total", "labels": map[string]string{"service_name": "web", "status_code": "500"}, "value": 7.0},
	}
	return godf.NewDataFrame(records)
}

func TestDenormalizeCounterWithStatusBucketing(t *testing.T) {
	df := buildCounterData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	if result.Empty() {
		t.Fatal("Result is empty")
	}

	// Status codes should be bucketed: 200→success, 500→server_error
	// So we should see columns like http_requests__rate__success, http_requests__rate__server_error
	cols := result.Columns()
	hasSuccess := false
	hasServerError := false
	for _, c := range cols {
		if c == "http_requests__rate__success" || c == "http_requests__count__success" {
			hasSuccess = true
		}
		if c == "http_requests__rate__server_error" || c == "http_requests__count__server_error" {
			hasServerError = true
		}
	}

	if !hasSuccess {
		t.Errorf("Expected success-bucketed columns, got: %v", cols)
	}
	if !hasServerError {
		t.Errorf("Expected server_error-bucketed columns, got: %v", cols)
	}
}

func TestDenormalizeStatusColumnStability(t *testing.T) {
	df := buildCounterData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	// _ensure_status_columns should add client_error variants
	hasClientError := false
	for _, c := range result.Columns() {
		if c == "http_requests__rate__client_error" || c == "http_requests__count__client_error" {
			hasClientError = true
			break
		}
	}
	if !hasClientError {
		t.Error("Expected client_error columns for schema stability")
	}
}

func TestDenormalizeEntityKey(t *testing.T) {
	df := buildGaugeData()
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	if !result.HasColumn("entity_key") {
		t.Fatal("Missing entity_key column")
	}

	// All rows should have service_name=api in entity key
	for i := 0; i < result.NRows(); i++ {
		ek := result.Col("entity_key").Str(i)
		if ek != "service_name=api" {
			t.Errorf("entity_key[%d] = %q, want 'service_name=api'", i, ek)
		}
	}
}

func TestDenormalizeDropsCorrelationLabels(t *testing.T) {
	records := []map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "cpu_usage",
			"labels": map[string]string{"service_name": "web", "trace_id": "abc123", "request_id": "req1"},
			"value":  50.0},
	}
	df := godf.NewDataFrame(records)
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	// trace_id and request_id should not appear in any feature names
	for _, col := range result.Columns() {
		if col == "trace_id" || col == "request_id" {
			t.Errorf("Correlation label %q should have been dropped", col)
		}
	}
}

func TestDenormalizeForceDropLabels(t *testing.T) {
	records := []map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "cpu_usage",
			"labels": map[string]string{"service_name": "web", "custom_label": "foo"},
			"value":  50.0},
	}
	df := godf.NewDataFrame(records)
	cfg := DefaultConfig()
	cfg.IncludeDeltas = false
	cfg.ForceDropLabels = []string{"custom_label"}

	result := DenormalizeMetrics(df, cfg)
	if result.Empty() {
		t.Fatal("Result is empty")
	}
	// custom_label should not appear
	for _, col := range result.Columns() {
		if col == "custom_label" {
			t.Error("force-dropped label should not appear in output")
		}
	}
}
