package prometheus

import (
	"testing"
	"time"

	"github.com/symetryml/godf"
)

func buildTestMetricsDF() *godf.DataFrame {
	return godf.NewDataFrame([]map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 50.0},
		{"timestamp": "2024-01-01T00:00:30", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 51.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 55.0},
		{"timestamp": "2024-01-01T00:02:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 60.0},
		{"timestamp": "2024-01-01T00:03:00", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 65.0},
		{"timestamp": "2024-01-01T00:00:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "web"}, "value": 70.0},
		{"timestamp": "2024-01-01T00:01:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "web"}, "value": 72.0},
		{"timestamp": "2024-01-01T00:02:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "web"}, "value": 75.0},
		{"timestamp": "2024-01-01T00:03:00", "metric": "memory_usage", "labels": map[string]string{"service_name": "web"}, "value": 78.0},
	})
}

func TestFetchMetricsRangeDF_All(t *testing.T) {
	df := buildTestMetricsDF()
	result := FetchMetricsRangeDF(df, nil, nil, nil, "60s")

	if result.Empty() {
		t.Fatal("Result should not be empty")
	}

	// With 60s floor, the 00:00:00 and 00:00:30 cpu_usage rows should dedup to one
	// Expected: cpu_usage at 00:00, 00:01, 00:02, 00:03 + memory at 00:00, 00:01, 00:02, 00:03
	if result.NRows() != 8 {
		t.Errorf("NRows: got %d, want 8", result.NRows())
	}
}

func TestFetchMetricsRangeDF_FilterMetrics(t *testing.T) {
	df := buildTestMetricsDF()
	result := FetchMetricsRangeDF(df, []string{"cpu_usage"}, nil, nil, "60s")

	// Should only have cpu_usage rows
	metricCol := result.Col("metric")
	for i := 0; i < result.NRows(); i++ {
		if metricCol.Str(i) != "cpu_usage" {
			t.Errorf("Row %d: unexpected metric %q", i, metricCol.Str(i))
		}
	}
}

func TestFetchMetricsRangeDF_TimeRange(t *testing.T) {
	df := buildTestMetricsDF()
	start := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)
	end := time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC)

	result := FetchMetricsRangeDF(df, nil, &start, &end, "60s")

	if result.Empty() {
		t.Fatal("Result should not be empty for time-ranged query")
	}

	// Should only have rows at 00:01 and 00:02
	tsSeries := result.Col("timestamp")
	for i := 0; i < result.NRows(); i++ {
		ts := tsSeries.Time(i)
		if ts.Before(start) || ts.After(end) {
			t.Errorf("Row %d: timestamp %v outside range [%v, %v]", i, ts, start, end)
		}
	}
}

func TestFetchMetricsRangeDF_Empty(t *testing.T) {
	df := godf.NewDataFrame(nil)
	result := FetchMetricsRangeDF(df, nil, nil, nil, "60s")
	if !result.Empty() {
		t.Error("Expected empty result for empty input")
	}
}

func TestFetchMetricsRangeDF_NoMatch(t *testing.T) {
	df := buildTestMetricsDF()
	result := FetchMetricsRangeDF(df, []string{"nonexistent_metric"}, nil, nil, "60s")
	if !result.Empty() {
		t.Error("Expected empty result for non-matching metric filter")
	}
}

func TestFetchMetricsRangeDF_Dedup(t *testing.T) {
	// Two rows at same timestamp for same metric+labels — should keep last
	df := godf.NewDataFrame([]map[string]any{
		{"timestamp": "2024-01-01T00:00:00", "metric": "cpu", "labels": map[string]string{"svc": "a"}, "value": 1.0},
		{"timestamp": "2024-01-01T00:00:30", "metric": "cpu", "labels": map[string]string{"svc": "a"}, "value": 2.0},
	})

	result := FetchMetricsRangeDF(df, nil, nil, nil, "60s")
	if result.NRows() != 1 {
		t.Fatalf("Dedup: got %d rows, want 1", result.NRows())
	}
	// Should keep the last value (2.0)
	val := result.Col("value").Float(0)
	if val != 2.0 {
		t.Errorf("Dedup: kept value %f, want 2.0", val)
	}
}

func TestIterMetricsWindows(t *testing.T) {
	df := buildTestMetricsDF()
	windows := IterMetricsWindows(df, nil, 2, "60s")

	if len(windows) == 0 {
		t.Fatal("Expected at least one window")
	}

	// Total span is 3 minutes (00:00 to 00:03), window=2min
	// Should get windows: [00:00, 00:02) and [00:02, 00:04)
	if len(windows) != 2 {
		t.Errorf("Expected 2 windows, got %d", len(windows))
	}

	// Each window should have both metrics
	for i, w := range windows {
		if w.DF.Empty() {
			t.Errorf("Window %d is empty", i)
		}
		if !w.DF.HasColumn("metric") {
			t.Errorf("Window %d missing 'metric' column", i)
		}
		if w.DF.HasColumn("_sk") {
			t.Errorf("Window %d should not expose internal '_sk' column", i)
		}
	}
}

func TestIterMetricsWindows_Empty(t *testing.T) {
	df := godf.NewDataFrame(nil)
	windows := IterMetricsWindows(df, nil, 5, "60s")
	if len(windows) != 0 {
		t.Error("Expected no windows for empty input")
	}
}

func TestIterMetricsWindows_FilterMetrics(t *testing.T) {
	df := buildTestMetricsDF()
	windows := IterMetricsWindows(df, []string{"cpu_usage"}, 5, "60s")

	for _, w := range windows {
		metricCol := w.DF.Col("metric")
		for i := 0; i < w.DF.NRows(); i++ {
			if metricCol.Str(i) != "cpu_usage" {
				t.Errorf("Window contains unexpected metric %q", metricCol.Str(i))
			}
		}
	}
}

func TestStepToDuration(t *testing.T) {
	tests := map[string]time.Duration{
		"60s": 60 * time.Second,
		"5m":  5 * time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
	}
	for step, want := range tests {
		got := stepToDuration(step)
		if got != want {
			t.Errorf("stepToDuration(%q) = %v, want %v", step, got, want)
		}
	}
}
