package pipeline

import (
	"testing"

	"github.com/symetryml/godf"
)

func buildProfilerTestData() *godf.DataFrame {
	var records []map[string]any
	for _, ts := range []string{"2024-01-01T00:00:00", "2024-01-01T00:01:00"} {
		// Gauge metrics with different services
		records = append(records, map[string]any{
			"timestamp": ts, "metric": "cpu_usage",
			"labels": map[string]string{"service_name": "web", "instance": "i-1"}, "value": 50.0,
		})
		records = append(records, map[string]any{
			"timestamp": ts, "metric": "cpu_usage",
			"labels": map[string]string{"service_name": "api", "instance": "i-2"}, "value": 60.0,
		})

		// Counter with status codes (high cardinality simulation)
		for _, code := range []string{"200", "201", "301", "400", "401", "403", "404", "500", "502", "503"} {
			records = append(records, map[string]any{
				"timestamp": ts, "metric": "http_requests_total",
				"labels": map[string]string{"service_name": "web", "status_code": code}, "value": 100.0,
			})
		}

		// Histogram
		for _, le := range []string{"0.01", "0.1", "1.0", "+Inf"} {
			records = append(records, map[string]any{
				"timestamp": ts, "metric": "http_request_duration_bucket",
				"labels": map[string]string{"service_name": "web", "le": le}, "value": 50.0,
			})
		}
	}
	return godf.NewDataFrame(records)
}

func TestRunProfilerFromDataFrame(t *testing.T) {
	df := buildProfilerTestData()
	thresholds := DefaultCardinalityThresholds()

	result, err := RunProfilerFromDataFrame(df, thresholds, 20)
	if err != nil {
		t.Fatalf("RunProfilerFromDataFrame failed: %v", err)
	}

	if result.TotalFamilies == 0 {
		t.Error("Expected at least one family")
	}
	if result.TotalRows != df.NRows() {
		t.Errorf("TotalRows: got %d, want %d", result.TotalRows, df.NRows())
	}

	// Check cpu_usage family exists
	cpuFamily, ok := result.Families["cpu_usage"]
	if !ok {
		t.Fatal("Missing cpu_usage family")
	}
	if cpuFamily.Type != "gauge" {
		t.Errorf("cpu_usage type: got %q, want 'gauge'", cpuFamily.Type)
	}

	// Check service_name label in cpu_usage
	snLabel, ok := cpuFamily.Labels["service_name"]
	if !ok {
		t.Fatal("Missing service_name label in cpu_usage")
	}
	if snLabel.Cardinality != 2 {
		t.Errorf("service_name cardinality: got %d, want 2", snLabel.Cardinality)
	}
	if snLabel.Tier != 1 {
		t.Errorf("service_name tier: got %d, want 1 (cardinality=2, tier1_max=10)", snLabel.Tier)
	}

	// Check status_code label in http_requests
	httpFamily, ok := result.Families["http_requests"]
	if !ok {
		t.Fatal("Missing http_requests family")
	}
	scLabel, ok := httpFamily.Labels["status_code"]
	if !ok {
		t.Fatal("Missing status_code label in http_requests")
	}
	if scLabel.Cardinality != 10 {
		t.Errorf("status_code cardinality: got %d, want 10", scLabel.Cardinality)
	}
}

func TestRunProfilerFromDataFrame_Empty(t *testing.T) {
	df := godf.NewDataFrame(nil)
	_, err := RunProfilerFromDataFrame(df, DefaultCardinalityThresholds(), 20)
	if err == nil {
		t.Error("Expected error for empty DataFrame")
	}
}

func TestProfileResultToSchemaConfig(t *testing.T) {
	df := buildProfilerTestData()
	result, err := RunProfilerFromDataFrame(df, DefaultCardinalityThresholds(), 20)
	if err != nil {
		t.Fatal(err)
	}

	schema := result.ToSchemaConfig()
	if len(schema) == 0 {
		t.Error("Expected non-empty schema config")
	}

	// Check that status_code gets a bucket_type
	if httpSchema, ok := schema["http_requests"]; ok {
		if scSchema, ok := httpSchema.Labels["status_code"]; ok {
			if scSchema.BucketType != "status_code" {
				t.Errorf("status_code bucket_type: got %q, want 'status_code'", scSchema.BucketType)
			}
		} else {
			t.Error("Missing status_code in http_requests schema")
		}
	}
}

func TestProfilerTierAssignment(t *testing.T) {
	th := CardinalityThresholds{Tier1Max: 5, Tier2Max: 20, Tier3Max: 100}

	tests := map[int]int{
		1: 1, 5: 1, 6: 2, 20: 2, 21: 3, 100: 3, 101: 4, 1000: 4,
	}
	for card, wantTier := range tests {
		got := getTier(card, th)
		if got != wantTier {
			t.Errorf("getTier(%d) = %d, want %d", card, got, wantTier)
		}
	}
}
