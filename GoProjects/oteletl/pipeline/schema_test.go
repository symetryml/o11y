package pipeline

import (
	"os"
	"testing"
)

var realSchemaPath = func() string {
	if p := os.Getenv("SCHEMA_PATH"); p != "" {
		return p
	}
	return "../testdata/schema_config-otel001.yaml"
}()

func TestLoadSchemaFile(t *testing.T) {
	if _, err := os.Stat(realSchemaPath); os.IsNotExist(err) {
		t.Skipf("Schema file not found: %s", realSchemaPath)
	}

	schema, err := LoadSchemaFile(realSchemaPath)
	if err != nil {
		t.Fatalf("LoadSchemaFile: %v", err)
	}

	if len(schema) == 0 {
		t.Fatal("Schema has no metrics")
	}

	t.Logf("Loaded schema with %d metric families", len(schema))

	// Check a known metric exists
	if _, ok := schema["app_ads_ad_requests"]; !ok {
		t.Error("Expected app_ads_ad_requests in schema")
	}

	// Check label actions are parsed
	if ms, ok := schema["app_ads_ad_requests"]; ok {
		if ls, ok := ms.Labels["service_instance_id"]; ok {
			if ls.Action != "drop" {
				t.Errorf("service_instance_id action: got %q, want 'drop'", ls.Action)
			}
		} else {
			t.Error("Expected service_instance_id label in app_ads_ad_requests")
		}
	}

	// Check bucket_type is parsed for status_code labels
	found := false
	for _, ms := range schema {
		for _, ls := range ms.Labels {
			if ls.BucketType == "status_code" {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	if !found {
		t.Error("Expected at least one label with bucket_type=status_code")
	}
}

func TestDenormalizeWithSchemaPath(t *testing.T) {
	if _, err := os.Stat(realSchemaPath); os.IsNotExist(err) {
		t.Skipf("Schema file not found: %s", realSchemaPath)
	}

	// Build simple gauge data matching a metric in the schema
	df := buildGaugeData()

	cfg := DefaultConfig()
	cfg.SchemaPath = realSchemaPath
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)

	if result.Empty() {
		t.Fatal("Result is empty")
	}

	t.Logf("Output: %d rows, %d columns", result.NRows(), len(result.Columns()))
}

func TestLoadSchemaFileNotFound(t *testing.T) {
	_, err := LoadSchemaFile("/nonexistent/path.yaml")
	if err == nil {
		t.Error("Expected error for missing file")
	}
}

func TestSaveAndLoadSchemaRoundTrip(t *testing.T) {
	original := map[string]MetricSchema{
		"http_requests": {
			Labels: map[string]LabelSchema{
				"status_code": {Action: "bucket", BucketType: "status_code"},
				"method":      {Action: "bucket", BucketType: "http_method"},
				"trace_id":    {Action: "drop"},
				"route":       {Action: "top_n", BucketType: "route", TopValues: []string{"/api/v1", "/api/v2"}},
			},
		},
		"cpu_usage": {
			Labels: map[string]LabelSchema{
				"service_name": {Action: "keep"},
			},
		},
	}

	tmpFile := t.TempDir() + "/test_schema.yaml"

	err := SaveSchemaFile(original, tmpFile)
	if err != nil {
		t.Fatalf("SaveSchemaFile: %v", err)
	}

	loaded, err := LoadSchemaFile(tmpFile)
	if err != nil {
		t.Fatalf("LoadSchemaFile: %v", err)
	}

	// Verify round-trip
	if len(loaded) != len(original) {
		t.Fatalf("Metric count: got %d, want %d", len(loaded), len(original))
	}

	for metricName, origMS := range original {
		loadedMS, ok := loaded[metricName]
		if !ok {
			t.Errorf("Missing metric: %s", metricName)
			continue
		}
		for labelName, origLS := range origMS.Labels {
			loadedLS, ok := loadedMS.Labels[labelName]
			if !ok {
				t.Errorf("Missing label %s.%s", metricName, labelName)
				continue
			}
			if loadedLS.Action != origLS.Action {
				t.Errorf("%s.%s action: got %q, want %q", metricName, labelName, loadedLS.Action, origLS.Action)
			}
			if loadedLS.BucketType != origLS.BucketType {
				t.Errorf("%s.%s bucket_type: got %q, want %q", metricName, labelName, loadedLS.BucketType, origLS.BucketType)
			}
		}
	}
}

func TestProfilerToSchemaToFile(t *testing.T) {
	// Generate schema from profiler, save to file, reload, use in pipeline
	df := buildProfilerTestData()

	profile, err := RunProfilerFromDataFrame(df, DefaultCardinalityThresholds(), 20)
	if err != nil {
		t.Fatal(err)
	}

	schema := profile.ToSchemaConfig()

	tmpFile := t.TempDir() + "/profiled_schema.yaml"
	err = SaveSchemaFile(schema, tmpFile)
	if err != nil {
		t.Fatalf("SaveSchemaFile: %v", err)
	}

	// Now use that schema file in a pipeline run
	cfg := DefaultConfig()
	cfg.SchemaPath = tmpFile
	cfg.IncludeDeltas = false

	result := DenormalizeMetrics(df, cfg)
	if result.Empty() {
		t.Fatal("Result is empty with profiled schema")
	}

	t.Logf("Profiler → schema → pipeline: %d rows, %d columns", result.NRows(), len(result.Columns()))
}
