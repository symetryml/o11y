package pipeline

import (
	"testing"

	"github.com/symetryml/godf"
)

func buildFilterTestData() *godf.DataFrame {
	return godf.NewDataFrame([]map[string]any{
		{"timestamp": "t1", "metric": "http_requests_total", "labels": map[string]string{"service_name": "web", "status_code": "200"}, "value": 100.0},
		{"timestamp": "t1", "metric": "http_requests_total", "labels": map[string]string{"service_name": "api", "status_code": "500"}, "value": 5.0},
		{"timestamp": "t1", "metric": "cpu_usage", "labels": map[string]string{"service_name": "web"}, "value": 50.0},
		{"timestamp": "t1", "metric": "cpu_usage", "labels": map[string]string{"service_name": "api"}, "value": 60.0},
	})
}

func TestFilterByService(t *testing.T) {
	df := buildFilterTestData()
	result := FilterByService(df, []string{"web"}, "")
	if result.NRows() != 2 {
		t.Errorf("FilterByService: got %d rows, want 2", result.NRows())
	}
}

func TestFilterByLabels(t *testing.T) {
	df := buildFilterTestData()
	result := FilterByLabels(df, map[string][]string{"status_code": {"200"}})
	if result.NRows() != 1 {
		t.Errorf("FilterByLabels: got %d rows, want 1", result.NRows())
	}
}

func TestExcludeByLabels(t *testing.T) {
	df := buildFilterTestData()
	result := ExcludeByLabels(df, map[string][]string{"service_name": {"api"}})
	if result.NRows() != 2 {
		t.Errorf("ExcludeByLabels: got %d rows, want 2", result.NRows())
	}
}

func TestFilterByMetrics(t *testing.T) {
	df := buildFilterTestData()
	result := FilterByMetrics(df, []string{`^http_`}, false)
	if result.NRows() != 2 {
		t.Errorf("FilterByMetrics include: got %d rows, want 2", result.NRows())
	}

	result2 := FilterByMetrics(df, []string{`^http_`}, true)
	if result2.NRows() != 2 {
		t.Errorf("FilterByMetrics exclude: got %d rows, want 2", result2.NRows())
	}
}

func TestGetAvailableServices(t *testing.T) {
	df := buildFilterTestData()
	services := GetAvailableServices(df, "")
	if len(services) != 2 {
		t.Errorf("GetAvailableServices: got %d, want 2", len(services))
	}
	if services[0] != "api" || services[1] != "web" {
		t.Errorf("GetAvailableServices: got %v, want [api, web]", services)
	}
}

func TestGetLabelValues(t *testing.T) {
	df := buildFilterTestData()
	vals := GetLabelValues(df, "status_code")
	if len(vals) != 2 {
		t.Errorf("GetLabelValues: got %d, want 2", len(vals))
	}
}

func TestFilterSalientMetrics(t *testing.T) {
	metrics := []string{
		"rpc_client_duration_ms_bucket",
		"rpc_client_duration_ms_sum",
		"rpc_client_duration_ms_count",
		"rpc_client_request_size_bytes_bucket",
		"rpc_client_request_size_bytes_sum",
		"http_server_request_duration_bucket",
		"http_server_request_duration_sum",
		"http_server_requests_total",
		"go_memory_used_bytes",
		"go_config_gogc",
	}

	cfg := DefaultSalientConfig()
	result := FilterSalientMetrics(metrics, cfg)

	// Should keep duration metrics, drop size_bytes, go_config
	hasRPCDuration := false
	hasHTTPDuration := false
	hasSizeBytes := false
	hasGoConfig := false

	for _, m := range result {
		if m == "rpc_client_duration_ms_bucket" {
			hasRPCDuration = true
		}
		if m == "http_server_request_duration_bucket" {
			hasHTTPDuration = true
		}
		if m == "rpc_client_request_size_bytes_bucket" {
			hasSizeBytes = true
		}
		if m == "go_config_gogc" {
			hasGoConfig = true
		}
	}

	if !hasRPCDuration {
		t.Error("Should keep rpc_client_duration metrics")
	}
	if !hasHTTPDuration {
		t.Error("Should keep http_server_request_duration metrics")
	}
	if hasSizeBytes {
		t.Error("Should drop size_bytes metrics")
	}
	if hasGoConfig {
		t.Error("Should drop go_config metrics")
	}
}
