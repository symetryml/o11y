package classifier

import "testing"

func TestClassifyLabel(t *testing.T) {
	tests := map[string]struct {
		wantCat    LabelCategory
		wantBucket string
	}{
		"service_name": {Resource, ""},
		"status_code":  {Signal, "status_code"},
		"http_method":  {Dimension, "http_method"},
		"trace_id":     {Correlation, ""},
		"le":           {HistogramInternal, ""},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			c := ClassifyLabel(name)
			if c.Category != tc.wantCat {
				t.Errorf("ClassifyLabel(%q).Category = %q, want %q", name, c.Category, tc.wantCat)
			}
			if c.BucketType != tc.wantBucket {
				t.Errorf("ClassifyLabel(%q).BucketType = %q, want %q", name, c.BucketType, tc.wantBucket)
			}
		})
	}
}

func TestIsEntityLabel(t *testing.T) {
	tests := map[string]bool{
		"service_name": true,
		"instance":     true,
		"trace_id":     false,
		"http_method":  false,
	}
	for name, want := range tests {
		t.Run(name, func(t *testing.T) {
			got := IsEntityLabel(name)
			if got != want {
				t.Errorf("IsEntityLabel(%q) = %v, want %v", name, got, want)
			}
		})
	}
}

func TestExtractMetricFamily(t *testing.T) {
	tests := map[string]string{
		"http_requests_total":          "http_requests",
		"http_request_duration_bucket": "http_request_duration",
		"http_request_duration_sum":    "http_request_duration",
		"cpu_usage":                    "cpu_usage",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			got := ExtractMetricFamily(input)
			if got != want {
				t.Errorf("ExtractMetricFamily(%q) = %q, want %q", input, got, want)
			}
		})
	}
}

func TestClassifyMetricType(t *testing.T) {
	tests := map[string]string{
		"http_requests_total":          "counter",
		"http_request_duration_bucket": "histogram",
		"http_request_duration_sum":    "histogram_component",
		"cpu_usage":                    "gauge",
	}
	for input, want := range tests {
		t.Run(input, func(t *testing.T) {
			got := ClassifyMetricType(input)
			if got != want {
				t.Errorf("ClassifyMetricType(%q) = %q, want %q", input, got, want)
			}
		})
	}
}
