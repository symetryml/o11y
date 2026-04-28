package classifier

import (
	"regexp"
	"strings"
)

var reNonAlphaNum = regexp.MustCompile(`[^a-z0-9_]`)
var reMultiUnderscore = regexp.MustCompile(`_+`)

// MetricSuffixes are standard OTel/Prometheus metric suffixes.
var MetricSuffixes = []string{"_total", "_bucket", "_sum", "_count", "_info", "_created"}

// ExtractMetricFamily strips standard suffixes from a metric name.
func ExtractMetricFamily(metricName string) string {
	for _, suffix := range MetricSuffixes {
		if strings.HasSuffix(metricName, suffix) {
			return metricName[:len(metricName)-len(suffix)]
		}
	}
	return metricName
}

// ClassifyMetricType classifies a metric by its naming convention.
func ClassifyMetricType(metricName string) string {
	switch {
	case strings.HasSuffix(metricName, "_total"):
		return "counter"
	case strings.HasSuffix(metricName, "_bucket"):
		return "histogram"
	case strings.HasSuffix(metricName, "_sum") || strings.HasSuffix(metricName, "_count"):
		return "histogram_component"
	case strings.HasSuffix(metricName, "_info"):
		return "info"
	case strings.HasSuffix(metricName, "_created"):
		return "timestamp"
	default:
		return "gauge"
	}
}

// SanitizeName sanitizes a metric or label name for use in feature names.
func SanitizeName(name string) string {
	result := strings.ToLower(name)
	result = strings.ReplaceAll(result, ".", "_")
	result = strings.ReplaceAll(result, "-", "_")
	result = reNonAlphaNum.ReplaceAllString(result, "")
	result = reMultiUnderscore.ReplaceAllString(result, "_")
	result = strings.Trim(result, "_")
	return result
}
