// Package classifier provides semantic classification of OTel metric labels.
package classifier

import (
	"regexp"
	"strings"
)

// LabelCategory represents the semantic category of a label.
type LabelCategory string

const (
	Resource          LabelCategory = "resource"
	Signal            LabelCategory = "signal"
	Dimension         LabelCategory = "dimension"
	Correlation       LabelCategory = "correlation"
	HistogramInternal LabelCategory = "internal"
	Metadata          LabelCategory = "metadata"
)

// LabelClassification holds the classification result.
type LabelClassification struct {
	Category   LabelCategory
	Handling   string
	BucketType string // Empty if not applicable.
}

type labelRule struct {
	names      map[string]bool
	category   LabelCategory
	handling   string
	bucketType string
}

var labelRules []labelRule

func init() {
	defs := []struct {
		names      []string
		category   LabelCategory
		handling   string
		bucketType string
	}{
		// Resource labels
		{[]string{"service", "service_name", "job", "app", "application"}, Resource, "keep", ""},
		{[]string{"instance", "host", "pod", "node", "container", "namespace", "pod_name", "container_name"}, Resource, "keep_or_aggregate", ""},
		{[]string{"env", "environment", "deployment", "cluster", "region", "zone", "dc", "datacenter"}, Resource, "keep", ""},

		// Signal labels
		{[]string{"status", "status_code", "http_status_code", "code", "grpc_code", "grpc_status", "response_code"}, Signal, "bucket", "status_code"},
		{[]string{"error", "exception", "error_type", "exception_type", "exception_class"}, Signal, "keep_type", ""},
		{[]string{"error_message", "exception_message", "error_msg"}, Signal, "drop", ""},

		// Dimension labels
		{[]string{"method", "http_method", "request_method", "http_request_method"}, Dimension, "bucket", "http_method"},
		{[]string{"route", "http_route", "uri", "path", "url", "endpoint", "target", "http_target", "url_path"}, Dimension, "parameterize_or_top_n", "route"},
		{[]string{"operation", "db_operation", "db_statement", "command", "db_system"}, Dimension, "bucket", "operation"},
		{[]string{"rpc_method", "rpc_service", "grpc_method", "grpc_service"}, Dimension, "bucket", "rpc_operation"},
		{[]string{"messaging_operation", "messaging_destination", "messaging_system"}, Dimension, "bucket", "messaging"},

		// Correlation labels
		{[]string{"trace_id", "span_id", "traceid", "spanid"}, Correlation, "drop", ""},
		{[]string{"user_id", "userid", "customer_id", "customerid", "account_id"}, Correlation, "drop", ""},
		{[]string{"request_id", "requestid", "correlation_id", "correlationid", "session_id", "sessionid"}, Correlation, "drop", ""},

		// Histogram internals
		{[]string{"le", "quantile"}, HistogramInternal, "special", ""},

		// Metadata labels
		{[]string{"version", "sdk_version", "library_version", "otel_scope_version"}, Metadata, "drop_or_resource", ""},
		{[]string{"otel_scope_name", "telemetry_sdk_name", "telemetry_sdk_language", "telemetry_sdk_version"}, Metadata, "drop", ""},
	}

	for _, d := range defs {
		m := make(map[string]bool, len(d.names))
		for _, n := range d.names {
			m[n] = true
		}
		labelRules = append(labelRules, labelRule{
			names:      m,
			category:   d.category,
			handling:   d.handling,
			bucketType: d.bucketType,
		})
	}
}

var suffixPatterns = []struct {
	re       *regexp.Regexp
	category LabelCategory
	handling string
}{
	{regexp.MustCompile(`_id$`), Correlation, "drop"},
	{regexp.MustCompile(`_uuid$`), Correlation, "drop"},
	{regexp.MustCompile(`_key$`), Correlation, "drop"},
}

var statusSubstrings = []string{"status_code", "status", "grpc_status", "grpc_code", "response_code"}

// ClassifyLabel classifies a label by its name pattern.
func ClassifyLabel(labelName string) LabelClassification {
	normalized := strings.ToLower(strings.ReplaceAll(labelName, "-", "_"))

	// Exact match
	for _, rule := range labelRules {
		if rule.names[normalized] {
			return LabelClassification{
				Category:   rule.category,
				Handling:   rule.handling,
				BucketType: rule.bucketType,
			}
		}
	}

	// Substring match for status labels
	for _, sub := range statusSubstrings {
		if strings.Contains(normalized, sub) {
			return LabelClassification{
				Category:   Signal,
				Handling:   "bucket",
				BucketType: "status_code",
			}
		}
	}

	// Suffix patterns
	for _, sp := range suffixPatterns {
		if sp.re.MatchString(normalized) {
			return LabelClassification{
				Category: sp.category,
				Handling: sp.handling,
			}
		}
	}

	return LabelClassification{
		Category: Dimension,
		Handling: "auto",
	}
}

// IsEntityLabel checks if a label should be used as part of the entity key.
func IsEntityLabel(labelName string) bool {
	c := ClassifyLabel(labelName)
	if c.Category != Resource {
		return false
	}
	if c.Handling == "drop" || c.Handling == "drop_or_resource" {
		return false
	}

	normalized := strings.ToLower(strings.ReplaceAll(labelName, "-", "_"))
	entityLabels := map[string]bool{
		"service": true, "service_name": true, "job": true, "app": true, "application": true,
		"instance": true, "host": true, "pod": true, "node": true, "namespace": true,
	}
	return entityLabels[normalized]
}
