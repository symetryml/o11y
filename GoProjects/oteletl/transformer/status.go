// Package transformer provides label value transformations for OTel metrics.
package transformer

import (
	"regexp"
	"strconv"
	"strings"
)

// StatusBucket represents a categorized status code.
type StatusBucket string

const (
	StatusSuccess       StatusBucket = "success"
	StatusRedirect      StatusBucket = "redirect"
	StatusClientError   StatusBucket = "client_error"
	StatusServerError   StatusBucket = "server_error"
	StatusInformational StatusBucket = "informational"
	StatusUnknown       StatusBucket = "unknown"
)

// grpcStatusMap maps gRPC numeric status codes to buckets.
var grpcStatusMap = map[int]StatusBucket{
	0:  StatusSuccess,     // OK
	1:  StatusClientError, // CANCELLED
	2:  StatusServerError, // UNKNOWN
	3:  StatusClientError, // INVALID_ARGUMENT
	4:  StatusServerError, // DEADLINE_EXCEEDED
	5:  StatusClientError, // NOT_FOUND
	6:  StatusClientError, // ALREADY_EXISTS
	7:  StatusClientError, // PERMISSION_DENIED
	8:  StatusServerError, // RESOURCE_EXHAUSTED
	9:  StatusClientError, // FAILED_PRECONDITION
	10: StatusClientError, // ABORTED
	11: StatusClientError, // OUT_OF_RANGE
	12: StatusServerError, // UNIMPLEMENTED
	13: StatusServerError, // INTERNAL
	14: StatusServerError, // UNAVAILABLE
	15: StatusServerError, // DATA_LOSS
	16: StatusClientError, // UNAUTHENTICATED
}

// BucketHTTPStatus buckets an HTTP status code (numeric or string).
func BucketHTTPStatus(status string) StatusBucket {
	code, err := strconv.Atoi(strings.TrimSpace(status))
	if err != nil {
		return bucketByText(status)
	}

	switch {
	case code >= 100 && code < 200:
		return StatusInformational
	case code >= 200 && code < 300:
		return StatusSuccess
	case code >= 300 && code < 400:
		return StatusRedirect
	case code >= 400 && code < 500:
		return StatusClientError
	case code >= 500 && code < 600:
		return StatusServerError
	}
	return StatusUnknown
}

// BucketGRPCStatus buckets a gRPC status code (numeric or string).
func BucketGRPCStatus(status string) StatusBucket {
	code, err := strconv.Atoi(strings.TrimSpace(status))
	if err != nil {
		return bucketByText(status)
	}
	if bucket, ok := grpcStatusMap[code]; ok {
		return bucket
	}
	return StatusUnknown
}

var (
	reSuccessText     = regexp.MustCompile(`\b(ok|success|200|2\d\d)\b`)
	reErrorText       = regexp.MustCompile(`\b(err|fail|exception|error)\b`)
	reClientErrorText = regexp.MustCompile(`\b(not.?found|unauthorized|forbidden|4\d\d)\b`)
	reServerErrorText = regexp.MustCompile(`\b(5\d\d|internal|unavailable)\b`)
	reRedirectText    = regexp.MustCompile(`\b(redirect|3\d\d)\b`)
)

func bucketByText(status string) StatusBucket {
	lower := strings.ToLower(status)

	if reSuccessText.MatchString(lower) {
		return StatusSuccess
	}
	if reErrorText.MatchString(lower) {
		return StatusServerError
	}
	if reClientErrorText.MatchString(lower) {
		return StatusClientError
	}
	if reServerErrorText.MatchString(lower) {
		return StatusServerError
	}
	if reRedirectText.MatchString(lower) {
		return StatusRedirect
	}
	return StatusUnknown
}

// BucketStatusCode performs smart status code bucketing based on the label name hint.
func BucketStatusCode(status, labelName string) StatusBucket {
	if labelName != "" {
		lowerName := strings.ToLower(labelName)
		if strings.Contains(lowerName, "grpc") {
			return BucketGRPCStatus(status)
		}
		if strings.Contains(lowerName, "http") {
			return BucketHTTPStatus(status)
		}
	}

	code, err := strconv.Atoi(strings.TrimSpace(status))
	if err != nil {
		return bucketByText(status)
	}
	if code >= 100 {
		return BucketHTTPStatus(status)
	}
	return BucketGRPCStatus(status)
}
