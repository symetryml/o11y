package transformer

import "strings"

// MethodBucket represents a categorized HTTP method.
type MethodBucket string

const (
	MethodRead    MethodBucket = "read"
	MethodWrite   MethodBucket = "write"
	MethodOther   MethodBucket = "other"
	MethodUnknown MethodBucket = "unknown"
)

var readMethods = map[string]bool{
	"GET": true, "HEAD": true, "OPTIONS": true, "TRACE": true,
}

var writeMethods = map[string]bool{
	"POST": true, "PUT": true, "PATCH": true, "DELETE": true,
}

var otherMethods = map[string]bool{
	"CONNECT": true,
}

// BucketHTTPMethod buckets an HTTP method into read/write/other.
func BucketHTTPMethod(method string) MethodBucket {
	if method == "" {
		return MethodUnknown
	}
	upper := strings.ToUpper(strings.TrimSpace(method))
	if readMethods[upper] {
		return MethodRead
	}
	if writeMethods[upper] {
		return MethodWrite
	}
	if otherMethods[upper] {
		return MethodOther
	}
	return MethodUnknown
}
