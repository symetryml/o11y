package transformer

import "testing"

func TestBucketStatusCode(t *testing.T) {
	tests := map[string]struct {
		status, label string
		want          StatusBucket
	}{
		"http_200":  {"200", "http_status_code", StatusSuccess},
		"http_404":  {"404", "http_status_code", StatusClientError},
		"http_500":  {"500", "http_status_code", StatusServerError},
		"grpc_0":    {"0", "grpc_status", StatusSuccess},
		"grpc_13":   {"13", "grpc_status", StatusServerError},
		"auto_200":  {"200", "", StatusSuccess},
		"auto_0":    {"0", "", StatusSuccess},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := BucketStatusCode(tc.status, tc.label)
			if got != tc.want {
				t.Errorf("BucketStatusCode(%q, %q) = %q, want %q", tc.status, tc.label, got, tc.want)
			}
		})
	}
}

func TestBucketHTTPMethod(t *testing.T) {
	tests := map[string]MethodBucket{
		"GET": MethodRead, "POST": MethodWrite, "DELETE": MethodWrite,
		"HEAD": MethodRead, "CONNECT": MethodOther, "WEIRD": MethodUnknown,
	}
	for method, want := range tests {
		t.Run(method, func(t *testing.T) {
			got := BucketHTTPMethod(method)
			if got != want {
				t.Errorf("BucketHTTPMethod(%q) = %q, want %q", method, got, want)
			}
		})
	}
}

func TestBucketOperation(t *testing.T) {
	tests := map[string]OperationBucket{
		"SELECT * FROM users": OpRead,
		"INSERT INTO orders":  OpWrite,
		"GetUser":             OpRead,
		"CreateOrder":         OpWrite,
		"StreamEvents":        OpStream,
	}
	for op, want := range tests {
		t.Run(op, func(t *testing.T) {
			got := BucketOperation(op)
			if got != want {
				t.Errorf("BucketOperation(%q) = %q, want %q", op, got, want)
			}
		})
	}
}

func TestParameterizeRoute(t *testing.T) {
	tests := map[string]struct {
		input, want string
	}{
		"uuid":       {"/api/users/550e8400-e29b-41d4-a716-446655440000/profile", "/api/users/{uuid}/profile"},
		"numeric_id": {"/api/orders/12345/items", "/api/orders/{id}/items"},
		"date":       {"/api/reports/2024-01-15", "/api/reports/{date}"},
		"plain":      {"/api/health", "/api/health"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := ParameterizeRoute(tc.input)
			if got != tc.want {
				t.Errorf("ParameterizeRoute(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestTopNFilter(t *testing.T) {
	f := NewTopNFilter([]string{"a", "b", "c"}, "")
	if f.Filter("a") != "a" {
		t.Error("expected 'a'")
	}
	if f.Filter("d") != OtherBucket {
		t.Errorf("expected %q, got %q", OtherBucket, f.Filter("d"))
	}
}
