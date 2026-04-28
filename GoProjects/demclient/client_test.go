package demclient

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// mockDEM creates a test HTTP server that mimics the SymetryML DEM API.
func mockDEM(t *testing.T) (*httptest.Server, *Client) {
	t.Helper()

	projects := map[string]bool{}

	mux := http.NewServeMux()

	// List projects
	mux.HandleFunc("/symetry/rest/testkey/projects/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			http.Error(w, "method not allowed", 405)
			return
		}
		// Check auth headers exist
		if r.Header.Get("sym-date") == "" || r.Header.Get("Authorization") == "" {
			t.Error("Missing auth headers")
		}

		var names []string
		for p := range projects {
			names = append(names, p+":cpu")
		}
		resp := map[string]any{
			"statusCode":   "OK",
			"statusString": "OK",
			"values": map[string]any{
				"stringList": map[string]any{
					"values": names,
				},
			},
		}
		json.NewEncoder(w).Encode(resp)
	})

	// Create project
	mux.HandleFunc("/symetry/rest/testkey/projects", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			http.Error(w, "method not allowed", 405)
			return
		}
		pid := r.URL.Query().Get("pid")
		if pid == "" {
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": 400, "statusString": "missing pid",
			})
			return
		}
		projects[pid] = true
		json.NewEncoder(w).Encode(map[string]any{
			"statusCode": 200, "statusString": "OK", "values": map[string]any{},
		})
	})

	// Catch-all for project-specific routes
	mux.HandleFunc("/symetry/rest/testkey/projects/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		parts := strings.Split(strings.TrimPrefix(path, "/symetry/rest/testkey/projects/"), "/")
		pid := parts[0]

		if len(parts) >= 2 && parts[1] == "learn" {
			// Stream data
			body, _ := io.ReadAll(r.Body)
			var df SMLDataFrame
			if err := json.Unmarshal(body, &df); err != nil {
				json.NewEncoder(w).Encode(map[string]any{
					"statusCode": 400, "statusString": "invalid dataframe",
				})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": 200, "statusString": "OK",
				"values": map[string]any{"rowsProcessed": len(df.Data)},
			})
			return
		}

		if len(parts) >= 2 && parts[1] == "info" {
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": "OK", "statusString": "OK",
				"values": map[string]any{
					"projectName": pid,
					"type":        "cpu",
					"numRows":     42,
				},
			})
			return
		}

		if len(parts) >= 2 && parts[1] == "explore" {
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": "OK", "statusString": "OK",
				"values": map[string]any{
					"KSVDMap": map[string]any{
						"values": []map[string]any{
							{"mean": 3.14, "stddev": 1.5, "min": 0.0, "max": 10.0, "count": 100.0},
						},
					},
				},
			})
			return
		}

		if len(parts) >= 2 && parts[1] == "densityEstimate" {
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": "OK", "statusString": "OK",
				"values": map[string]any{
					"bins": []float64{1, 2, 3, 4, 5},
				},
			})
			return
		}

		if r.Method == "DELETE" {
			delete(projects, pid)
			json.NewEncoder(w).Encode(map[string]any{
				"statusCode": 200, "statusString": "OK", "values": map[string]any{},
			})
			return
		}

		http.NotFound(w, r)
	})

	server := httptest.NewServer(mux)
	client := NewClient(Config{
		Server:       server.URL,
		SymKeyID:     "testkey",
		SymSecretKey: "dGVzdHNlY3JldA==", // base64("testsecret")
	})

	return server, client
}

func TestListProjectsEmpty(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	projects, err := client.ListProjects()
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(projects) != 0 {
		t.Errorf("Expected 0 projects, got %d", len(projects))
	}
}

func TestCreateAndListProject(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	err := client.CreateProject("test_proj", "cpu", true)
	if err != nil {
		t.Fatalf("CreateProject: %v", err)
	}

	projects, err := client.ListProjects()
	if err != nil {
		t.Fatalf("ListProjects: %v", err)
	}
	if len(projects) != 1 || projects[0] != "test_proj" {
		t.Errorf("Expected [test_proj], got %v", projects)
	}
}

func TestEnsureProject(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	// First call creates
	err := client.EnsureProject("my_proj", "cpu")
	if err != nil {
		t.Fatalf("EnsureProject (create): %v", err)
	}

	// Second call is a no-op
	err = client.EnsureProject("my_proj", "cpu")
	if err != nil {
		t.Fatalf("EnsureProject (exists): %v", err)
	}
}

func TestStreamData(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	client.CreateProject("data_proj", "cpu", true)

	df := &SMLDataFrame{
		AttributeNames: []string{"x", "y"},
		AttributeTypes: []string{"C", "C"},
		Data: [][]any{
			{1.0, 2.0},
			{3.0, 4.0},
			{5.0, 6.0},
		},
	}

	err := client.StreamData("data_proj", df)
	if err != nil {
		t.Fatalf("StreamData: %v", err)
	}
}

func TestStreamCSV(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	client.CreateProject("csv_proj", "cpu", true)

	csv := "x,y,name\n1.0,2.0,alice\n3.0,4.0,bob\n5.0,6.0,charlie\n"
	n, err := client.StreamCSV("csv_proj", strings.NewReader(csv), 2)
	if err != nil {
		t.Fatalf("StreamCSV: %v", err)
	}
	if n != 3 {
		t.Errorf("StreamCSV: streamed %d rows, want 3", n)
	}
}

func TestGetProjectInfo(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	client.CreateProject("info_proj", "cpu", true)

	resp, err := client.GetProjectInfo("info_proj")
	if err != nil {
		t.Fatalf("GetProjectInfo: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode: got %d, want 200", resp.StatusCode)
	}
}

func TestGetUnivariateStats(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	stats, err := client.GetUnivariateStats("proj", "age")
	if err != nil {
		t.Fatalf("GetUnivariateStats: %v", err)
	}
	if stats["mean"] != 3.14 {
		t.Errorf("mean: got %v, want 3.14", stats["mean"])
	}
}

func TestGetBivariateStats(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	stats, err := client.GetBivariateStats("proj", "age", "bmi")
	if err != nil {
		t.Fatalf("GetBivariateStats: %v", err)
	}
	if stats == nil {
		t.Error("Expected non-nil stats")
	}
}

func TestGetHistogram(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	resp, err := client.GetHistogram("proj", "age", 20)
	if err != nil {
		t.Fatalf("GetHistogram: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Errorf("StatusCode: got %d, want 200", resp.StatusCode)
	}
}

func TestDeleteProject(t *testing.T) {
	server, client := mockDEM(t)
	defer server.Close()

	client.CreateProject("del_proj", "cpu", true)
	err := client.DeleteProject("del_proj")
	if err != nil {
		t.Fatalf("DeleteProject: %v", err)
	}

	projects, _ := client.ListProjects()
	for _, p := range projects {
		if p == "del_proj" {
			t.Error("Project should have been deleted")
		}
	}
}

func TestAuthHeaders(t *testing.T) {
	var capturedHeaders http.Header
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedHeaders = r.Header
		json.NewEncoder(w).Encode(map[string]any{
			"statusCode": "OK", "statusString": "OK",
			"values": map[string]any{"stringList": map[string]any{"values": []string{}}},
		})
	}))
	defer server.Close()

	client := NewClient(Config{
		Server:       server.URL,
		SymKeyID:     "mykey",
		SymSecretKey: "dGVzdHNlY3JldA==",
	})

	client.ListProjects()

	if capturedHeaders.Get("sym-date") == "" {
		t.Error("Missing sym-date header")
	}
	if capturedHeaders.Get("Authorization") == "" {
		t.Error("Missing Authorization header")
	}
	if capturedHeaders.Get("sym-version") == "" {
		t.Error("Missing sym-version header")
	}
	if capturedHeaders.Get("sym-client") != "demclient" {
		t.Errorf("sym-client: got %q, want 'demclient'", capturedHeaders.Get("sym-client"))
	}
}

func TestDetectColumnTypes(t *testing.T) {
	header := []string{"x", "y", "name", "flag"}
	rows := [][]string{
		{"1.5", "2", "alice", "1"},
		{"3.0", "4", "bob", "0"},
		{"5.5", "6", "charlie", "1"},
	}

	types := detectColumnTypes(header, rows)
	if types[0] != "C" {
		t.Errorf("x: got %q, want C", types[0])
	}
	if types[2] != "S" {
		t.Errorf("name: got %q, want S", types[2])
	}
	if types[3] != "B" {
		t.Errorf("flag: got %q, want B", types[3])
	}
}

func TestCSVRowsToSML(t *testing.T) {
	header := []string{"x", "name"}
	types := []string{"C", "S"}
	rows := [][]string{{"1.5", "alice"}, {"3.0", "bob"}}

	df := csvRowsToSML(header, types, rows)
	if len(df.Data) != 2 {
		t.Fatalf("Data rows: got %d, want 2", len(df.Data))
	}
	if df.Data[0][0] != 1.5 {
		t.Errorf("Data[0][0]: got %v, want 1.5", df.Data[0][0])
	}
	if df.Data[1][1] != "bob" {
		t.Errorf("Data[1][1]: got %v, want 'bob'", df.Data[1][1])
	}
}
