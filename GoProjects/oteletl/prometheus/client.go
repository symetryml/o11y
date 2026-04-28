// Package prometheus provides a Prometheus HTTP API client and DataFrame-based
// metric fetching/windowing utilities.
package prometheus

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client wraps the Prometheus HTTP API.
type Client struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewClient creates a Client with sensible defaults.
func NewClient(baseURL string) *Client {
	return &Client{
		BaseURL:    strings.TrimRight(baseURL, "/"),
		HTTPClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// apiResponse is the generic Prometheus API envelope.
type apiResponse struct {
	Status string          `json:"status"`
	Data   json.RawMessage `json:"data"`
	Error  string          `json:"error"`
}

func (c *Client) get(endpoint string, params url.Values) (json.RawMessage, error) {
	u := c.BaseURL + endpoint
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	resp, err := c.HTTPClient.Get(u)
	if err != nil {
		return nil, fmt.Errorf("prometheus request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading prometheus response: %w", err)
	}

	var ar apiResponse
	if err := json.Unmarshal(body, &ar); err != nil {
		return nil, fmt.Errorf("parsing prometheus response: %w", err)
	}
	if ar.Status != "success" {
		return nil, fmt.Errorf("prometheus query failed: %s", ar.Error)
	}

	return ar.Data, nil
}

// GetMetricNames returns all metric names (/api/v1/label/__name__/values).
func (c *Client) GetMetricNames() ([]string, error) {
	data, err := c.get("/api/v1/label/__name__/values", nil)
	if err != nil {
		return nil, err
	}
	var names []string
	return names, json.Unmarshal(data, &names)
}

// queryResult is the structure for instant query results.
type queryResult struct {
	ResultType string           `json:"resultType"`
	Result     []instantVector  `json:"result"`
}

type instantVector struct {
	Metric map[string]string `json:"metric"`
	Value  [2]json.RawMessage `json:"value"`
}

type rangeVector struct {
	Metric map[string]string  `json:"metric"`
	Values [][2]json.RawMessage `json:"values"`
}

type rangeResult struct {
	ResultType string        `json:"resultType"`
	Result     []rangeVector `json:"result"`
}

// QueryRange executes a range query and returns raw results.
func (c *Client) QueryRange(promql string, start, end time.Time, step string) ([]rangeVector, error) {
	params := url.Values{
		"query": {promql},
		"start": {formatTimestamp(start)},
		"end":   {formatTimestamp(end)},
		"step":  {step},
	}
	data, err := c.get("/api/v1/query_range", params)
	if err != nil {
		return nil, err
	}
	var rr rangeResult
	if err := json.Unmarshal(data, &rr); err != nil {
		return nil, err
	}
	return rr.Result, nil
}

// Query executes an instant query.
func (c *Client) Query(promql string) ([]instantVector, error) {
	params := url.Values{"query": {promql}}
	data, err := c.get("/api/v1/query", params)
	if err != nil {
		return nil, err
	}
	var qr queryResult
	if err := json.Unmarshal(data, &qr); err != nil {
		return nil, err
	}
	return qr.Result, nil
}

// MetricRow is a single data point in the standard (timestamp, metric, labels, value) format.
type MetricRow struct {
	Timestamp time.Time
	Metric    string
	Labels    map[string]string
	Value     float64
}

// FetchMetricsRange fetches multiple metrics over a time range from the Prometheus API.
func (c *Client) FetchMetricsRange(metricNames []string, start, end time.Time, step string) ([]MetricRow, error) {
	var rows []MetricRow

	for _, name := range metricNames {
		result, err := c.QueryRange(name, start, end, step)
		if err != nil {
			continue // skip failures like Python version
		}
		for _, series := range result {
			labels := make(map[string]string)
			actualName := name
			for k, v := range series.Metric {
				if k == "__name__" {
					actualName = v
				} else {
					labels[k] = v
				}
			}
			for _, pair := range series.Values {
				ts, val := parseValuePair(pair)
				rows = append(rows, MetricRow{
					Timestamp: ts,
					Metric:    actualName,
					Labels:    labels,
					Value:     val,
				})
			}
		}
	}

	return rows, nil
}

// GetScrapeInterval fetches the global scrape_interval from Prometheus config.
func (c *Client) GetScrapeInterval() (int, error) {
	data, err := c.get("/api/v1/status/config", nil)
	if err != nil {
		return 60, err
	}

	var cfg struct {
		YAML string `json:"yaml"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return 60, err
	}

	// Simple parse of scrape_interval from YAML text
	for _, line := range strings.Split(cfg.YAML, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "scrape_interval:") {
			val := strings.TrimSpace(strings.TrimPrefix(line, "scrape_interval:"))
			return parseDuration(val), nil
		}
	}

	return 60, nil
}

// GetMetricsDataframe2 fetches all metrics and their services in one request.
// Returns rows of (service, metric, type, subtype).
func (c *Client) GetMetricsDataframe2() ([]MetricInfo, error) {
	now := time.Now()
	params := url.Values{
		"match[]": {`{__name__=~".+"}`},
		"start":   {fmt.Sprintf("%d", now.Add(-5*time.Minute).Unix())},
		"end":     {fmt.Sprintf("%d", now.Unix())},
	}

	data, err := c.get("/api/v1/series", params)
	if err != nil {
		return nil, err
	}

	var seriesList []map[string]string
	if err := json.Unmarshal(data, &seriesList); err != nil {
		return nil, err
	}

	seen := make(map[string]bool)
	var result []MetricInfo

	for _, labels := range seriesList {
		metric := labels["__name__"]
		service := firstOf(labels, "service_name", "service", "job", "container")
		if service == "" {
			service = "unknown"
		}

		key := service + "|" + metric
		if seen[key] {
			continue
		}
		seen[key] = true

		mType, mSubtype := detectMetricType(metric, labels)
		result = append(result, MetricInfo{
			Service: service,
			Metric:  metric,
			Type:    mType,
			Subtype: mSubtype,
		})
	}

	return result, nil
}

// MetricInfo holds metadata about a discovered metric.
type MetricInfo struct {
	Service string
	Metric  string
	Type    string
	Subtype string
}

// --- Helpers ---

func formatTimestamp(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func parseValuePair(pair [2]json.RawMessage) (time.Time, float64) {
	var tsFloat float64
	json.Unmarshal(pair[0], &tsFloat)
	ts := time.Unix(int64(tsFloat), int64((tsFloat-float64(int64(tsFloat)))*1e9))

	var valStr string
	json.Unmarshal(pair[1], &valStr)
	val := math.NaN()
	if valStr != "NaN" {
		fmt.Sscanf(valStr, "%f", &val)
	}
	return ts, val
}

func parseDuration(s string) int {
	s = strings.TrimSpace(s)
	if strings.HasSuffix(s, "s") {
		var v int
		fmt.Sscanf(s, "%ds", &v)
		return v
	}
	if strings.HasSuffix(s, "m") {
		var v int
		fmt.Sscanf(s, "%dm", &v)
		return v * 60
	}
	return 60
}

func firstOf(m map[string]string, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != "" {
			return v
		}
	}
	return ""
}

func detectMetricType(name string, labels map[string]string) (string, string) {
	switch {
	case strings.HasSuffix(name, "_bucket"):
		return "histogram", "bucket"
	case strings.HasSuffix(name, "_count"):
		return "histogram", "count"
	case strings.HasSuffix(name, "_sum"):
		return "histogram", "sum"
	case strings.HasSuffix(name, "_total"):
		return "counter", ""
	}
	if _, ok := labels["le"]; ok {
		return "histogram", "bucket"
	}
	if _, ok := labels["quantile"]; ok {
		return "summary", "quantile"
	}
	return "gauge", ""
}
