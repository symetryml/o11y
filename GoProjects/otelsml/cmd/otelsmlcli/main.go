// otelsmlcli is a standalone CLI for the SymetryML OTel metrics pipeline.
//
// It replicates the Python workflow:
//
//	df_metrics = get_metrics_dataframe2(prometheus_url)
//	the_metrics = filter_salient_metrics(metrics_for_service)
//	raw_df = filter_by_service(df0, [the_service])
//	for window_start, window_end, window_df in iter_metrics_windows(raw_df, ...):
//	    features_df = denormalize_metrics(window_df, ...)
package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/symetryml/demclient"
	"github.com/symetryml/godf"
	"github.com/symetryml/oteletl/pipeline"
	"github.com/symetryml/oteletl/prometheus"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	args := os.Args[2:]

	switch cmd {
	case "run":
		cmdRun(args)
	case "discover":
		cmdDiscover(args)
	case "profile":
		cmdProfile(args)
	default:
		fmt.Fprintf(os.Stderr, "unknown command: %s\n", cmd)
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `Usage: otelsmlcli <command> [flags]

Commands:
  run       Full pipeline: fetch → discover → filter → window → denormalize → output
  discover  List available services and metrics from Prometheus
  profile   Run profiler and emit schema YAML

Run "otelsmlcli <command> -help" for command-specific flags.
`)
}

// --- Flags helpers ---

func envOrDefault(envKey, def string) string {
	if v := os.Getenv(envKey); v != "" {
		return v
	}
	return def
}

func splitComma(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// --- run command ---

func cmdRun(args []string) {
	prometheusURL := flagStr(args, "-prometheus-url", envOrDefault("SML_PROMETHEUS_URL", ""))
	inputFile := flagStr(args, "-input-file", "")
	service := flagStr(args, "-service", envOrDefault("SML_SERVICES", ""))
	serviceLabel := flagStr(args, "-service-label", envOrDefault("SML_SERVICE_LABEL", "service_name"))
	schemaPath := flagStr(args, "-schema-path", envOrDefault("SML_SCHEMA_PATH", ""))
	windowSeconds := flagFloat(args, "-window-seconds", envFloat("SML_WINDOW_SECONDS", 60))
	windowMinutes := flagInt(args, "-window-minutes", 5)
	step := flagStr(args, "-step", "60s")
	includeDeltasStr := flagStr(args, "-include-deltas", envOrDefault("SML_INCLUDE_DELTAS", "true"))
	includeDeltas := includeDeltasStr == "true" || includeDeltasStr == "1"
	entityLabels := splitComma(flagStr(args, "-entity-labels", envOrDefault("SML_ENTITY_LABELS", "service_name")))
	forceDropLabels := splitComma(flagStr(args, "-force-drop-labels", envOrDefault("SML_FORCE_DROP_LABELS", "")))
	output := flagStr(args, "-output", "json")
	demEndpoint := flagStr(args, "-dem-endpoint", envOrDefault("SML_SERVER", ""))
	demProject := flagStr(args, "-dem-project", envOrDefault("SML_PROJECT_NAME", ""))

	if prometheusURL == "" && inputFile == "" {
		fatal("one of -prometheus-url or -input-file is required")
	}

	// Step 1: Load raw data and discover metrics for the service
	var rawDF *godf.DataFrame
	var theMetrics []string

	if inputFile != "" {
		// MODE B: Raw CSV file
		rawDF = loadCSV(inputFile)
		fmt.Fprintf(os.Stderr, "Loaded %d rows from %s\n", rawDF.NRows(), inputFile)

		// Extract unique metric names
		metricCol := rawDF.Col("metric")
		metricSet := make(map[string]bool)
		for i := 0; i < rawDF.NRows(); i++ {
			metricSet[metricCol.Str(i)] = true
		}

		// If service specified, get only metrics for that service
		if service != "" {
			filtered := pipeline.FilterByService(rawDF, []string{service}, serviceLabel)
			metricSet = make(map[string]bool)
			fMetricCol := filtered.Col("metric")
			for i := 0; i < filtered.NRows(); i++ {
				metricSet[fMetricCol.Str(i)] = true
			}
		}

		allMetrics := make([]string, 0, len(metricSet))
		for m := range metricSet {
			allMetrics = append(allMetrics, m)
		}
		sort.Strings(allMetrics)
		theMetrics = pipeline.FilterSalientMetrics(allMetrics, pipeline.DefaultSalientConfig())
		fmt.Fprintf(os.Stderr, "Salient metrics: %d (from %d total)\n", len(theMetrics), len(allMetrics))
	} else {
		// MODE A: Live Prometheus
		client := prometheus.NewClient(prometheusURL)

		// Step 1: get_metrics_dataframe2
		metricInfos, err := client.GetMetricsDataframe2()
		if err != nil {
			fatal("GetMetricsDataframe2: %v", err)
		}
		fmt.Fprintf(os.Stderr, "Discovered %d metric/service pairs\n", len(metricInfos))

		// Filter by service to get metric names
		var serviceMetrics []string
		metricSet := make(map[string]bool)
		for _, mi := range metricInfos {
			if service != "" && mi.Service != service {
				continue
			}
			if !metricSet[mi.Metric] {
				metricSet[mi.Metric] = true
				serviceMetrics = append(serviceMetrics, mi.Metric)
			}
		}
		sort.Strings(serviceMetrics)

		// filter_salient_metrics
		theMetrics = pipeline.FilterSalientMetrics(serviceMetrics, pipeline.DefaultSalientConfig())
		fmt.Fprintf(os.Stderr, "Salient metrics for %q: %d (from %d)\n", service, len(theMetrics), len(serviceMetrics))

		// Fetch raw data from Prometheus
		now := time.Now()
		lookback := time.Duration(windowMinutes) * time.Minute
		start := now.Add(-lookback)

		rows, err := client.FetchMetricsRange(theMetrics, start, now, step)
		if err != nil {
			fatal("FetchMetricsRange: %v", err)
		}
		fmt.Fprintf(os.Stderr, "Fetched %d raw rows from Prometheus\n", len(rows))

		// Convert MetricRow → DataFrame
		rawDF = metricRowsToDF(rows)
	}

	// Step 2: filter_by_service
	if service != "" {
		rawDF = pipeline.FilterByService(rawDF, []string{service}, serviceLabel)
		if rawDF.Empty() {
			fatal("no data after filtering for service %q", service)
		}
		fmt.Fprintf(os.Stderr, "After service filter: %d rows\n", rawDF.NRows())
	}

	// Step 3: Build pipeline config
	cfg := pipeline.Config{
		WindowSeconds:   windowSeconds,
		IncludeDeltas:   includeDeltas,
		DeltaWindows:    []int{5, 60},
		EntityLabels:    entityLabels,
		ForceDropLabels: forceDropLabels,
		SchemaPath:      schemaPath,
	}

	// Step 4: iter_metrics_windows → denormalize_metrics
	windows := prometheus.IterMetricsWindows(rawDF, theMetrics, windowMinutes, step)
	fmt.Fprintf(os.Stderr, "Processing %d windows\n", len(windows))

	// Set up DEM client if needed
	var demClient *demclient.Client
	if output == "dem" {
		if demEndpoint == "" || demProject == "" {
			fatal("-dem-endpoint and -dem-project required for dem output")
		}
		demClient = demclient.NewClient(demclient.Config{
			Server:       demEndpoint,
			SymKeyID:     os.Getenv("SML_KEY_ID"),
			SymSecretKey: os.Getenv("SML_SECRET_KEY"),
		})
	}

	csvHeaderWritten := false
	for i, w := range windows {
		wide := pipeline.DenormalizeMetrics(w.DF, cfg)
		if wide.Empty() {
			fmt.Fprintf(os.Stderr, "  Window %d [%s → %s]: empty\n", i+1, w.Start.Format(time.RFC3339), w.End.Format(time.RFC3339))
			continue
		}
		fmt.Fprintf(os.Stderr, "  Window %d [%s → %s]: %d rows × %d cols\n",
			i+1, w.Start.Format(time.RFC3339), w.End.Format(time.RFC3339), wide.NRows(), len(wide.Columns()))

		switch output {
		case "json":
			outputJSON(wide)
		case "csv":
			csvHeaderWritten = outputCSV(wide, csvHeaderWritten)
		case "dem":
			if err := streamToDEM(demClient, demProject, wide); err != nil {
				fmt.Fprintf(os.Stderr, "  DEM stream error: %v\n", err)
			}
		default:
			fatal("unknown output format: %s", output)
		}
	}
}

// --- discover command ---

func cmdDiscover(args []string) {
	prometheusURL := flagStr(args, "-prometheus-url", envOrDefault("SML_PROMETHEUS_URL", ""))
	if prometheusURL == "" {
		fatal("-prometheus-url is required for discover")
	}

	client := prometheus.NewClient(prometheusURL)
	metricInfos, err := client.GetMetricsDataframe2()
	if err != nil {
		fatal("GetMetricsDataframe2: %v", err)
	}

	// Group by service
	byService := make(map[string][]string)
	for _, mi := range metricInfos {
		byService[mi.Service] = append(byService[mi.Service], mi.Metric)
	}

	services := make([]string, 0, len(byService))
	for s := range byService {
		services = append(services, s)
	}
	sort.Strings(services)

	for _, svc := range services {
		metrics := byService[svc]
		sort.Strings(metrics)
		fmt.Printf("\n=== %s (%d metrics) ===\n", svc, len(metrics))
		for _, m := range metrics {
			fmt.Printf("  %s\n", m)
		}
	}
}

// --- profile command ---

func cmdProfile(args []string) {
	prometheusURL := flagStr(args, "-prometheus-url", envOrDefault("SML_PROMETHEUS_URL", ""))
	inputFile := flagStr(args, "-input-file", "")
	outputPath := flagStr(args, "-output-path", "")
	service := flagStr(args, "-service", envOrDefault("SML_SERVICES", ""))
	serviceLabel := flagStr(args, "-service-label", envOrDefault("SML_SERVICE_LABEL", "service_name"))
	step := flagStr(args, "-step", "60s")
	windowMinutes := flagInt(args, "-window-minutes", 5)

	if prometheusURL == "" && inputFile == "" {
		fatal("one of -prometheus-url or -input-file is required for profile")
	}

	var rawDF *godf.DataFrame

	if inputFile != "" {
		rawDF = loadCSV(inputFile)
	} else {
		client := prometheus.NewClient(prometheusURL)
		metricInfos, err := client.GetMetricsDataframe2()
		if err != nil {
			fatal("GetMetricsDataframe2: %v", err)
		}

		var metricNames []string
		seen := make(map[string]bool)
		for _, mi := range metricInfos {
			if service != "" && mi.Service != service {
				continue
			}
			if !seen[mi.Metric] {
				seen[mi.Metric] = true
				metricNames = append(metricNames, mi.Metric)
			}
		}

		now := time.Now()
		start := now.Add(-time.Duration(windowMinutes) * time.Minute)
		rows, err := client.FetchMetricsRange(metricNames, start, now, step)
		if err != nil {
			fatal("FetchMetricsRange: %v", err)
		}
		rawDF = metricRowsToDF(rows)
	}

	if service != "" {
		rawDF = pipeline.FilterByService(rawDF, []string{service}, serviceLabel)
	}

	result, err := pipeline.RunProfilerFromDataFrame(rawDF, pipeline.DefaultCardinalityThresholds(), 20)
	if err != nil {
		fatal("RunProfilerFromDataFrame: %v", err)
	}
	schema := result.ToSchemaConfig()

	if outputPath == "" {
		// Write to stdout
		if err := pipeline.SaveSchemaFile(schema, "/dev/stdout"); err != nil {
			fatal("SaveSchemaFile: %v", err)
		}
	} else {
		if err := pipeline.SaveSchemaFile(schema, outputPath); err != nil {
			fatal("SaveSchemaFile: %v", err)
		}
		fmt.Fprintf(os.Stderr, "Schema written to %s\n", outputPath)
	}
}

// --- CSV loading ---

// loadCSV reads a raw metrics CSV file with columns: timestamp, metric, labels, value.
// The labels column is a stringified Python dict: {'key': 'val', ...}
func loadCSV(path string) *godf.DataFrame {
	f, err := os.Open(path)
	if err != nil {
		fatal("open %s: %v", path, err)
	}
	defer f.Close()

	reader := csv.NewReader(f)
	reader.LazyQuotes = true

	header, err := reader.Read()
	if err != nil {
		fatal("read header: %v", err)
	}

	// Find column indices
	colIdx := make(map[string]int)
	for i, h := range header {
		colIdx[strings.TrimSpace(h)] = i
	}
	for _, required := range []string{"timestamp", "metric", "labels", "value"} {
		if _, ok := colIdx[required]; !ok {
			fatal("CSV missing required column: %s (have: %v)", required, header)
		}
	}

	var records []map[string]any
	lineNum := 1
	for {
		row, err := reader.Read()
		if err != nil {
			break
		}
		lineNum++

		val, err := strconv.ParseFloat(row[colIdx["value"]], 64)
		if err != nil {
			continue // skip unparseable values
		}

		labels := parsePythonDict(row[colIdx["labels"]])

		// Normalize timestamp: "2026-01-22 00:00:00" → "2026-01-22T00:00:00"
		ts := row[colIdx["timestamp"]]
		if len(ts) == 19 && ts[10] == ' ' {
			ts = ts[:10] + "T" + ts[11:]
		}

		records = append(records, map[string]any{
			"timestamp": ts,
			"metric":    row[colIdx["metric"]],
			"labels":    labels,
			"value":     val,
		})
	}

	if len(records) == 0 {
		fatal("CSV file %s has no valid data rows", path)
	}

	return godf.NewDataFrame(records)
}

// parsePythonDict converts a Python dict string like {'key': 'val'} to map[string]string.
func parsePythonDict(s string) map[string]string {
	s = strings.TrimSpace(s)
	if s == "" || s == "{}" {
		return map[string]string{}
	}

	// Replace single quotes with double quotes for JSON parsing
	jsonStr := strings.ReplaceAll(s, "'", "\"")

	var result map[string]string
	if err := json.Unmarshal([]byte(jsonStr), &result); err != nil {
		// Fallback: return empty map
		return map[string]string{}
	}
	return result
}

// --- Prometheus helpers ---

func metricRowsToDF(rows []prometheus.MetricRow) *godf.DataFrame {
	records := make([]map[string]any, len(rows))
	for i, r := range rows {
		records[i] = map[string]any{
			"timestamp": r.Timestamp.Format("2006-01-02T15:04:05"),
			"metric":    r.Metric,
			"labels":    r.Labels,
			"value":     r.Value,
		}
	}
	return godf.NewDataFrame(records)
}

// --- Output helpers ---

func outputJSON(wide *godf.DataFrame) {
	records := wide.Records()
	enc := json.NewEncoder(os.Stdout)
	for _, rec := range records {
		// Remove NaN values
		clean := make(map[string]any, len(rec))
		for k, v := range rec {
			if f, ok := v.(float64); ok && math.IsNaN(f) {
				continue
			}
			clean[k] = v
		}
		enc.Encode(clean)
	}
}

func outputCSV(wide *godf.DataFrame, headerWritten bool) bool {
	cols := wide.Columns()
	if len(cols) == 0 {
		return headerWritten
	}

	w := csv.NewWriter(os.Stdout)
	defer w.Flush()

	if !headerWritten {
		w.Write(cols)
	}

	for i := 0; i < wide.NRows(); i++ {
		row := make([]string, len(cols))
		rec := wide.ILoc(i)
		for j, col := range cols {
			v := rec[col]
			switch val := v.(type) {
			case float64:
				if math.IsNaN(val) {
					row[j] = ""
				} else {
					row[j] = strconv.FormatFloat(val, 'f', -1, 64)
				}
			case nil:
				row[j] = ""
			default:
				row[j] = fmt.Sprintf("%v", val)
			}
		}
		w.Write(row)
	}

	return true
}

func streamToDEM(client *demclient.Client, project string, wide *godf.DataFrame) error {
	cols := wide.Columns()
	records := wide.Records()

	if len(records) == 0 {
		return nil
	}

	// Build attribute names/types and data
	attrNames := make([]string, len(cols))
	attrTypes := make([]string, len(cols))
	for i, col := range cols {
		attrNames[i] = col
		// Determine type from first non-nil value
		attrTypes[i] = "C" // default to continuous
		for _, rec := range records {
			v := rec[col]
			if v == nil {
				continue
			}
			switch v.(type) {
			case float64:
				attrTypes[i] = "C"
			case string:
				attrTypes[i] = "S"
			default:
				attrTypes[i] = "S"
			}
			break
		}
	}

	data := make([][]any, len(records))
	for i, rec := range records {
		row := make([]any, len(cols))
		for j, col := range cols {
			v := rec[col]
			if f, ok := v.(float64); ok && math.IsNaN(f) {
				row[j] = nil
			} else {
				row[j] = v
			}
		}
		data[i] = row
	}

	smlDF := &demclient.SMLDataFrame{
		AttributeNames: attrNames,
		AttributeTypes: attrTypes,
		Data:           data,
	}

	return client.StreamData(project, smlDF)
}

// --- Simple flag parsing (no flag package, supports intermixed flags) ---

func flagStr(args []string, name, def string) string {
	for i, a := range args {
		if a == name && i+1 < len(args) {
			return args[i+1]
		}
		if strings.HasPrefix(a, name+"=") {
			return strings.TrimPrefix(a, name+"=")
		}
	}
	return def
}

func flagInt(args []string, name string, def int) int {
	s := flagStr(args, name, "")
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func flagFloat(args []string, name string, def float64) float64 {
	s := flagStr(args, name, "")
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func envFloat(key string, def float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return v
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "Error: "+format+"\n", args...)
	os.Exit(1)
}
