package pipeline

import (
	"regexp"
	"sort"
	"strings"

	"github.com/symetryml/godf"
	"github.com/symetryml/oteletl/classifier"
)

// FilterByLabels filters a raw metrics DataFrame by label values.
// labelFilters maps label_name → allowed values.
func FilterByLabels(df *godf.DataFrame, labelFilters map[string][]string) *godf.DataFrame {
	if df.Empty() || len(labelFilters) == 0 {
		return df
	}

	labelCol := df.Col("labels")
	mask := make([]bool, df.NRows())
	for i := 0; i < df.NRows(); i++ {
		mask[i] = true
	}

	for labelName, allowedValues := range labelFilters {
		allowed := make(map[string]bool, len(allowedValues))
		for _, v := range allowedValues {
			allowed[v] = true
		}
		for i := 0; i < df.NRows(); i++ {
			if !mask[i] {
				continue
			}
			labels, ok := labelCol.Any(i).(map[string]string)
			if !ok {
				mask[i] = false
				continue
			}
			val, exists := labels[labelName]
			if !exists || !allowed[val] {
				mask[i] = false
			}
		}
	}

	return df.Filter(mask)
}

// FilterByService filters a raw metrics DataFrame by service name(s).
func FilterByService(df *godf.DataFrame, services []string, serviceLabel string) *godf.DataFrame {
	if serviceLabel == "" {
		serviceLabel = "service_name"
	}
	return FilterByLabels(df, map[string][]string{serviceLabel: services})
}

// ExcludeByLabels excludes rows matching label values.
func ExcludeByLabels(df *godf.DataFrame, labelFilters map[string][]string) *godf.DataFrame {
	if df.Empty() || len(labelFilters) == 0 {
		return df
	}

	labelCol := df.Col("labels")
	mask := make([]bool, df.NRows())
	for i := 0; i < df.NRows(); i++ {
		mask[i] = true
	}

	for labelName, excludeValues := range labelFilters {
		excluded := make(map[string]bool, len(excludeValues))
		for _, v := range excludeValues {
			excluded[v] = true
		}
		for i := 0; i < df.NRows(); i++ {
			if !mask[i] {
				continue
			}
			labels, ok := labelCol.Any(i).(map[string]string)
			if !ok {
				continue
			}
			val, exists := labels[labelName]
			if exists && excluded[val] {
				mask[i] = false
			}
		}
	}

	return df.Filter(mask)
}

// FilterByMetrics filters a DataFrame by metric name regex patterns.
// If exclude is true, matching metrics are excluded instead of included.
func FilterByMetrics(df *godf.DataFrame, patterns []string, exclude bool) *godf.DataFrame {
	if df.Empty() || len(patterns) == 0 {
		return df
	}

	compiled := make([]*regexp.Regexp, len(patterns))
	for i, p := range patterns {
		compiled[i] = regexp.MustCompile(p)
	}

	metricCol := df.Col("metric")
	mask := make([]bool, df.NRows())

	for i := 0; i < df.NRows(); i++ {
		name := metricCol.Str(i)
		matches := false
		for _, re := range compiled {
			if re.MatchString(name) {
				matches = true
				break
			}
		}
		if exclude {
			mask[i] = !matches
		} else {
			mask[i] = matches
		}
	}

	return df.Filter(mask)
}

// GetAvailableServices returns sorted unique service names found in the DataFrame.
func GetAvailableServices(df *godf.DataFrame, serviceLabel string) []string {
	if serviceLabel == "" {
		serviceLabel = "service_name"
	}
	return GetLabelValues(df, serviceLabel)
}

// GetLabelValues returns sorted unique values for a given label.
func GetLabelValues(df *godf.DataFrame, labelName string) []string {
	if df.Empty() {
		return nil
	}

	labelCol := df.Col("labels")
	seen := make(map[string]bool)
	var result []string

	for i := 0; i < df.NRows(); i++ {
		labels, ok := labelCol.Any(i).(map[string]string)
		if !ok {
			continue
		}
		if val, exists := labels[labelName]; exists && !seen[val] {
			seen[val] = true
			result = append(result, val)
		}
	}

	sort.Strings(result)
	return result
}

// FilterSalientMetricsConfig holds configuration for salient metric filtering.
type FilterSalientMetricsConfig struct {
	PreferPatterns           []string
	DropPatterns             []string
	KeepOnePerGroup          bool
	KeepLatencyAndThroughput bool
}

// DefaultSalientConfig returns the default salient metrics filter configuration.
func DefaultSalientConfig() FilterSalientMetricsConfig {
	return FilterSalientMetricsConfig{
		PreferPatterns: []string{
			`duration`, `latency`,
			`calls_total$`, `requests_total`,
			`exceptions`, `errors`,
			`memory_usage`, `memory_used`,
			`gc_collections`,
		},
		DropPatterns: []string{
			`^target_info$`, `^target$`,
			`_size_bytes`, `_per_rpc`,
			`^go_memory_(?!used)`, `^go_config`, `^go_processor`,
			`^aspnetcore_`, `^dotnet_assembly`, `^dotnet_jit`, `^dotnet_monitor`,
			`^dotnet_thread_pool`, `^dotnet_timer`, `^dotnet_process_cpu`,
			`^jvm_class`, `^jvm_cpu_(?!recent_utilization)`, `^jvm_thread_count$`,
			`^nodejs_eventloop_(?!delay_(p90|p99))`, `^nodejs_eventloop_time`,
			`^v8js_memory_(?!heap_used)`,
			`^cpython_gc_(?!collections_total)`,
			`^process_cpu_count`, `^process_thread_count`, `^process_open_file`,
			`^process_context_switches`, `^process_runtime_`, `^process_disk`,
			`^system_cpu_(?!(utilization|load_average))`,
			`^system_disk_`, `^system_filesystem_`, `^system_paging_`,
			`^system_processes_`, `^system_swap_`, `^system_thread_count`,
			`^system_uptime`, `^system_memory_(?!usage)`,
			`^system_network_(?!(errors|dropped))`,
			`^kestrel_queued`, `^kestrel_active`,
			`^feature_flag_`,
			`^traces_span_metrics_`,
			`^otelcol_`, `^otlp_exporter`, `^processedLogs`, `^processedSpans`, `^queueSize`,
			`^container_`, `^httpcheck_`, `^nginx_`, `^jaeger_storage_`,
			`^kafka_consumer_(?!(records_lag|fetch_latency|records_consumed))`,
			`^kafka_controller`, `^kafka_isr`, `^kafka_leaderElection`,
			`^kafka_logs`, `^kafka_message_count`, `^kafka_network_io`,
			`^kafka_partition_(?!offline)`, `^kafka_purgatory`,
			`^kafka_request_(?!(time_99p|failed))`,
			`^postgresql_bgwriter`, `^postgresql_blks`, `^postgresql_blocks`,
			`^postgresql_database_count`, `^postgresql_index_(?!scans)`,
			`^postgresql_table_(?!size)`, `^postgresql_tup_`,
			`^redis_clients_(?!connected)`, `^redis_cpu`,
			`^redis_db_(?!keys)`, `^redis_keys_(?!evicted|expired)`,
			`^redis_keyspace_(?!hits|misses)`, `^redis_latest_fork`,
			`^redis_memory_(?!used)`, `^redis_net_`, `^redis_rdb_`,
			`^redis_replication_`, `^redis_slaves`, `^redis_uptime`,
		},
		KeepOnePerGroup:          true,
		KeepLatencyAndThroughput: true,
	}
}

// FilterSalientMetrics filters metrics to keep only the most salient ones per category.
func FilterSalientMetrics(metricNames []string, cfg FilterSalientMetricsConfig) []string {
	latencyPatterns := []*regexp.Regexp{
		regexp.MustCompile(`duration`),
		regexp.MustCompile(`latency`),
		regexp.MustCompile(`time_seconds`),
	}
	throughputPatterns := []*regexp.Regexp{
		regexp.MustCompile(`calls_total$`),
		regexp.MustCompile(`requests_total$`),
		regexp.MustCompile(`_total$`),
	}

	preferCompiled := make([]*regexp.Regexp, len(cfg.PreferPatterns))
	for i, p := range cfg.PreferPatterns {
		preferCompiled[i] = regexp.MustCompile(p)
	}

	dropRules := compileDropRules(cfg.DropPatterns)

	// Get unique metric families
	families := make(map[string][]string)
	for _, metric := range metricNames {
		family := classifier.ExtractMetricFamily(metric)
		families[family] = append(families[family], metric)
	}

	// Group families by broad prefix
	prefixGroups := make(map[string][]string)
	for family := range families {
		prefix := getBroadPrefix(family)
		prefixGroups[prefix] = append(prefixGroups[prefix], family)
	}

	// Sort prefix keys for deterministic tie-breaking
	prefixKeys := make([]string, 0, len(prefixGroups))
	for k := range prefixGroups {
		prefixKeys = append(prefixKeys, k)
	}
	sort.Strings(prefixKeys)

	// For each prefix group, select the most salient family
	selectedFamilies := make(map[string]bool)

	for _, prefix := range prefixKeys {
		groupFamilies := prefixGroups[prefix]
		sort.Strings(groupFamilies)
		// Filter out dropped families
		var kept []string
		for _, family := range groupFamilies {
			if !matchesDropRules(family, dropRules) {
				kept = append(kept, family)
			}
		}

		if len(kept) == 0 {
			continue
		}

		// Score by preference
		scoreFamily := func(family string) int {
			for i, re := range preferCompiled {
				if re.MatchString(family) {
					return i
				}
			}
			return len(preferCompiled) + 1
		}

		sort.SliceStable(kept, func(i, j int) bool {
			si, sj := scoreFamily(kept[i]), scoreFamily(kept[j])
			if si != sj {
				return si < sj
			}
			return kept[i] < kept[j]
		})

		if cfg.KeepOnePerGroup {
			selectedFamilies[kept[0]] = true

			if cfg.KeepLatencyAndThroughput {
				best := kept[0]
				isLatency := matchesAnyRe(best, latencyPatterns)
				isThroughput := matchesAnyRe(best, throughputPatterns)

				for _, family := range kept[1:] {
					if isLatency && matchesAnyRe(family, throughputPatterns) {
						selectedFamilies[family] = true
						break
					}
					if isThroughput && matchesAnyRe(family, latencyPatterns) {
						selectedFamilies[family] = true
						break
					}
				}
			}
		} else {
			anySelected := false
			for _, family := range kept {
				if scoreFamily(family) < len(preferCompiled) {
					selectedFamilies[family] = true
					anySelected = true
				}
			}
			if !anySelected {
				selectedFamilies[kept[0]] = true
			}
		}
	}

	// Return metrics belonging to selected families
	var result []string
	for _, metric := range metricNames {
		family := classifier.ExtractMetricFamily(metric)
		if selectedFamilies[family] {
			result = append(result, metric)
		}
	}
	return result
}

// dropRule handles patterns that may use negative lookahead (unsupported in Go).
// For patterns like `^go_memory_(?!used)`, we split into: match `^go_memory_` but exclude if also matches `^go_memory_used`.
type dropRule struct {
	match   *regexp.Regexp
	exclude *regexp.Regexp // nil if no negative lookahead
}

func compileDropRules(patterns []string) []dropRule {
	rules := make([]dropRule, 0, len(patterns))
	for _, p := range patterns {
		idx := strings.Index(p, "(?!")
		if idx >= 0 {
			// Find matching closing paren by counting depth
			depth := 1
			start := idx + 3 // after "(?!"
			end := -1
			for i := start; i < len(p); i++ {
				if p[i] == '(' {
					depth++
				} else if p[i] == ')' {
					depth--
					if depth == 0 {
						end = i
						break
					}
				}
			}
			if end < 0 {
				continue
			}
			matchPat := p[:idx]
			lookaheadContent := p[start:end]
			excludePat := matchPat + lookaheadContent

			matchRe, err := regexp.Compile(matchPat)
			if err != nil {
				continue
			}
			excludeRe, err := regexp.Compile(excludePat)
			if err != nil {
				rules = append(rules, dropRule{match: matchRe})
				continue
			}
			rules = append(rules, dropRule{match: matchRe, exclude: excludeRe})
		} else {
			re, err := regexp.Compile(p)
			if err != nil {
				continue
			}
			rules = append(rules, dropRule{match: re})
		}
	}
	return rules
}

func matchesDropRules(s string, rules []dropRule) bool {
	for _, r := range rules {
		if r.match.MatchString(s) {
			if r.exclude != nil && r.exclude.MatchString(s) {
				continue // excluded by negative lookahead
			}
			return true
		}
	}
	return false
}

func matchesAnyRe(s string, patterns []*regexp.Regexp) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

func getBroadPrefix(family string) string {
	parts := strings.Split(family, "_")
	if len(parts) == 1 {
		return family
	}

	first := parts[0]

	runtimePrefixes := map[string]bool{
		"go": true, "dotnet": true, "jvm": true, "nodejs": true, "v8js": true, "cpython": true,
		"process": true, "system": true,
		"kestrel": true, "aspnetcore": true,
		"traces": true, "feature": true,
		"postgresql": true, "redis": true, "nginx": true,
		"otelcol": true, "otlp": true, "jaeger": true,
		"container": true, "httpcheck": true,
		"gen": true,
	}

	if runtimePrefixes[first] {
		return first
	}

	twoWordPrefixes := map[string]bool{
		"rpc": true, "http": true, "grpc": true, "db": true, "dns": true, "kafka": true, "app": true,
	}

	if twoWordPrefixes[first] && len(parts) >= 2 {
		return parts[0] + "_" + parts[1]
	}

	if len(parts) >= 2 {
		return parts[0] + "_" + parts[1]
	}

	return family
}
