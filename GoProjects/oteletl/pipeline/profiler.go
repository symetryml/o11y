package pipeline

import (
	"fmt"
	"sort"

	"github.com/symetryml/godf"
	"github.com/symetryml/oteletl/classifier"
)

// ProfileResult holds the output of RunProfilerFromDataFrame.
type ProfileResult struct {
	Families          map[string]*FamilyProfile
	TotalFamilies     int
	TotalMetrics      int
	TotalRows         int
}

// FamilyProfile holds profile data for a metric family.
type FamilyProfile struct {
	Name    string
	Type    string
	Metrics []string
	Labels  map[string]*LabelProfile
}

// LabelProfile holds cardinality and value data for a label.
type LabelProfile struct {
	Label       string
	Cardinality int
	Tier        int
	Action      string
	TopValues   []string // Only populated for top_n action
}

// CardinalityThresholds defines tier boundaries.
type CardinalityThresholds struct {
	Tier1Max int // 1-N: always keep
	Tier2Max int // N+1-M: bucket
	Tier3Max int // M+1-P: top-N only
	// >P: drop
}

// DefaultCardinalityThresholds returns standard tier boundaries.
func DefaultCardinalityThresholds() CardinalityThresholds {
	return CardinalityThresholds{
		Tier1Max: 10,
		Tier2Max: 50,
		Tier3Max: 200,
	}
}

// RunProfilerFromDataFrame analyses an in-memory DataFrame to generate
// a profile of metric families, label cardinalities, and recommended actions.
//
// This is the Go equivalent of Python's run_profiler_from_dataframe().
//
// df must have columns: timestamp, metric (String), labels (Any - map[string]string), value (Float64).
func RunProfilerFromDataFrame(
	df *godf.DataFrame,
	thresholds CardinalityThresholds,
	topN int,
) (*ProfileResult, error) {
	if df.Empty() {
		return nil, fmt.Errorf("cannot profile an empty DataFrame")
	}

	if topN <= 0 {
		topN = 20
	}

	metricCol := df.Col("metric")
	labelCol := df.Col("labels")
	nRows := df.NRows()

	// Collect label values per (metric, label_key)
	type counter map[string]int
	metricLabels := make(map[string]map[string]counter) // metric → label → value → count
	metricNames := make(map[string]bool)

	for i := 0; i < nRows; i++ {
		metric := metricCol.Str(i)
		metricNames[metric] = true

		labels, ok := labelCol.Any(i).(map[string]string)
		if !ok {
			continue
		}

		if _, exists := metricLabels[metric]; !exists {
			metricLabels[metric] = make(map[string]counter)
		}

		for k, v := range labels {
			if _, exists := metricLabels[metric][k]; !exists {
				metricLabels[metric][k] = make(counter)
			}
			metricLabels[metric][k][v]++
		}
	}

	// Build families
	families := make(map[string]*FamilyProfile)
	for metricName := range metricNames {
		familyName := classifier.ExtractMetricFamily(metricName)
		otelType := classifier.ClassifyMetricType(metricName)

		if _, exists := families[familyName]; !exists {
			families[familyName] = &FamilyProfile{
				Name:   familyName,
				Type:   otelType,
				Labels: make(map[string]*LabelProfile),
			}
		}
		families[familyName].Metrics = append(families[familyName].Metrics, metricName)
	}

	// Build label profiles per family
	for familyName, family := range families {
		// Merge labels across all metrics in this family
		mergedLabels := make(map[string]counter)
		for _, metricName := range family.Metrics {
			if ml, ok := metricLabels[metricName]; ok {
				for labelKey, valueCounts := range ml {
					if _, exists := mergedLabels[labelKey]; !exists {
						mergedLabels[labelKey] = make(counter)
					}
					for v, c := range valueCounts {
						mergedLabels[labelKey][v] += c
					}
				}
			}
		}

		for labelKey, valueCounts := range mergedLabels {
			cardinality := len(valueCounts)
			tier := getTier(cardinality, thresholds)
			action := getAction(tier)

			var topValues []string
			if action == "top_n" {
				topValues = topNValues(valueCounts, topN)
			}

			families[familyName].Labels[labelKey] = &LabelProfile{
				Label:       labelKey,
				Cardinality: cardinality,
				Tier:        tier,
				Action:      action,
				TopValues:   topValues,
			}
		}
	}

	return &ProfileResult{
		Families:      families,
		TotalFamilies: len(families),
		TotalMetrics:  len(metricNames),
		TotalRows:     nRows,
	}, nil
}

// ToSchemaConfig converts a ProfileResult into a pipeline Config's SchemaConfig.
func (pr *ProfileResult) ToSchemaConfig() map[string]MetricSchema {
	schema := make(map[string]MetricSchema, len(pr.Families))

	for familyName, family := range pr.Families {
		ms := MetricSchema{Labels: make(map[string]LabelSchema)}

		for labelName, lp := range family.Labels {
			// Use semantic classifier to determine bucket type
			c := classifier.ClassifyLabel(labelName)

			ls := LabelSchema{
				Action:     lp.Action,
				BucketType: c.BucketType,
			}
			if lp.Action == "top_n" {
				ls.TopValues = lp.TopValues
			}

			ms.Labels[labelName] = ls
		}

		schema[familyName] = ms
	}

	return schema
}

func getTier(cardinality int, t CardinalityThresholds) int {
	if cardinality <= t.Tier1Max {
		return 1
	}
	if cardinality <= t.Tier2Max {
		return 2
	}
	if cardinality <= t.Tier3Max {
		return 3
	}
	return 4
}

func getAction(tier int) string {
	switch tier {
	case 1:
		return "keep"
	case 2:
		return "bucket"
	case 3:
		return "top_n"
	default:
		return "drop"
	}
}

func topNValues(counts map[string]int, n int) []string {
	type kv struct {
		key   string
		count int
	}
	pairs := make([]kv, 0, len(counts))
	for k, v := range counts {
		pairs = append(pairs, kv{k, v})
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].count > pairs[j].count
	})

	result := make([]string, 0, n)
	for i, p := range pairs {
		if i >= n {
			break
		}
		result = append(result, p.key)
	}
	return result
}
