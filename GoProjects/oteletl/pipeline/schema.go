package pipeline

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// SchemaFile is the YAML schema file structure matching Python's SchemaConfig.
type SchemaFile struct {
	ProfiledAt             string                       `yaml:"profiled_at"`
	ProfilingWindowHours   float64                      `yaml:"profiling_window_hours"`
	CardinalityThresholds  schemaThresholds             `yaml:"cardinality_thresholds"`
	Metrics                map[string]schemaMetricEntry `yaml:"metrics"`
}

type schemaThresholds struct {
	Tier1Max int `yaml:"tier1_max"`
	Tier2Max int `yaml:"tier2_max"`
	Tier3Max int `yaml:"tier3_max"`
}

type schemaMetricEntry struct {
	Type   string                      `yaml:"type"`
	Labels map[string]schemaLabelEntry `yaml:"labels"`
}

type schemaLabelEntry struct {
	Tier             int      `yaml:"tier"`
	Cardinality      int      `yaml:"cardinality"`
	Action           string   `yaml:"action"`
	BucketType       *string  `yaml:"bucket_type"`
	TopValues        []string `yaml:"top_values"`
	SemanticCategory string   `yaml:"semantic_category"`
}

// LoadSchemaFile loads a schema YAML file and converts it to the pipeline's
// SchemaConfig format (map[string]MetricSchema).
func LoadSchemaFile(path string) (map[string]MetricSchema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read schema file: %w", err)
	}

	var sf SchemaFile
	if err := yaml.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("parse schema YAML: %w", err)
	}

	return schemaFileToConfig(&sf), nil
}

// SaveSchemaFile saves a SchemaConfig to a YAML file.
func SaveSchemaFile(schema map[string]MetricSchema, path string) error {
	sf := configToSchemaFile(schema)
	data, err := yaml.Marshal(sf)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}
	return os.WriteFile(path, data, 0644)
}

func schemaFileToConfig(sf *SchemaFile) map[string]MetricSchema {
	result := make(map[string]MetricSchema, len(sf.Metrics))

	for metricName, me := range sf.Metrics {
		ms := MetricSchema{
			Labels: make(map[string]LabelSchema, len(me.Labels)),
		}

		for labelName, le := range me.Labels {
			ls := LabelSchema{
				Action: le.Action,
			}
			if le.BucketType != nil {
				ls.BucketType = *le.BucketType
			}
			if le.TopValues != nil {
				ls.TopValues = le.TopValues
			}
			ms.Labels[labelName] = ls
		}

		result[metricName] = ms
	}

	return result
}

func configToSchemaFile(config map[string]MetricSchema) *SchemaFile {
	sf := &SchemaFile{
		Metrics: make(map[string]schemaMetricEntry, len(config)),
	}

	for metricName, ms := range config {
		me := schemaMetricEntry{
			Labels: make(map[string]schemaLabelEntry, len(ms.Labels)),
		}

		for labelName, ls := range ms.Labels {
			le := schemaLabelEntry{
				Action: ls.Action,
			}
			if ls.BucketType != "" {
				bt := ls.BucketType
				le.BucketType = &bt
			}
			if ls.TopValues != nil {
				le.TopValues = ls.TopValues
			}
			me.Labels[labelName] = le
		}

		sf.Metrics[metricName] = me
	}

	return sf
}
