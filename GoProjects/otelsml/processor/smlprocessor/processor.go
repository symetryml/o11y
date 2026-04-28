package smlprocessor

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"go.uber.org/zap"

	"github.com/symetryml/godf"
	"github.com/symetryml/oteletl/classifier"
	"github.com/symetryml/oteletl/pipeline"
)

func (p *smlProcessor) shouldSanitize() bool {
	return p.cfg.SanitizeNames == nil || *p.cfg.SanitizeNames
}

type smlProcessor struct {
	cfg         *Config
	next        consumer.Metrics
	settings    component.TelemetrySettings
	pipelineCfg pipeline.Config // resolved once at Start()
}

func newProcessor(cfg *Config, next consumer.Metrics, settings component.TelemetrySettings) *smlProcessor {
	return &smlProcessor{
		cfg:      cfg,
		next:     next,
		settings: settings,
	}
}

func (p *smlProcessor) Start(_ context.Context, _ component.Host) error {
	// Apply env var overrides (env takes precedence over YAML config)
	applyEnvOverrides(p.cfg)

	p.settings.Logger.Info("smlprocessor config",
		zap.Bool("sanitize_names", p.shouldSanitize()),
		zap.Strings("services", p.cfg.Services),
		zap.String("service_label", p.cfg.ServiceLabel),
		zap.String("schema_path", p.cfg.SchemaPath),
	)

	// Build pipeline config once
	pcfg := pipeline.Config{
		WindowSeconds:   p.cfg.WindowSeconds,
		IncludeDeltas:   p.cfg.IncludeDeltas,
		DeltaWindows:    p.cfg.DeltaWindows,
		ForceDropLabels: p.cfg.ForceDropLabels,
		EntityLabels:    p.cfg.EntityLabels,
	}

	// Load schema file if configured
	if p.cfg.SchemaPath != "" {
		schema, err := pipeline.LoadSchemaFile(p.cfg.SchemaPath)
		if err != nil {
			return fmt.Errorf("load schema %q: %w", p.cfg.SchemaPath, err)
		}
		pcfg.SchemaConfig = schema
	}

	p.pipelineCfg = pcfg
	return nil
}

func (p *smlProcessor) Shutdown(_ context.Context) error {
	return nil
}

func (p *smlProcessor) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: true}
}

// ConsumeMetrics receives OTLP metrics, runs denormalize_metrics, and forwards
// the resulting wide-format features as new gauge metrics.
func (p *smlProcessor) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rows := otlpToRows(md, p.shouldSanitize())
	if len(rows) == 0 {
		return nil
	}

	rawDF := godf.NewDataFrame(rows)

	// Filter by service if configured
	if len(p.cfg.Services) > 0 {
		rawDF = pipeline.FilterByService(rawDF, p.cfg.Services, p.cfg.ServiceLabel)
		if rawDF.Empty() {
			return nil
		}
	}

	wide := pipeline.DenormalizeMetrics(rawDF, p.pipelineCfg)
	if wide.Empty() {
		return nil
	}

	result := wideToOTLP(wide)
	return p.next.ConsumeMetrics(ctx, result)
}

// applyEnvOverrides applies environment variable overrides to the config.
// Env vars take precedence over YAML values.
func applyEnvOverrides(cfg *Config) {
	if v := os.Getenv("SML_SERVICES"); v != "" {
		cfg.Services = strings.Split(v, ",")
	}
	if v := os.Getenv("SML_SERVICE_LABEL"); v != "" {
		cfg.ServiceLabel = v
	}
	if v := os.Getenv("SML_SCHEMA_PATH"); v != "" {
		cfg.SchemaPath = v
	}
	if v := os.Getenv("SML_WINDOW_SECONDS"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			cfg.WindowSeconds = f
		}
	}
	if v := os.Getenv("SML_INCLUDE_DELTAS"); v != "" {
		cfg.IncludeDeltas = v == "true" || v == "1" || v == "yes"
	}
	if v := os.Getenv("SML_ENTITY_LABELS"); v != "" {
		cfg.EntityLabels = strings.Split(v, ",")
	}
	if v := os.Getenv("SML_FORCE_DROP_LABELS"); v != "" {
		cfg.ForceDropLabels = strings.Split(v, ",")
	}
	if v := os.Getenv("SML_SANITIZE_NAMES"); v != "" {
		b := v == "true" || v == "1" || v == "yes"
		cfg.SanitizeNames = &b
	}
}

// --- OTLP conversion (unchanged) ---

func otlpToRows(md pmetric.Metrics, sanitize bool) []map[string]any {
	san := func(s string) string {
		if sanitize {
			return classifier.SanitizeName(s)
		}
		return s
	}

	var rows []map[string]any

	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		resourceAttrs := attributesToMap(rm.Resource().Attributes(), sanitize)

		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)

			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)
				metricName := san(m.Name())

				switch m.Type() {
				case pmetric.MetricTypeGauge:
					for di := 0; di < m.Gauge().DataPoints().Len(); di++ {
						dp := m.Gauge().DataPoints().At(di)
						labels := mergeLabels(resourceAttrs, attributesToMap(dp.Attributes(), sanitize))
						rows = append(rows, map[string]any{
							"timestamp": dp.Timestamp().AsTime().Format(time.RFC3339),
							"metric":    metricName,
							"labels":    labels,
							"value":     dpValue(dp),
						})
					}

				case pmetric.MetricTypeSum:
					for di := 0; di < m.Sum().DataPoints().Len(); di++ {
						dp := m.Sum().DataPoints().At(di)
						labels := mergeLabels(resourceAttrs, attributesToMap(dp.Attributes(), sanitize))
						name := metricName
						if m.Sum().IsMonotonic() && !strings.HasSuffix(name, "_total") {
							name += "_total"
						}
						rows = append(rows, map[string]any{
							"timestamp": dp.Timestamp().AsTime().Format(time.RFC3339),
							"metric":    name,
							"labels":    labels,
							"value":     dpValue(dp),
						})
					}

				case pmetric.MetricTypeHistogram:
					for di := 0; di < m.Histogram().DataPoints().Len(); di++ {
						dp := m.Histogram().DataPoints().At(di)
						labels := mergeLabels(resourceAttrs, attributesToMap(dp.Attributes(), sanitize))
						ts := dp.Timestamp().AsTime().Format(time.RFC3339)

						bounds := dp.ExplicitBounds().AsRaw()
						counts := dp.BucketCounts().AsRaw()
						cumulative := uint64(0)
						for bi := 0; bi < len(counts); bi++ {
							cumulative += counts[bi]
							le := "+Inf"
							if bi < len(bounds) {
								le = fmt.Sprintf("%g", bounds[bi])
							}
							bucketLabels := copyLabels(labels)
							bucketLabels["le"] = le
							rows = append(rows, map[string]any{
								"timestamp": ts,
								"metric":    metricName + "_bucket",
								"labels":    bucketLabels,
								"value":     float64(cumulative),
							})
						}

						if dp.HasSum() {
							rows = append(rows, map[string]any{
								"timestamp": ts,
								"metric":    metricName + "_sum",
								"labels":    labels,
								"value":     dp.Sum(),
							})
						}
						rows = append(rows, map[string]any{
							"timestamp": ts,
							"metric":    metricName + "_count",
							"labels":    labels,
							"value":     float64(dp.Count()),
						})
					}
				}
			}
		}
	}

	return rows
}

func wideToOTLP(wide *godf.DataFrame) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	sm.Scope().SetName("github.com/symetryml/otelsml")
	sm.Scope().SetVersion("0.1.0")

	exclude := map[string]bool{"timestamp": true, "entity_key": true}
	var featureCols []string
	for _, col := range wide.Columns() {
		if !exclude[col] && wide.Col(col).Dtype() == godf.Float64 {
			featureCols = append(featureCols, col)
		}
	}

	for _, col := range featureCols {
		m := sm.Metrics().AppendEmpty()
		m.SetName("sml." + col)
		m.SetDescription("SymetryML denormalized feature: " + col)
		gauge := m.SetEmptyGauge()

		series := wide.Col(col)
		for i := 0; i < wide.NRows(); i++ {
			val := series.Float(i)
			if math.IsNaN(val) {
				continue
			}

			dp := gauge.DataPoints().AppendEmpty()
			dp.SetDoubleValue(val)

			if wide.HasColumn("timestamp") {
				tsSeries := wide.Col("timestamp")
				if tsSeries.Dtype() == godf.String {
					if t, err := time.Parse(time.RFC3339, tsSeries.Str(i)); err == nil {
						dp.SetTimestamp(pcommon.NewTimestampFromTime(t))
					}
				} else if tsSeries.Dtype() == godf.DateTime {
					dp.SetTimestamp(pcommon.NewTimestampFromTime(tsSeries.Time(i)))
				}
			}

			if wide.HasColumn("entity_key") {
				dp.Attributes().PutStr("entity_key", wide.Col("entity_key").Str(i))
			}
		}
	}

	return md
}

func attributesToMap(attrs pcommon.Map, sanitize bool) map[string]string {
	result := make(map[string]string, attrs.Len())
	attrs.Range(func(k string, v pcommon.Value) bool {
		key := k
		if sanitize {
			key = classifier.SanitizeName(k)
		}
		result[key] = v.AsString()
		return true
	})
	return result
}

func mergeLabels(resource, datapoint map[string]string) map[string]string {
	result := make(map[string]string, len(resource)+len(datapoint))
	for k, v := range resource {
		result[k] = v
	}
	for k, v := range datapoint {
		result[k] = v
	}
	return result
}

func copyLabels(labels map[string]string) map[string]string {
	result := make(map[string]string, len(labels))
	for k, v := range labels {
		result[k] = v
	}
	return result
}

func dpValue(dp pmetric.NumberDataPoint) float64 {
	switch dp.ValueType() {
	case pmetric.NumberDataPointValueTypeDouble:
		return dp.DoubleValue()
	case pmetric.NumberDataPointValueTypeInt:
		return float64(dp.IntValue())
	default:
		return math.NaN()
	}
}
