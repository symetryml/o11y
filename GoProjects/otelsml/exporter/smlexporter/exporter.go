package smlexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strings"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"

	"github.com/symetryml/demclient"
)

type smlExporter struct {
	cfg          *Config
	writer       io.Writer
	file         *os.File
	csvHeaderSet map[string]bool // tracks if CSV header was written for a given column set
	demClient    *demclient.Client
	logger       *zap.Logger
}

func newExporter(cfg *Config, logger *zap.Logger) *smlExporter {
	return &smlExporter{cfg: cfg, csvHeaderSet: make(map[string]bool), logger: logger}
}

func (e *smlExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (e *smlExporter) Start(_ context.Context, _ component.Host) error {
	if e.cfg.OutputPath == "stdout" || e.cfg.OutputPath == "" {
		e.writer = os.Stdout
	} else {
		f, err := os.OpenFile(e.cfg.OutputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return fmt.Errorf("failed to open output file: %w", err)
		}
		e.file = f
		e.writer = f
	}

	// Initialize DEM client if endpoint is configured
	if e.cfg.DEMEndpoint != "" {
		keyID := os.Getenv("SML_KEY_ID")
		secretKey := os.Getenv("SML_SECRET_KEY")
		if keyID == "" || secretKey == "" {
			return fmt.Errorf("SML_KEY_ID and SML_SECRET_KEY env vars required when dem_endpoint is set")
		}
		e.demClient = demclient.NewClient(demclient.Config{
			Server:       e.cfg.DEMEndpoint,
			SymKeyID:     keyID,
			SymSecretKey: secretKey,
			ClientID:     "smlexporter",
		})
		if e.cfg.ProjectName != "" {
			if err := e.demClient.EnsureProject(e.cfg.ProjectName, "cpu"); err != nil {
				e.logger.Warn("failed to ensure DEM project, will retry on first stream", zap.Error(err))
			}
		}
		e.logger.Info("DEM client initialized", zap.String("endpoint", e.cfg.DEMEndpoint), zap.String("project", e.cfg.ProjectName))
	}

	return nil
}

func (e *smlExporter) Shutdown(_ context.Context) error {
	if e.file != nil {
		return e.file.Close()
	}
	return nil
}

// ConsumeMetrics writes metrics in the configured format and streams to DEM if configured.
func (e *smlExporter) ConsumeMetrics(_ context.Context, md pmetric.Metrics) error {
	var err error
	if e.cfg.Format == "csv" {
		err = e.consumeCSV(md)
	} else {
		err = e.consumeJSON(md)
	}
	if err != nil {
		return err
	}

	// Stream to DEM server if configured
	if e.demClient != nil && e.cfg.ProjectName != "" {
		if streamErr := e.streamToDEM(md); streamErr != nil {
			e.logger.Error("failed to stream to DEM", zap.Error(streamErr))
		}
	}

	return nil
}

// consumeJSON writes one JSON line per (metric, timestamp, entity) data point.
func (e *smlExporter) consumeJSON(md pmetric.Metrics) error {
	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)

				if m.Type() != pmetric.MetricTypeGauge {
					continue
				}

				gauge := m.Gauge()
				for di := 0; di < gauge.DataPoints().Len(); di++ {
					dp := gauge.DataPoints().At(di)
					val := dp.DoubleValue()
					if math.IsNaN(val) {
						continue
					}

					record := map[string]any{
						"metric":    m.Name(),
						"timestamp": dp.Timestamp().AsTime().Format("2006-01-02T15:04:05Z"),
						"value":     val,
					}

					dp.Attributes().Range(func(k string, v pcommon.Value) bool {
						record[k] = v.AsString()
						return true
					})

					line, err := json.Marshal(record)
					if err != nil {
						continue
					}
					fmt.Fprintln(e.writer, string(line))
				}
			}
		}
	}
	return nil
}

// consumeCSV writes wide-format CSV: one row per (timestamp, entity_key),
// all features as columns.
func (e *smlExporter) consumeCSV(md pmetric.Metrics) error {
	// Collect all data points into a wide map: rowKey → featureName → value
	type rowKey struct {
		timestamp string
		entityKey string
	}

	rows := make(map[rowKey]map[string]float64)
	var rowOrder []rowKey
	rowSeen := make(map[rowKey]bool)
	featureSet := make(map[string]bool)

	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)
				if m.Type() != pmetric.MetricTypeGauge {
					continue
				}

				// Strip "sml." prefix for cleaner CSV column names
				feature := m.Name()
				if strings.HasPrefix(feature, "sml.") {
					feature = feature[4:]
				}

				gauge := m.Gauge()
				for di := 0; di < gauge.DataPoints().Len(); di++ {
					dp := gauge.DataPoints().At(di)
					val := dp.DoubleValue()
					if math.IsNaN(val) {
						continue
					}

					ts := dp.Timestamp().AsTime().Format("2006-01-02T15:04:05Z")
					ek := ""
					if v, ok := dp.Attributes().Get("entity_key"); ok {
						ek = v.AsString()
					}

					rk := rowKey{timestamp: ts, entityKey: ek}
					if !rowSeen[rk] {
						rowSeen[rk] = true
						rowOrder = append(rowOrder, rk)
						rows[rk] = make(map[string]float64)
					}

					rows[rk][feature] = val
					featureSet[feature] = true
				}
			}
		}
	}

	if len(rows) == 0 {
		return nil
	}

	// Sort feature columns alphabetically
	features := make([]string, 0, len(featureSet))
	for f := range featureSet {
		features = append(features, f)
	}
	sort.Strings(features)

	// Build column signature for header tracking
	colSig := strings.Join(features, ",")

	// Write header if not yet written for this column set
	if !e.csvHeaderSet[colSig] {
		header := "timestamp,entity_key," + strings.Join(features, ",")
		fmt.Fprintln(e.writer, header)
		e.csvHeaderSet[colSig] = true
	}

	// Write rows
	for _, rk := range rowOrder {
		vals := rows[rk]
		parts := make([]string, 0, len(features)+2)
		parts = append(parts, rk.timestamp, rk.entityKey)
		for _, f := range features {
			if v, ok := vals[f]; ok {
				parts = append(parts, fmt.Sprintf("%g", v))
			} else {
				parts = append(parts, "")
			}
		}
		fmt.Fprintln(e.writer, strings.Join(parts, ","))
	}

	return nil
}

// streamToDEM converts metrics to an SMLDataFrame and streams to the DEM server.
func (e *smlExporter) streamToDEM(md pmetric.Metrics) error {
	type rowKey struct {
		timestamp string
		entityKey string
	}

	rows := make(map[rowKey]map[string]float64)
	var rowOrder []rowKey
	rowSeen := make(map[rowKey]bool)
	featureSet := make(map[string]bool)

	for ri := 0; ri < md.ResourceMetrics().Len(); ri++ {
		rm := md.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)
				if m.Type() != pmetric.MetricTypeGauge {
					continue
				}

				feature := m.Name()
				if strings.HasPrefix(feature, "sml.") {
					feature = feature[4:]
				}

				gauge := m.Gauge()
				for di := 0; di < gauge.DataPoints().Len(); di++ {
					dp := gauge.DataPoints().At(di)
					val := dp.DoubleValue()
					if math.IsNaN(val) {
						continue
					}

					ts := dp.Timestamp().AsTime().Format("2006-01-02T15:04:05Z")
					ek := ""
					if v, ok := dp.Attributes().Get("entity_key"); ok {
						ek = v.AsString()
					}

					rk := rowKey{timestamp: ts, entityKey: ek}
					if !rowSeen[rk] {
						rowSeen[rk] = true
						rowOrder = append(rowOrder, rk)
						rows[rk] = make(map[string]float64)
					}

					rows[rk][feature] = val
					featureSet[feature] = true
				}
			}
		}
	}

	if len(rows) == 0 {
		return nil
	}

	// Build sorted feature list
	features := make([]string, 0, len(featureSet))
	for f := range featureSet {
		features = append(features, f)
	}
	sort.Strings(features)

	// Build SMLDataFrame
	header := make([]string, 0, len(features)+2)
	header = append(header, "timestamp", "entity_key")
	header = append(header, features...)

	types := make([]string, len(header))
	types[0] = "S" // timestamp
	types[1] = "S" // entity_key
	for i := 2; i < len(types); i++ {
		types[i] = "C" // all metric values are continuous
	}

	data := make([][]any, 0, len(rowOrder))
	for _, rk := range rowOrder {
		vals := rows[rk]
		row := make([]any, len(header))
		row[0] = rk.timestamp
		row[1] = rk.entityKey
		for i, f := range features {
			if v, ok := vals[f]; ok {
				row[i+2] = v
			} else {
				row[i+2] = math.NaN()
			}
		}
		data = append(data, row)
	}

	df := &demclient.SMLDataFrame{
		AttributeNames: header,
		AttributeTypes: types,
		Data:           data,
	}

	return e.demClient.StreamData(e.cfg.ProjectName, df)
}
