package smlprocessor

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/symetryml/godf"
)

// --- Test helpers ---

func newTestProcessor(t *testing.T, cfg *Config) (*smlProcessor, *consumertest.MetricsSink) {
	t.Helper()
	sink := &consumertest.MetricsSink{}
	if cfg == nil {
		cfg = createDefaultConfig().(*Config)
		cfg.IncludeDeltas = false
	}
	p := newProcessor(cfg, sink, component.TelemetrySettings{})
	return p, sink
}

func buildGaugeMetrics(name string, values []float64, ts time.Time, attrs map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	gauge := m.SetEmptyGauge()

	for _, v := range values {
		dp := gauge.DataPoints().AppendEmpty()
		dp.SetDoubleValue(v)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		for k, val := range attrs {
			dp.Attributes().PutStr(k, val)
		}
	}

	return md
}

func buildHistogramMetrics(name string, bounds []float64, counts []uint64, sum float64, count uint64, ts time.Time, attrs map[string]string) pmetric.Metrics {
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()
	m := sm.Metrics().AppendEmpty()
	m.SetName(name)
	hist := m.SetEmptyHistogram()

	dp := hist.DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	dp.ExplicitBounds().FromRaw(bounds)
	dp.BucketCounts().FromRaw(counts)
	dp.SetSum(sum)
	dp.SetCount(count)
	for k, v := range attrs {
		dp.Attributes().PutStr(k, v)
	}

	return md
}

// --- Tests ---

func TestProcessorGaugeMetrics(t *testing.T) {
	p, sink := newTestProcessor(t, nil)

	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildGaugeMetrics("cpu_usage", []float64{50.0}, ts, map[string]string{"service_name": "web"})

	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("ConsumeMetrics failed: %v", err)
	}

	if len(sink.AllMetrics()) == 0 {
		t.Fatal("Sink received no metrics")
	}

	output := sink.AllMetrics()[0]
	found := false
	for ri := 0; ri < output.ResourceMetrics().Len(); ri++ {
		rm := output.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)
				if strings.HasPrefix(m.Name(), "sml.cpu_usage__") {
					found = true
				}
			}
		}
	}

	if !found {
		t.Error("Expected sml.cpu_usage__* metrics in output")
		listOutputMetrics(t, sink)
	}
}

func TestProcessorHistogramMetrics(t *testing.T) {
	p, sink := newTestProcessor(t, nil)

	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildHistogramMetrics(
		"http_request_duration",
		[]float64{0.01, 0.1, 1.0},
		[]uint64{5, 45, 45, 5},
		45.5, 100,
		ts,
		map[string]string{"service_name": "web"},
	)

	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("ConsumeMetrics failed: %v", err)
	}

	if len(sink.AllMetrics()) == 0 {
		t.Fatal("Sink received no metrics")
	}

	output := sink.AllMetrics()[0]
	hasP50 := false
	hasMean := false
	for ri := 0; ri < output.ResourceMetrics().Len(); ri++ {
		rm := output.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				name := sm.Metrics().At(mi).Name()
				if name == "sml.http_request_duration__p50" {
					hasP50 = true
				}
				if name == "sml.http_request_duration__mean" {
					hasMean = true
				}
			}
		}
	}

	if !hasP50 {
		t.Error("Expected sml.http_request_duration__p50 metric")
		listOutputMetrics(t, sink)
	}
	if !hasMean {
		t.Error("Expected sml.http_request_duration__mean metric")
	}
}

func TestProcessorCounterWithStatusBucketing(t *testing.T) {
	p, sink := newTestProcessor(t, nil)

	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)

	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	sm := rm.ScopeMetrics().AppendEmpty()

	m := sm.Metrics().AppendEmpty()
	m.SetName("http_requests")
	sum := m.SetEmptySum()
	sum.SetIsMonotonic(true)
	dp := sum.DataPoints().AppendEmpty()
	dp.SetDoubleValue(100)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(ts1))
	dp.Attributes().PutStr("service_name", "web")
	dp.Attributes().PutStr("status_code", "200")
	dp2 := sum.DataPoints().AppendEmpty()
	dp2.SetDoubleValue(150)
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(ts2))
	dp2.Attributes().PutStr("service_name", "web")
	dp2.Attributes().PutStr("status_code", "200")

	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("ConsumeMetrics failed: %v", err)
	}

	if len(sink.AllMetrics()) == 0 {
		t.Fatal("Sink received no metrics")
	}

	output := sink.AllMetrics()[0]
	hasSuccess := false
	for ri := 0; ri < output.ResourceMetrics().Len(); ri++ {
		rm := output.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				name := sm.Metrics().At(mi).Name()
				if strings.Contains(name, "__success") {
					hasSuccess = true
				}
			}
		}
	}

	if !hasSuccess {
		t.Error("Expected status-bucketed metrics with __success suffix")
		listOutputMetrics(t, sink)
	}
}

func TestProcessorEmptyInput(t *testing.T) {
	p, sink := newTestProcessor(t, nil)

	md := pmetric.NewMetrics()
	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatalf("ConsumeMetrics failed on empty: %v", err)
	}

	// Empty input passes through — no crash, no NaN in output
	for _, out := range sink.AllMetrics() {
		for ri := 0; ri < out.ResourceMetrics().Len(); ri++ {
			rm := out.ResourceMetrics().At(ri)
			for si := 0; si < rm.ScopeMetrics().Len(); si++ {
				sm := rm.ScopeMetrics().At(si)
				for mi := 0; mi < sm.Metrics().Len(); mi++ {
					m := sm.Metrics().At(mi)
					if m.Type() == pmetric.MetricTypeGauge {
						for di := 0; di < m.Gauge().DataPoints().Len(); di++ {
							if math.IsNaN(m.Gauge().DataPoints().At(di).DoubleValue()) {
								t.Error("Output contains NaN values")
							}
						}
					}
				}
			}
		}
	}
}

func TestProcessorEntityKeyInOutput(t *testing.T) {
	p, sink := newTestProcessor(t, nil)

	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildGaugeMetrics("cpu_usage", []float64{50.0}, ts, map[string]string{"service_name": "web"})

	p.ConsumeMetrics(context.Background(), md)

	if len(sink.AllMetrics()) == 0 {
		t.Fatal("No output")
	}

	output := sink.AllMetrics()[0]
	hasEntityKey := false
	for ri := 0; ri < output.ResourceMetrics().Len(); ri++ {
		rm := output.ResourceMetrics().At(ri)
		for si := 0; si < rm.ScopeMetrics().Len(); si++ {
			sm := rm.ScopeMetrics().At(si)
			for mi := 0; mi < sm.Metrics().Len(); mi++ {
				m := sm.Metrics().At(mi)
				if m.Type() == pmetric.MetricTypeGauge {
					for di := 0; di < m.Gauge().DataPoints().Len(); di++ {
						dp := m.Gauge().DataPoints().At(di)
						if v, ok := dp.Attributes().Get("entity_key"); ok {
							if strings.Contains(v.AsString(), "service_name=web") {
								hasEntityKey = true
							}
						}
					}
				}
			}
		}
	}

	if !hasEntityKey {
		t.Error("Expected entity_key attribute with service_name=web in output")
	}
}

func TestOtlpToRowsGauge(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildGaugeMetrics("cpu", []float64{50.0, 60.0}, ts, map[string]string{"host": "a"})

	rows := otlpToRows(md)
	if len(rows) != 2 {
		t.Fatalf("otlpToRows: got %d rows, want 2", len(rows))
	}
	if rows[0]["metric"] != "cpu" {
		t.Errorf("metric: got %v, want 'cpu'", rows[0]["metric"])
	}
}

func TestOtlpToRowsHistogram(t *testing.T) {
	ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	md := buildHistogramMetrics("duration", []float64{0.1, 1.0}, []uint64{10, 40, 50}, 25.0, 100, ts, nil)

	rows := otlpToRows(md)
	// 3 bucket rows + _sum + _count = 5
	if len(rows) != 5 {
		t.Fatalf("otlpToRows histogram: got %d rows, want 5", len(rows))
	}

	hasBucket, hasSum, hasCount := false, false, false
	for _, r := range rows {
		name := r["metric"].(string)
		if strings.HasSuffix(name, "_bucket") {
			hasBucket = true
		}
		if strings.HasSuffix(name, "_sum") {
			hasSum = true
		}
		if strings.HasSuffix(name, "_count") {
			hasCount = true
		}
	}

	if !hasBucket || !hasSum || !hasCount {
		t.Errorf("Missing histogram components: bucket=%v sum=%v count=%v", hasBucket, hasSum, hasCount)
	}
}

func TestWideToOTLP(t *testing.T) {
	wide := godf.NewDataFrame([]map[string]any{
		{"timestamp": "2024-01-01T00:00:00Z", "entity_key": "service_name=web", "cpu__last": 50.0, "cpu__mean": 50.0},
		{"timestamp": "2024-01-01T00:01:00Z", "entity_key": "service_name=web", "cpu__last": 60.0, "cpu__mean": 60.0},
	})

	result := wideToOTLP(wide)

	if result.ResourceMetrics().Len() == 0 {
		t.Fatal("wideToOTLP produced empty output")
	}

	sm := result.ResourceMetrics().At(0).ScopeMetrics().At(0)
	if sm.Scope().Name() != "github.com/symetryml/otelsml" {
		t.Errorf("Scope name: got %q", sm.Scope().Name())
	}

	metricCount := sm.Metrics().Len()
	if metricCount != 2 {
		t.Errorf("Expected 2 metrics (cpu__last, cpu__mean), got %d", metricCount)
	}
}

func TestProcessorCapabilities(t *testing.T) {
	p, _ := newTestProcessor(t, nil)
	if !p.Capabilities().MutatesData {
		t.Error("Processor should report MutatesData=true")
	}
}

// --- Debug helper ---

func listOutputMetrics(t *testing.T, sink *consumertest.MetricsSink) {
	t.Helper()
	for _, out := range sink.AllMetrics() {
		for ri := 0; ri < out.ResourceMetrics().Len(); ri++ {
			rm := out.ResourceMetrics().At(ri)
			for si := 0; si < rm.ScopeMetrics().Len(); si++ {
				sm := rm.ScopeMetrics().At(si)
				for mi := 0; mi < sm.Metrics().Len(); mi++ {
					t.Logf("  output metric: %s", sm.Metrics().At(mi).Name())
				}
			}
		}
	}
}
