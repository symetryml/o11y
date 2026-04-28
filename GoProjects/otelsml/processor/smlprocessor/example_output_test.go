package smlprocessor

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

// TestExampleOutput prints what the otelsml processor actually produces.
// Run with: go test -run TestExampleOutput -v
func TestExampleOutput(t *testing.T) {
	sink := &consumertest.MetricsSink{}
	cfg := createDefaultConfig().(*Config)
	cfg.IncludeDeltas = false
	p := newProcessor(cfg, sink, component.TelemetrySettings{})

	// Build realistic input: gauges + histogram + counter with status codes
	md := pmetric.NewMetrics()
	rm := md.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("service.name", "checkout")
	sm := rm.ScopeMetrics().AppendEmpty()

	ts1 := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 12, 1, 0, 0, time.UTC)

	// CPU gauge
	cpu := sm.Metrics().AppendEmpty()
	cpu.SetName("process_cpu_usage")
	g := cpu.SetEmptyGauge()
	for _, pair := range []struct{ ts time.Time; val float64 }{{ts1, 0.45}, {ts2, 0.52}} {
		dp := g.DataPoints().AppendEmpty()
		dp.SetDoubleValue(pair.val)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(pair.ts))
		dp.Attributes().PutStr("service_name", "checkout")
	}

	// Memory gauge
	mem := sm.Metrics().AppendEmpty()
	mem.SetName("process_memory_usage")
	g2 := mem.SetEmptyGauge()
	for _, pair := range []struct{ ts time.Time; val float64 }{{ts1, 256000000}, {ts2, 262000000}} {
		dp := g2.DataPoints().AppendEmpty()
		dp.SetDoubleValue(pair.val)
		dp.SetTimestamp(pcommon.NewTimestampFromTime(pair.ts))
		dp.Attributes().PutStr("service_name", "checkout")
	}

	// HTTP request duration histogram
	hist := sm.Metrics().AppendEmpty()
	hist.SetName("http_server_request_duration")
	h := hist.SetEmptyHistogram()
	hdp := h.DataPoints().AppendEmpty()
	hdp.SetTimestamp(pcommon.NewTimestampFromTime(ts1))
	hdp.ExplicitBounds().FromRaw([]float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0})
	hdp.BucketCounts().FromRaw([]uint64{10, 25, 40, 60, 80, 90, 95, 98, 2})
	hdp.SetSum(45.5)
	hdp.SetCount(500)
	hdp.Attributes().PutStr("service_name", "checkout")

	// HTTP requests counter with status codes
	for _, sc := range []struct{ code string; v1, v2 float64 }{{"200", 1000, 1050}, {"500", 10, 12}} {
		counter := sm.Metrics().AppendEmpty()
		counter.SetName("http_server_requests")
		s := counter.SetEmptySum()
		s.SetIsMonotonic(true)
		for _, pair := range []struct{ ts time.Time; val float64 }{{ts1, sc.v1}, {ts2, sc.v2}} {
			dp := s.DataPoints().AppendEmpty()
			dp.SetDoubleValue(pair.val)
			dp.SetTimestamp(pcommon.NewTimestampFromTime(pair.ts))
			dp.Attributes().PutStr("service_name", "checkout")
			dp.Attributes().PutStr("status_code", sc.code)
		}
	}

	err := p.ConsumeMetrics(context.Background(), md)
	if err != nil {
		t.Fatal(err)
	}

	// Print what the exporter would see
	fmt.Println("")
	fmt.Println("=== CURRENT JSON OUTPUT (one line per data point) ===")
	fmt.Println("")
	for _, out := range sink.AllMetrics() {
		for ri := 0; ri < out.ResourceMetrics().Len(); ri++ {
			rm := out.ResourceMetrics().At(ri)
			for si := 0; si < rm.ScopeMetrics().Len(); si++ {
				sm := rm.ScopeMetrics().At(si)
				for mi := 0; mi < sm.Metrics().Len(); mi++ {
					m := sm.Metrics().At(mi)
					if m.Type() != pmetric.MetricTypeGauge {
						continue
					}
					for di := 0; di < m.Gauge().DataPoints().Len(); di++ {
						dp := m.Gauge().DataPoints().At(di)
						rec := map[string]any{
							"metric":    m.Name(),
							"timestamp": dp.Timestamp().AsTime().Format(time.RFC3339),
							"value":     dp.DoubleValue(),
						}
						dp.Attributes().Range(func(k string, v pcommon.Value) bool {
							rec[k] = v.AsString()
							return true
						})
						line, _ := json.Marshal(rec)
						fmt.Println(string(line))
					}
				}
			}
		}
	}
}
