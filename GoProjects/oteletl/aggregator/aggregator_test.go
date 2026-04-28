package aggregator

import (
	"math"
	"testing"
	"time"
)

const epsilon = 1e-6

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.IsNaN(want) {
		if !math.IsNaN(got) {
			t.Errorf("%s: got %f, want NaN", name, got)
		}
		return
	}
	if math.Abs(got-want) > epsilon {
		t.Errorf("%s: got %f, want %f", name, got, want)
	}
}

func TestEstimatePercentile(t *testing.T) {
	boundaries := []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, math.Inf(1)}
	counts := []float64{0, 0, 0, 2, 5, 15, 45, 80, 95, 99, 100, 100}

	assertFloat(t, "p50", EstimatePercentile(boundaries, counts, 0.50), 0.5714285714285714)
	assertFloat(t, "p90", EstimatePercentile(boundaries, counts, 0.90), 2.0)
	assertFloat(t, "p99", EstimatePercentile(boundaries, counts, 0.99), 5.0)
}

func TestAggregateHistogram(t *testing.T) {
	leValues := []string{"0.01", "0.05", "0.1", "0.5", "1.0", "+Inf"}
	countValues := []float64{5, 20, 50, 85, 95, 100}

	result := AggregateHistogram(leValues, countValues, 45.5, 100)
	assertFloat(t, "count", result.Count, 100)
	assertFloat(t, "sum", result.Sum, 45.5)
	assertFloat(t, "mean", result.Mean, 0.455)
	// p50 should be around 0.1
	if result.P50 < 0.05 || result.P50 > 0.2 {
		t.Errorf("p50 out of range: %f", result.P50)
	}
}

func TestAggregateCounter(t *testing.T) {
	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)

	result := AggregateCounter(
		[]float64{100, 150},
		[]time.Time{ts1, ts2},
		60.0,
	)
	assertFloat(t, "count", result.Count, 50)
	assertFloat(t, "rate", result.RatePerSec, 50.0/60.0)
}

func TestAggregateCounterSingle(t *testing.T) {
	result := AggregateCounter([]float64{100}, []time.Time{time.Now()}, 60)
	assertFloat(t, "count", result.Count, 0)
	assertFloat(t, "rate", result.RatePerSec, 0)
}

func TestAggregateGauge(t *testing.T) {
	ts1 := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC)

	result := AggregateGauge(
		[]float64{50, 60},
		[]time.Time{ts1, ts2},
	)
	assertFloat(t, "last", result.Last, 60)
	assertFloat(t, "mean", result.Mean, 55)
	assertFloat(t, "min", result.Min, 50)
	assertFloat(t, "max", result.Max, 60)
}

func TestAggregateGaugeEmpty(t *testing.T) {
	result := AggregateGauge(nil, nil)
	assertFloat(t, "last", result.Last, math.NaN())
	assertFloat(t, "mean", result.Mean, math.NaN())
}
