package aggregator

import (
	"math"
	"sort"
	"time"
)

// GaugeResult holds the aggregation result for gauge metrics.
type GaugeResult struct {
	Last   float64
	Mean   float64
	Min    float64
	Max    float64
	Stddev float64
}

// AggregateGauge computes summary statistics for gauge values.
//
// values and timestamps are parallel slices. timestamps may be nil.
func AggregateGauge(values []float64, timestamps []time.Time) GaugeResult {
	// Filter non-NaN values
	clean := make([]float64, 0, len(values))
	for _, v := range values {
		if !math.IsNaN(v) {
			clean = append(clean, v)
		}
	}

	if len(clean) == 0 {
		return GaugeResult{
			Last: math.NaN(), Mean: math.NaN(),
			Min: math.NaN(), Max: math.NaN(), Stddev: math.NaN(),
		}
	}

	// Determine last value by timestamp order
	var lastVal float64
	if timestamps != nil && len(timestamps) == len(values) {
		type pair struct {
			ts  time.Time
			val float64
		}
		pairs := make([]pair, len(values))
		for i := range values {
			pairs[i] = pair{ts: timestamps[i], val: values[i]}
		}
		sort.Slice(pairs, func(i, j int) bool {
			return pairs[i].ts.Before(pairs[j].ts)
		})
		lastVal = pairs[len(pairs)-1].val
	} else {
		lastVal = values[len(values)-1]
	}

	sum := 0.0
	minV := math.Inf(1)
	maxV := math.Inf(-1)
	for _, v := range clean {
		sum += v
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	mean := sum / float64(len(clean))

	stddev := 0.0
	if len(clean) > 1 {
		sumSq := 0.0
		for _, v := range clean {
			d := v - mean
			sumSq += d * d
		}
		stddev = math.Sqrt(sumSq / float64(len(clean)-1))
	}

	return GaugeResult{
		Last:   lastVal,
		Mean:   mean,
		Min:    minV,
		Max:    maxV,
		Stddev: stddev,
	}
}
