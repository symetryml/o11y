package aggregator

import (
	"sort"
	"time"
)

// CounterResult holds the aggregation result for counter metrics.
type CounterResult struct {
	RatePerSec float64
	Count      float64
}

// AggregateCounter computes rate and delta for counter values.
//
// values and timestamps are parallel slices sorted by time.
// windowSeconds is the expected collection window.
func AggregateCounter(values []float64, timestamps []time.Time, windowSeconds float64) CounterResult {
	if len(values) < 2 {
		return CounterResult{}
	}

	// Sort by timestamp
	type pair struct {
		ts  time.Time
		val float64
	}
	pairs := make([]pair, len(values))
	for i := range values {
		pairs[i] = pair{ts: timestamps[i], val: values[i]}
	}
	sort.SliceStable(pairs, func(i, j int) bool {
		if pairs[i].ts.Equal(pairs[j].ts) {
			return pairs[i].val < pairs[j].val
		}
		return pairs[i].ts.Before(pairs[j].ts)
	})

	first := pairs[0].val
	last := pairs[len(pairs)-1].val

	delta := last - first
	if delta < 0 {
		delta = last // counter reset
	}

	timeDelta := pairs[len(pairs)-1].ts.Sub(pairs[0].ts).Seconds()
	if timeDelta <= 0 {
		timeDelta = windowSeconds
	}

	rate := 0.0
	if timeDelta > 0 {
		rate = delta / timeDelta
	}

	return CounterResult{
		RatePerSec: rate,
		Count:      delta,
	}
}
