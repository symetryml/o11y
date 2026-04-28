// Package aggregator provides metric-type-specific aggregation functions.
package aggregator

import (
	"math"
	"sort"
	"strconv"
)

// HistogramResult holds the aggregation result for histogram metrics.
type HistogramResult struct {
	P50   float64
	P75   float64
	P90   float64
	P95   float64
	P99   float64
	Mean  float64
	Count float64
	Sum   float64
}

// EstimatePercentile estimates a percentile from histogram bucket data
// using linear interpolation within buckets.
func EstimatePercentile(boundaries []float64, counts []float64, percentile float64) float64 {
	if len(counts) == 0 {
		return 0
	}
	allZero := true
	for _, c := range counts {
		if c != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return 0
	}

	totalCount := counts[len(counts)-1]
	if totalCount == 0 {
		return 0
	}

	targetCount := percentile * totalCount
	prevBoundary := 0.0
	prevCount := 0.0

	for i := range boundaries {
		boundary := boundaries[i]
		count := counts[i]

		if count >= targetCount {
			if count == prevCount {
				return boundary
			}
			if math.IsInf(boundary, 0) {
				return prevBoundary
			}
			fraction := (targetCount - prevCount) / (count - prevCount)
			return prevBoundary + fraction*(boundary-prevBoundary)
		}

		prevBoundary = boundary
		prevCount = count
	}

	if len(boundaries) > 1 {
		return boundaries[len(boundaries)-2]
	}
	return 0
}

// AggregateHistogram aggregates histogram bucket data.
//
// leValues and countValues are parallel slices of bucket boundaries ("le" labels)
// and cumulative counts. sumValue and countValue come from the _sum and _count metrics.
func AggregateHistogram(leValues []string, countValues []float64, sumValue, countValue float64) HistogramResult {
	if len(leValues) == 0 {
		return HistogramResult{}
	}

	// Parse and sort by boundary
	type bucket struct {
		boundary float64
		count    float64
	}
	buckets := make([]bucket, 0, len(leValues))
	for i, le := range leValues {
		var boundary float64
		if le == "+Inf" || le == "Inf" {
			boundary = math.Inf(1)
		} else {
			var err error
			boundary, err = strconv.ParseFloat(le, 64)
			if err != nil {
				continue
			}
		}
		buckets = append(buckets, bucket{boundary: boundary, count: countValues[i]})
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].boundary < buckets[j].boundary
	})

	// Merge duplicate boundaries by summing counts.
	// Multiple histogram instances (e.g. different routes) in the same
	// aggregation group produce duplicate le values.
	merged := make([]bucket, 0, len(buckets))
	for _, b := range buckets {
		if len(merged) > 0 && merged[len(merged)-1].boundary == b.boundary {
			merged[len(merged)-1].count += b.count
		} else {
			merged = append(merged, b)
		}
	}

	boundaries := make([]float64, len(merged))
	counts := make([]float64, len(merged))
	for i, b := range merged {
		boundaries[i] = b.boundary
		counts[i] = b.count
	}

	totalCount := countValue
	if totalCount == 0 && len(counts) > 0 {
		totalCount = counts[len(counts)-1]
	}

	mean := 0.0
	if totalCount > 0 {
		mean = sumValue / totalCount
	}

	return HistogramResult{
		P50:   EstimatePercentile(boundaries, counts, 0.50),
		P75:   EstimatePercentile(boundaries, counts, 0.75),
		P90:   EstimatePercentile(boundaries, counts, 0.90),
		P95:   EstimatePercentile(boundaries, counts, 0.95),
		P99:   EstimatePercentile(boundaries, counts, 0.99),
		Mean:  mean,
		Count: totalCount,
		Sum:   sumValue,
	}
}
