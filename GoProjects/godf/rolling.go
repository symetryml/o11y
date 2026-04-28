package godf

import (
	"math"
)

// RollingSeries provides rolling window operations on a Float64 Series.
type RollingSeries struct {
	series     *Series
	window     int
	minPeriods int
}

// Rolling creates a RollingSeries for windowed computations.
//
// Parameters:
//   - window:     the window size (number of elements)
//   - minPeriods: minimum number of non-null observations required for a value;
//     if less than minPeriods non-null values are in the window, the result is NaN.
//     Use 0 or negative to default to window size.
func (s *Series) Rolling(window, minPeriods int) *RollingSeries {
	if s.dtype != Float64 {
		panic("Rolling requires Float64 series")
	}
	if minPeriods <= 0 {
		minPeriods = window
	}
	return &RollingSeries{
		series:     s,
		window:     window,
		minPeriods: minPeriods,
	}
}

// Mean computes the rolling mean.
func (r *RollingSeries) Mean() *Series {
	n := r.series.length
	result := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - r.window + 1
		if start < 0 {
			start = 0
		}

		sum := 0.0
		count := 0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		if count >= r.minPeriods {
			result[i] = sum / float64(count)
		} else {
			result[i] = math.NaN()
		}
	}

	return NewFloat64Series(r.series.name, result)
}

// Std computes the rolling sample standard deviation (ddof=1).
func (r *RollingSeries) Std() *Series {
	n := r.series.length
	result := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - r.window + 1
		if start < 0 {
			start = 0
		}

		// Collect non-null values in window
		sum := 0.0
		count := 0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		if count < r.minPeriods {
			result[i] = math.NaN()
			continue
		}

		if count < 2 {
			result[i] = math.NaN()
			continue
		}

		mean := sum / float64(count)
		sumSq := 0.0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				d := v - mean
				sumSq += d * d
			}
		}

		result[i] = math.Sqrt(sumSq / float64(count-1))
	}

	return NewFloat64Series(r.series.name, result)
}

// Sum computes the rolling sum.
func (r *RollingSeries) Sum() *Series {
	n := r.series.length
	result := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - r.window + 1
		if start < 0 {
			start = 0
		}

		sum := 0.0
		count := 0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				sum += v
				count++
			}
		}

		if count >= r.minPeriods {
			result[i] = sum
		} else {
			result[i] = math.NaN()
		}
	}

	return NewFloat64Series(r.series.name, result)
}

// Min computes the rolling minimum.
func (r *RollingSeries) Min() *Series {
	n := r.series.length
	result := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - r.window + 1
		if start < 0 {
			start = 0
		}

		min := math.Inf(1)
		count := 0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				if v < min {
					min = v
				}
				count++
			}
		}

		if count >= r.minPeriods {
			result[i] = min
		} else {
			result[i] = math.NaN()
		}
	}

	return NewFloat64Series(r.series.name, result)
}

// Max computes the rolling maximum.
func (r *RollingSeries) Max() *Series {
	n := r.series.length
	result := make([]float64, n)

	for i := 0; i < n; i++ {
		start := i - r.window + 1
		if start < 0 {
			start = 0
		}

		max := math.Inf(-1)
		count := 0
		for j := start; j <= i; j++ {
			v := r.series.floats[j]
			if !math.IsNaN(v) {
				if v > max {
					max = v
				}
				count++
			}
		}

		if count >= r.minPeriods {
			result[i] = max
		} else {
			result[i] = math.NaN()
		}
	}

	return NewFloat64Series(r.series.name, result)
}
