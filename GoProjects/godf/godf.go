// Package godf provides a lightweight, pandas-inspired DataFrame library for Go.
//
// It is designed for high-throughput telemetry processing where pandas-style
// operations (groupby, pivot, rolling windows, delta computation) are needed
// with the performance characteristics of compiled Go code.
//
// Core types:
//   - Series: a typed, nullable column of data (float64, string, time.Time, bool, or any)
//   - DataFrame: an ordered collection of named Series with aligned row indices
//   - GroupedDataFrame: the result of a GroupBy operation, supporting aggregation and transforms
package godf

import (
	"math"
	"time"
)

// DType represents the data type of a Series.
type DType int

const (
	Float64  DType = iota // Numeric data stored as float64
	String                // String data
	DateTime              // Time data stored as time.Time
	Bool                  // Boolean data
	Any                   // Mixed/unknown types stored as interface{}
)

// String returns the human-readable name of a DType.
func (d DType) String() string {
	switch d {
	case Float64:
		return "float64"
	case String:
		return "string"
	case DateTime:
		return "datetime"
	case Bool:
		return "bool"
	case Any:
		return "any"
	default:
		return "unknown"
	}
}

// NaN is a convenience alias for math.NaN().
var NaN = math.NaN()

// IsNaN reports whether f is a NaN value.
func IsNaN(f float64) bool {
	return math.IsNaN(f)
}

// zeroTime is the zero value of time.Time, used as the null sentinel for DateTime series.
var zeroTime time.Time
