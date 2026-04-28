package godf

import (
	"math"
	"sort"
)

// SortBy returns a new DataFrame sorted by the given columns in ascending order.
// Columns are sorted left-to-right (primary, secondary, etc.).
func (df *DataFrame) SortBy(cols ...string) *DataFrame {
	for _, col := range cols {
		if !df.HasColumn(col) {
			panic("SortBy: column not found: " + col)
		}
	}

	indices := make([]int, df.nRows)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		ai, bi := indices[a], indices[b]
		for _, col := range cols {
			s := df.columns[col]
			cmp := compareSeries(s, ai, bi)
			if cmp != 0 {
				return cmp < 0
			}
		}
		return false
	})

	return df.Take(indices)
}

// SortByDesc returns a new DataFrame sorted by the given columns in descending order.
func (df *DataFrame) SortByDesc(cols ...string) *DataFrame {
	for _, col := range cols {
		if !df.HasColumn(col) {
			panic("SortByDesc: column not found: " + col)
		}
	}

	indices := make([]int, df.nRows)
	for i := range indices {
		indices[i] = i
	}

	sort.SliceStable(indices, func(a, b int) bool {
		ai, bi := indices[a], indices[b]
		for _, col := range cols {
			s := df.columns[col]
			cmp := compareSeries(s, ai, bi)
			if cmp != 0 {
				return cmp > 0
			}
		}
		return false
	})

	return df.Take(indices)
}

// compareSeries compares two elements within a series.
// Returns -1, 0, or 1. NaN/null sorts last.
func compareSeries(s *Series, i, j int) int {
	iNull := s.IsNull(i)
	jNull := s.IsNull(j)
	if iNull && jNull {
		return 0
	}
	if iNull {
		return 1
	}
	if jNull {
		return -1
	}

	switch s.dtype {
	case Float64:
		a, b := s.floats[i], s.floats[j]
		aNaN, bNaN := math.IsNaN(a), math.IsNaN(b)
		if aNaN && bNaN {
			return 0
		}
		if aNaN {
			return 1
		}
		if bNaN {
			return -1
		}
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	case String:
		a, b := s.strings[i], s.strings[j]
		if a < b {
			return -1
		}
		if a > b {
			return 1
		}
		return 0
	case DateTime:
		a, b := s.times[i], s.times[j]
		if a.Before(b) {
			return -1
		}
		if a.After(b) {
			return 1
		}
		return 0
	case Bool:
		a, b := s.bools[i], s.bools[j]
		if !a && b {
			return -1
		}
		if a && !b {
			return 1
		}
		return 0
	default:
		return 0
	}
}
