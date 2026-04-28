package godf

import (
	"math"
	"time"
)

// Concat concatenates DataFrames.
//
//   - axis=0: stack rows (union of columns, missing values filled with NaN/null)
//   - axis=1: join columns (all DataFrames must have the same number of rows)
func Concat(dfs []*DataFrame, axis int) *DataFrame {
	if len(dfs) == 0 {
		return NewDataFrame(nil)
	}
	if len(dfs) == 1 {
		return dfs[0].Copy()
	}

	switch axis {
	case 0:
		return concatRows(dfs)
	case 1:
		return concatCols(dfs)
	default:
		panic("Concat: axis must be 0 or 1")
	}
}

// concatRows stacks DataFrames vertically.
func concatRows(dfs []*DataFrame) *DataFrame {
	// Collect all column names in order
	colSet := make(map[string]struct{})
	var colOrder []string
	for _, df := range dfs {
		for _, col := range df.colOrder {
			if _, ok := colSet[col]; !ok {
				colSet[col] = struct{}{}
				colOrder = append(colOrder, col)
			}
		}
	}

	// Determine column types from first DataFrame that has each column
	colTypes := make(map[string]DType)
	for _, col := range colOrder {
		for _, df := range dfs {
			if s, ok := df.columns[col]; ok {
				colTypes[col] = s.Dtype()
				break
			}
		}
	}

	// Count total rows
	totalRows := 0
	for _, df := range dfs {
		totalRows += df.nRows
	}

	// Build columns
	columns := make(map[string]*Series, len(colOrder))
	for _, col := range colOrder {
		dtype := colTypes[col]
		switch dtype {
		case Float64:
			vals := make([]float64, 0, totalRows)
			for _, df := range dfs {
				if s, ok := df.columns[col]; ok {
					vals = append(vals, s.floats...)
				} else {
					for j := 0; j < df.nRows; j++ {
						vals = append(vals, math.NaN())
					}
				}
			}
			columns[col] = NewFloat64Series(col, vals)
		case String:
			vals := make([]string, 0, totalRows)
			nulls := make([]bool, 0, totalRows)
			for _, df := range dfs {
				if s, ok := df.columns[col]; ok {
					vals = append(vals, s.strings...)
					nulls = append(nulls, s.nulls...)
				} else {
					for j := 0; j < df.nRows; j++ {
						vals = append(vals, "")
						nulls = append(nulls, true)
					}
				}
			}
			columns[col] = NewStringSeriesFromSlice(col, vals, nulls)
		case DateTime:
			vals := make([]time.Time, 0, totalRows)
			nulls := make([]bool, 0, totalRows)
			for _, df := range dfs {
				if s, ok := df.columns[col]; ok {
					vals = append(vals, s.times...)
					nulls = append(nulls, s.nulls...)
				} else {
					for j := 0; j < df.nRows; j++ {
						vals = append(vals, time.Time{})
						nulls = append(nulls, true)
					}
				}
			}
			columns[col] = NewDateTimeSeries(col, vals, nulls)
		case Bool:
			vals := make([]bool, 0, totalRows)
			nulls := make([]bool, 0, totalRows)
			for _, df := range dfs {
				if s, ok := df.columns[col]; ok {
					vals = append(vals, s.bools...)
					nulls = append(nulls, s.nulls...)
				} else {
					for j := 0; j < df.nRows; j++ {
						vals = append(vals, false)
						nulls = append(nulls, true)
					}
				}
			}
			columns[col] = NewBoolSeries(col, vals, nulls)
		default:
			vals := make([]any, 0, totalRows)
			for _, df := range dfs {
				if s, ok := df.columns[col]; ok {
					for j := 0; j < s.Len(); j++ {
						vals = append(vals, s.Any(j))
					}
				} else {
					for j := 0; j < df.nRows; j++ {
						vals = append(vals, nil)
					}
				}
			}
			columns[col] = NewAnySeries(col, vals)
		}
	}

	return &DataFrame{
		columns:  columns,
		colOrder: colOrder,
		nRows:    totalRows,
	}
}

// concatCols joins DataFrames side by side.
func concatCols(dfs []*DataFrame) *DataFrame {
	if len(dfs) == 0 {
		return NewDataFrame(nil)
	}

	nRows := dfs[0].nRows
	columns := make(map[string]*Series)
	var colOrder []string

	for _, df := range dfs {
		if df.nRows != nRows {
			panic("Concat axis=1: all DataFrames must have the same number of rows")
		}
		for _, col := range df.colOrder {
			if _, exists := columns[col]; exists {
				// Later DataFrame overwrites
				columns[col] = df.columns[col]
			} else {
				columns[col] = df.columns[col]
				colOrder = append(colOrder, col)
			}
		}
	}

	return &DataFrame{
		columns:  columns,
		colOrder: colOrder,
		nRows:    nRows,
	}
}
