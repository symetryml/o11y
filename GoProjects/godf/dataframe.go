package godf

import (
	"fmt"
	"math"
	"time"
)

// DataFrame is an ordered collection of named Series with aligned row indices.
type DataFrame struct {
	columns  map[string]*Series
	colOrder []string
	nRows    int
}

// NewDataFrame creates a DataFrame from a list of records (maps).
// Column types are inferred from the first non-nil value in each column.
func NewDataFrame(records []map[string]any) *DataFrame {
	if len(records) == 0 {
		return &DataFrame{
			columns:  make(map[string]*Series),
			colOrder: nil,
			nRows:    0,
		}
	}

	// Collect column names in stable order (first-seen order from first record)
	colOrderMap := make(map[string]int)
	var colOrder []string
	for _, rec := range records {
		for k := range rec {
			if _, ok := colOrderMap[k]; !ok {
				colOrderMap[k] = len(colOrder)
				colOrder = append(colOrder, k)
			}
		}
	}

	nRows := len(records)

	// Detect types from first non-nil value
	colTypes := make(map[string]DType)
	for _, col := range colOrder {
		for _, rec := range records {
			v := rec[col]
			if v == nil {
				continue
			}
			switch v.(type) {
			case float64, float32, int, int64, int32, int16, int8, uint, uint64, uint32:
				colTypes[col] = Float64
			case string:
				colTypes[col] = String
			case time.Time:
				colTypes[col] = DateTime
			case bool:
				colTypes[col] = Bool
			default:
				colTypes[col] = Any
			}
			break
		}
		if _, ok := colTypes[col]; !ok {
			colTypes[col] = Float64 // default to float if all nil
		}
	}

	// Build series
	columns := make(map[string]*Series, len(colOrder))
	for _, col := range colOrder {
		dtype := colTypes[col]
		switch dtype {
		case Float64:
			vals := make([]float64, nRows)
			for i, rec := range records {
				v, ok := rec[col]
				if !ok || v == nil {
					vals[i] = math.NaN()
				} else if f, ok := toFloat64(v); ok {
					vals[i] = f
				} else {
					vals[i] = math.NaN()
				}
			}
			columns[col] = NewFloat64Series(col, vals)
		case String:
			vals := make([]string, nRows)
			nulls := make([]bool, nRows)
			for i, rec := range records {
				v, ok := rec[col]
				if !ok || v == nil {
					nulls[i] = true
				} else if sv, ok := v.(string); ok {
					vals[i] = sv
				} else {
					vals[i] = fmt.Sprintf("%v", v)
				}
			}
			columns[col] = NewStringSeriesFromSlice(col, vals, nulls)
		case DateTime:
			vals := make([]time.Time, nRows)
			nulls := make([]bool, nRows)
			for i, rec := range records {
				v, ok := rec[col]
				if !ok || v == nil {
					nulls[i] = true
				} else if tv, ok := v.(time.Time); ok {
					vals[i] = tv
				} else {
					nulls[i] = true
				}
			}
			columns[col] = NewDateTimeSeries(col, vals, nulls)
		case Bool:
			vals := make([]bool, nRows)
			nulls := make([]bool, nRows)
			for i, rec := range records {
				v, ok := rec[col]
				if !ok || v == nil {
					nulls[i] = true
				} else if bv, ok := v.(bool); ok {
					vals[i] = bv
				} else {
					nulls[i] = true
				}
			}
			columns[col] = NewBoolSeries(col, vals, nulls)
		default:
			vals := make([]any, nRows)
			for i, rec := range records {
				vals[i] = rec[col]
			}
			columns[col] = NewAnySeries(col, vals)
		}
	}

	return &DataFrame{
		columns:  columns,
		colOrder: colOrder,
		nRows:    nRows,
	}
}

// NewDataFrameFromSeries creates a DataFrame from named Series.
// All series must have the same length.
func NewDataFrameFromSeries(series ...*Series) *DataFrame {
	if len(series) == 0 {
		return &DataFrame{
			columns:  make(map[string]*Series),
			colOrder: nil,
			nRows:    0,
		}
	}

	nRows := series[0].Len()
	columns := make(map[string]*Series, len(series))
	colOrder := make([]string, len(series))

	for i, s := range series {
		if s.Len() != nRows {
			panic(fmt.Sprintf("series %q has length %d, expected %d", s.Name(), s.Len(), nRows))
		}
		columns[s.Name()] = s
		colOrder[i] = s.Name()
	}

	return &DataFrame{
		columns:  columns,
		colOrder: colOrder,
		nRows:    nRows,
	}
}

// Empty reports whether the DataFrame has zero rows.
func (df *DataFrame) Empty() bool {
	return df.nRows == 0
}

// NRows returns the number of rows.
func (df *DataFrame) NRows() int {
	return df.nRows
}

// NCols returns the number of columns.
func (df *DataFrame) NCols() int {
	return len(df.colOrder)
}

// Columns returns column names in order.
func (df *DataFrame) Columns() []string {
	out := make([]string, len(df.colOrder))
	copy(out, df.colOrder)
	return out
}

// HasColumn reports whether a column exists.
func (df *DataFrame) HasColumn(name string) bool {
	_, ok := df.columns[name]
	return ok
}

// Col returns the named Series. Panics if not found.
func (df *DataFrame) Col(name string) *Series {
	s, ok := df.columns[name]
	if !ok {
		panic(fmt.Sprintf("column %q not found", name))
	}
	return s
}

// ColSafe returns the named Series and a boolean indicating if it was found.
func (df *DataFrame) ColSafe(name string) (*Series, bool) {
	s, ok := df.columns[name]
	return s, ok
}

// SetCol adds or replaces a column. The series length must match NRows.
func (df *DataFrame) SetCol(name string, s *Series) *DataFrame {
	if df.nRows > 0 && s.Len() != df.nRows {
		panic(fmt.Sprintf("SetCol: series %q has length %d, DataFrame has %d rows",
			name, s.Len(), df.nRows))
	}

	// Rename series to match column name
	s = s.Copy()
	s.name = name

	if _, exists := df.columns[name]; !exists {
		df.colOrder = append(df.colOrder, name)
	}
	df.columns[name] = s
	if df.nRows == 0 {
		df.nRows = s.Len()
	}
	return df
}

// DropCol removes a column. No-op if the column doesn't exist.
func (df *DataFrame) DropCol(name string) *DataFrame {
	if _, ok := df.columns[name]; !ok {
		return df
	}
	delete(df.columns, name)
	newOrder := make([]string, 0, len(df.colOrder)-1)
	for _, col := range df.colOrder {
		if col != name {
			newOrder = append(newOrder, col)
		}
	}
	df.colOrder = newOrder
	return df
}

// RenameCol renames a column. Panics if old name not found.
func (df *DataFrame) RenameCol(oldName, newName string) *DataFrame {
	s, ok := df.columns[oldName]
	if !ok {
		panic(fmt.Sprintf("RenameCol: column %q not found", oldName))
	}
	delete(df.columns, oldName)
	s.name = newName
	df.columns[newName] = s
	for i, name := range df.colOrder {
		if name == oldName {
			df.colOrder[i] = newName
			break
		}
	}
	return df
}

// SelectCols returns a new DataFrame with only the specified columns.
func (df *DataFrame) SelectCols(names []string) *DataFrame {
	ndf := &DataFrame{
		columns:  make(map[string]*Series, len(names)),
		colOrder: make([]string, 0, len(names)),
		nRows:    df.nRows,
	}
	for _, name := range names {
		if s, ok := df.columns[name]; ok {
			ndf.columns[name] = s
			ndf.colOrder = append(ndf.colOrder, name)
		}
	}
	return ndf
}

// ReorderCols returns a new DataFrame with columns in the specified order.
// Columns not in the list are appended at the end in their original order.
func (df *DataFrame) ReorderCols(names []string) *DataFrame {
	seen := make(map[string]bool, len(names))
	newOrder := make([]string, 0, len(df.colOrder))
	for _, name := range names {
		if _, ok := df.columns[name]; ok && !seen[name] {
			newOrder = append(newOrder, name)
			seen[name] = true
		}
	}
	for _, name := range df.colOrder {
		if !seen[name] {
			newOrder = append(newOrder, name)
		}
	}

	ndf := &DataFrame{
		columns:  df.columns,
		colOrder: newOrder,
		nRows:    df.nRows,
	}
	return ndf
}

// Copy returns a deep copy of the DataFrame.
func (df *DataFrame) Copy() *DataFrame {
	ndf := &DataFrame{
		columns:  make(map[string]*Series, len(df.colOrder)),
		colOrder: make([]string, len(df.colOrder)),
		nRows:    df.nRows,
	}
	copy(ndf.colOrder, df.colOrder)
	for name, s := range df.columns {
		ndf.columns[name] = s.Copy()
	}
	return ndf
}

// Filter returns a new DataFrame with only rows where mask[i] is true.
func (df *DataFrame) Filter(mask []bool) *DataFrame {
	count := 0
	for _, v := range mask {
		if v {
			count++
		}
	}

	ndf := &DataFrame{
		columns:  make(map[string]*Series, len(df.colOrder)),
		colOrder: make([]string, len(df.colOrder)),
		nRows:    count,
	}
	copy(ndf.colOrder, df.colOrder)
	for name, s := range df.columns {
		ndf.columns[name] = s.Filter(mask)
	}
	return ndf
}

// ILoc returns the row at index i as a map.
func (df *DataFrame) ILoc(i int) map[string]any {
	row := make(map[string]any, len(df.colOrder))
	for _, name := range df.colOrder {
		row[name] = df.columns[name].Any(i)
	}
	return row
}

// Head returns the first n rows.
func (df *DataFrame) Head(n int) *DataFrame {
	if n > df.nRows {
		n = df.nRows
	}
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	return df.Take(indices)
}

// Tail returns the last n rows.
func (df *DataFrame) Tail(n int) *DataFrame {
	if n > df.nRows {
		n = df.nRows
	}
	indices := make([]int, n)
	start := df.nRows - n
	for i := range indices {
		indices[i] = start + i
	}
	return df.Take(indices)
}

// Take returns a new DataFrame with rows at the specified indices.
func (df *DataFrame) Take(indices []int) *DataFrame {
	ndf := &DataFrame{
		columns:  make(map[string]*Series, len(df.colOrder)),
		colOrder: make([]string, len(df.colOrder)),
		nRows:    len(indices),
	}
	copy(ndf.colOrder, df.colOrder)
	for name, s := range df.columns {
		ndf.columns[name] = s.Take(indices)
	}
	return ndf
}

// Apply applies fn to each row and returns the results as a new Series.
func (df *DataFrame) Apply(fn func(row map[string]any) any) *Series {
	results := make([]any, df.nRows)
	for i := 0; i < df.nRows; i++ {
		row := df.ILoc(i)
		results[i] = fn(row)
	}

	// Infer type from first non-nil result
	var dtype DType = Any
	for _, v := range results {
		if v == nil {
			continue
		}
		switch v.(type) {
		case float64, float32, int, int64, int32:
			dtype = Float64
		case string:
			dtype = String
		case time.Time:
			dtype = DateTime
		case bool:
			dtype = Bool
		}
		break
	}

	switch dtype {
	case Float64:
		vals := make([]float64, len(results))
		for i, v := range results {
			if v == nil {
				vals[i] = math.NaN()
			} else if f, ok := toFloat64(v); ok {
				vals[i] = f
			} else {
				vals[i] = math.NaN()
			}
		}
		return NewFloat64Series("", vals)
	case String:
		vals := make([]string, len(results))
		nulls := make([]bool, len(results))
		for i, v := range results {
			if v == nil {
				nulls[i] = true
			} else if sv, ok := v.(string); ok {
				vals[i] = sv
			} else {
				vals[i] = fmt.Sprintf("%v", v)
			}
		}
		return NewStringSeriesFromSlice("", vals, nulls)
	default:
		return NewAnySeries("", results)
	}
}

// DropDuplicates returns a new DataFrame with duplicate rows removed.
// If cols is empty, all columns are considered.
func (df *DataFrame) DropDuplicates(cols ...string) *DataFrame {
	if len(cols) == 0 {
		cols = df.colOrder
	}

	seen := make(map[string]struct{})
	mask := make([]bool, df.nRows)

	for i := 0; i < df.nRows; i++ {
		key := df.rowKey(i, cols)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			mask[i] = true
		}
	}

	return df.Filter(mask)
}

// ResetIndex is a no-op that returns the DataFrame itself (Go DataFrames
// don't have a hierarchical index like pandas). Included for API compatibility.
func (df *DataFrame) ResetIndex() *DataFrame {
	return df
}

// Records returns the DataFrame as a slice of maps.
func (df *DataFrame) Records() []map[string]any {
	records := make([]map[string]any, df.nRows)
	for i := 0; i < df.nRows; i++ {
		records[i] = df.ILoc(i)
	}
	return records
}

// --- Internal helpers ---

// rowKey builds a composite string key for a row using the given columns.
func (df *DataFrame) rowKey(i int, cols []string) string {
	parts := make([]string, len(cols))
	for j, col := range cols {
		s := df.columns[col]
		parts[j] = s.stringKey(i)
	}
	return fmt.Sprintf("%v", parts)
}
