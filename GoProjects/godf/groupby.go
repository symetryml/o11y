package godf

import (
	"fmt"
	"math"
)

// GroupedDataFrame represents the result of a GroupBy operation.
type GroupedDataFrame struct {
	keys     []string            // column names used for grouping
	groups   []groupEntry        // ordered list of groups
	groupMap map[string]int      // key string → index into groups
	parent   *DataFrame          // the original DataFrame
}

type groupEntry struct {
	keyValues []any   // the actual values of the group key columns
	keyStr    string  // composite key string
	indices   []int   // row indices in parent DataFrame
}

// GroupBy groups the DataFrame by the given columns.
func (df *DataFrame) GroupBy(cols ...string) *GroupedDataFrame {
	for _, col := range cols {
		if !df.HasColumn(col) {
			panic(fmt.Sprintf("GroupBy: column %q not found", col))
		}
	}

	gdf := &GroupedDataFrame{
		keys:     cols,
		groupMap: make(map[string]int),
		parent:   df,
	}

	for i := 0; i < df.nRows; i++ {
		key := df.rowKey(i, cols)
		idx, exists := gdf.groupMap[key]
		if !exists {
			keyVals := make([]any, len(cols))
			for j, col := range cols {
				keyVals[j] = df.columns[col].Any(i)
			}
			idx = len(gdf.groups)
			gdf.groups = append(gdf.groups, groupEntry{
				keyValues: keyVals,
				keyStr:    key,
				indices:   nil,
			})
			gdf.groupMap[key] = idx
		}
		gdf.groups[idx].indices = append(gdf.groups[idx].indices, i)
	}

	return gdf
}

// NGroups returns the number of groups.
func (g *GroupedDataFrame) NGroups() int {
	return len(g.groups)
}

// ForEach iterates over each group, calling fn with the group key values and sub-DataFrame.
func (g *GroupedDataFrame) ForEach(fn func(keyValues []any, group *DataFrame)) {
	for _, ge := range g.groups {
		sub := g.parent.Take(ge.indices)
		fn(ge.keyValues, sub)
	}
}

// Iter returns a slice of (keyValues, sub-DataFrame) pairs.
func (g *GroupedDataFrame) Iter() []struct {
	Key   []any
	Group *DataFrame
} {
	result := make([]struct {
		Key   []any
		Group *DataFrame
	}, len(g.groups))
	for i, ge := range g.groups {
		result[i].Key = ge.keyValues
		result[i].Group = g.parent.Take(ge.indices)
	}
	return result
}

// Agg performs named aggregations on the grouped DataFrame.
//
// specs maps column_name → aggregation_function. Supported functions:
// "mean", "sum", "min", "max", "std", "count", "first", "last", "nunique".
func (g *GroupedDataFrame) Agg(specs map[string]string) *DataFrame {
	nGroups := len(g.groups)
	if nGroups == 0 {
		return NewDataFrame(nil)
	}

	// Prepare result columns: group key columns + aggregated columns
	records := make([]map[string]any, nGroups)

	for gi, ge := range g.groups {
		row := make(map[string]any)
		for j, col := range g.keys {
			row[col] = ge.keyValues[j]
		}

		sub := g.parent.Take(ge.indices)

		for col, aggFn := range specs {
			s := sub.Col(col)
			row[col] = applyAgg(s, aggFn)
		}

		records[gi] = row
	}

	return NewDataFrame(records)
}

// AggMulti performs multiple aggregations per column.
//
// specs maps column_name → list of aggregation functions.
// Output columns are named "column__aggfunc".
func (g *GroupedDataFrame) AggMulti(specs map[string][]string) *DataFrame {
	nGroups := len(g.groups)
	if nGroups == 0 {
		return NewDataFrame(nil)
	}

	records := make([]map[string]any, nGroups)

	for gi, ge := range g.groups {
		row := make(map[string]any)
		for j, col := range g.keys {
			row[col] = ge.keyValues[j]
		}

		sub := g.parent.Take(ge.indices)

		for col, aggFns := range specs {
			s := sub.Col(col)
			for _, aggFn := range aggFns {
				outCol := fmt.Sprintf("%s__%s", col, aggFn)
				row[outCol] = applyAgg(s, aggFn)
			}
		}

		records[gi] = row
	}

	return NewDataFrame(records)
}

// Shift shifts a column within each group by n positions.
// Returns a Series aligned to the original DataFrame's row order.
func (g *GroupedDataFrame) Shift(col string, n int) *Series {
	src := g.parent.Col(col)
	if src.Dtype() != Float64 {
		panic("GroupedDataFrame.Shift only supports Float64 series")
	}

	result := make([]float64, g.parent.NRows())
	for i := range result {
		result[i] = math.NaN()
	}

	for _, ge := range g.groups {
		for j, origIdx := range ge.indices {
			srcIdx := j - n
			if srcIdx >= 0 && srcIdx < len(ge.indices) {
				result[origIdx] = src.Float(ge.indices[srcIdx])
			}
		}
	}

	return NewFloat64Series(col, result)
}

// Transform applies a function within each group and returns a Series
// aligned to the original DataFrame's row order.
func (g *GroupedDataFrame) Transform(col string, fn func(s *Series) *Series) *Series {
	src := g.parent.Col(col)
	dtype := src.Dtype()

	// Build result with same length as parent
	switch dtype {
	case Float64:
		result := make([]float64, g.parent.NRows())
		for i := range result {
			result[i] = math.NaN()
		}
		for _, ge := range g.groups {
			groupSeries := src.Take(ge.indices)
			transformed := fn(groupSeries)
			for j, origIdx := range ge.indices {
				if j < transformed.Len() {
					result[origIdx] = transformed.Float(j)
				}
			}
		}
		return NewFloat64Series(col, result)
	default:
		panic(fmt.Sprintf("Transform not implemented for dtype %v", dtype))
	}
}

func applyAgg(s *Series, aggFn string) any {
	switch aggFn {
	case "mean":
		return s.Mean()
	case "sum":
		return s.Sum()
	case "min":
		return s.Min()
	case "max":
		return s.Max()
	case "std":
		return s.Std()
	case "count":
		return float64(s.Count())
	case "first":
		if s.Dtype() == Float64 {
			return s.First()
		}
		for i := 0; i < s.Len(); i++ {
			if !s.IsNull(i) {
				return s.Any(i)
			}
		}
		return nil
	case "last":
		if s.Dtype() == Float64 {
			return s.Last()
		}
		for i := s.Len() - 1; i >= 0; i-- {
			if !s.IsNull(i) {
				return s.Any(i)
			}
		}
		return nil
	case "nunique":
		return float64(s.NUnique())
	default:
		panic(fmt.Sprintf("unknown aggregation function: %q", aggFn))
	}
}
