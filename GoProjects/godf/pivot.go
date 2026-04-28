package godf

import (
	"math"
)

// PivotTable pivots a long-format DataFrame to wide format.
//
// Parameters:
//   - indexCols: columns that form the row identity (e.g., ["timestamp", "entity_key"])
//   - columnCol: column whose values become the new column names
//   - valueCol:  column whose values fill the cells
//   - aggFunc:   aggregation when duplicates exist ("first", "mean", "sum", "max", "min")
//
// This mirrors pandas pivot_table().
func (df *DataFrame) PivotTable(indexCols []string, columnCol, valueCol, aggFunc string) *DataFrame {
	if df.Empty() {
		return NewDataFrame(nil)
	}

	colSeries := df.Col(columnCol)
	valSeries := df.Col(valueCol)

	// Collect unique pivot column values (in order of first appearance)
	pivotColOrder := make([]string, 0)
	pivotColSet := make(map[string]struct{})
	for i := 0; i < df.nRows; i++ {
		var pv string
		if colSeries.Dtype() == String {
			pv = colSeries.Str(i)
		} else {
			pv = colSeries.stringKey(i)
		}
		if _, ok := pivotColSet[pv]; !ok {
			pivotColSet[pv] = struct{}{}
			pivotColOrder = append(pivotColOrder, pv)
		}
	}

	// Build row groups by index columns
	type rowGroup struct {
		keyValues map[string]any
		// pivotCol → list of values for aggregation
		cells map[string][]float64
	}

	rowGroupOrder := make([]string, 0)
	rowGroupMap := make(map[string]*rowGroup)

	for i := 0; i < df.nRows; i++ {
		rowKey := df.rowKey(i, indexCols)

		rg, exists := rowGroupMap[rowKey]
		if !exists {
			kv := make(map[string]any, len(indexCols))
			for _, col := range indexCols {
				kv[col] = df.columns[col].Any(i)
			}
			rg = &rowGroup{
				keyValues: kv,
				cells:     make(map[string][]float64),
			}
			rowGroupMap[rowKey] = rg
			rowGroupOrder = append(rowGroupOrder, rowKey)
		}

		var pv string
		if colSeries.Dtype() == String {
			pv = colSeries.Str(i)
		} else {
			pv = colSeries.stringKey(i)
		}

		var val float64
		if valSeries.Dtype() == Float64 {
			val = valSeries.Float(i)
		} else if f, ok := toFloat64(valSeries.Any(i)); ok {
			val = f
		} else {
			val = math.NaN()
		}

		rg.cells[pv] = append(rg.cells[pv], val)
	}

	// Build output records
	records := make([]map[string]any, len(rowGroupOrder))
	for ri, rowKey := range rowGroupOrder {
		rg := rowGroupMap[rowKey]
		row := make(map[string]any, len(indexCols)+len(pivotColOrder))

		for k, v := range rg.keyValues {
			row[k] = v
		}

		for _, pc := range pivotColOrder {
			vals, ok := rg.cells[pc]
			if !ok || len(vals) == 0 {
				row[pc] = math.NaN()
				continue
			}
			row[pc] = aggregate(vals, aggFunc)
		}

		records[ri] = row
	}

	// Build DataFrame with controlled column order: indexCols first, then pivotCols
	result := NewDataFrame(records)
	return result.ReorderCols(append(indexCols, pivotColOrder...))
}

// Melt converts a wide-format DataFrame to long format.
//
// Parameters:
//   - idVars:    columns to keep as identifiers
//   - varName:   name for the variable column (default: "variable")
//   - valueName: name for the value column (default: "value")
//
// All columns not in idVars are melted.
func (df *DataFrame) Melt(idVars []string, varName, valueName string) *DataFrame {
	if varName == "" {
		varName = "variable"
	}
	if valueName == "" {
		valueName = "value"
	}

	idSet := make(map[string]bool, len(idVars))
	for _, col := range idVars {
		idSet[col] = true
	}

	valueCols := make([]string, 0)
	for _, col := range df.colOrder {
		if !idSet[col] {
			valueCols = append(valueCols, col)
		}
	}

	nOutputRows := df.nRows * len(valueCols)
	records := make([]map[string]any, 0, nOutputRows)

	for i := 0; i < df.nRows; i++ {
		for _, vc := range valueCols {
			row := make(map[string]any, len(idVars)+2)
			for _, id := range idVars {
				row[id] = df.columns[id].Any(i)
			}
			row[varName] = vc
			row[valueName] = df.columns[vc].Any(i)
			records = append(records, row)
		}
	}

	return NewDataFrame(records)
}

func aggregate(vals []float64, aggFunc string) float64 {
	if len(vals) == 0 {
		return math.NaN()
	}

	switch aggFunc {
	case "first":
		for _, v := range vals {
			if !math.IsNaN(v) {
				return v
			}
		}
		return math.NaN()
	case "last":
		for i := len(vals) - 1; i >= 0; i-- {
			if !math.IsNaN(vals[i]) {
				return vals[i]
			}
		}
		return math.NaN()
	case "mean":
		sum := 0.0
		n := 0
		for _, v := range vals {
			if !math.IsNaN(v) {
				sum += v
				n++
			}
		}
		if n == 0 {
			return math.NaN()
		}
		return sum / float64(n)
	case "sum":
		sum := 0.0
		for _, v := range vals {
			if !math.IsNaN(v) {
				sum += v
			}
		}
		return sum
	case "min":
		min := math.Inf(1)
		found := false
		for _, v := range vals {
			if !math.IsNaN(v) {
				if v < min {
					min = v
				}
				found = true
			}
		}
		if !found {
			return math.NaN()
		}
		return min
	case "max":
		max := math.Inf(-1)
		found := false
		for _, v := range vals {
			if !math.IsNaN(v) {
				if v > max {
					max = v
				}
				found = true
			}
		}
		if !found {
			return math.NaN()
		}
		return max
	case "count":
		n := 0
		for _, v := range vals {
			if !math.IsNaN(v) {
				n++
			}
		}
		return float64(n)
	default:
		return vals[0]
	}
}
