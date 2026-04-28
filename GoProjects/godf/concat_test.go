package godf

import (
	"math"
	"testing"
)

func TestConcatCols(t *testing.T) {
	df1 := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 3.0},
		{"a": 2.0, "b": 4.0},
	})
	df2 := NewDataFrame([]map[string]any{
		{"c": 5.0, "d": 7.0},
		{"c": 6.0, "d": 8.0},
	})

	result := Concat([]*DataFrame{df1, df2}, 1)
	cols := result.Columns()
	if len(cols) != 4 {
		t.Fatalf("Concat cols: got %d, want 4", len(cols))
	}
	if result.NRows() != 2 {
		t.Fatalf("Concat NRows: got %d, want 2", result.NRows())
	}
	assertFloat(t, "a[0]", result.Col("a").Float(0), 1.0)
	assertFloat(t, "d[1]", result.Col("d").Float(1), 8.0)
}

func TestConcatRows(t *testing.T) {
	df1 := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": "x"},
	})
	df2 := NewDataFrame([]map[string]any{
		{"a": 2.0, "b": "y"},
		{"a": 3.0, "b": "z"},
	})

	result := Concat([]*DataFrame{df1, df2}, 0)
	if result.NRows() != 3 {
		t.Fatalf("ConcatRows NRows: got %d, want 3", result.NRows())
	}
	assertFloat(t, "Row[2] a", result.Col("a").Float(2), 3.0)
	if result.Col("b").Str(2) != "z" {
		t.Errorf("Row[2] b: got %s, want z", result.Col("b").Str(2))
	}
}

func TestConcatRowsMismatchedCols(t *testing.T) {
	df1 := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 2.0},
	})
	df2 := NewDataFrame([]map[string]any{
		{"a": 3.0, "c": 4.0},
	})

	result := Concat([]*DataFrame{df1, df2}, 0)
	if result.NRows() != 2 {
		t.Fatalf("ConcatMismatch NRows: got %d, want 2", result.NRows())
	}
	// df1 row should have NaN for "c"
	assertFloat(t, "df1 c", result.Col("c").Float(0), math.NaN())
	// df2 row should have NaN for "b"
	assertFloat(t, "df2 b", result.Col("b").Float(1), math.NaN())
}

func TestConcatEmpty(t *testing.T) {
	result := Concat(nil, 0)
	if !result.Empty() {
		t.Error("Concat nil should be empty")
	}
}

func TestConcatSingle(t *testing.T) {
	df := NewDataFrame([]map[string]any{{"a": 1.0}})
	result := Concat([]*DataFrame{df}, 0)
	if result.NRows() != 1 {
		t.Errorf("ConcatSingle NRows: got %d, want 1", result.NRows())
	}
}
