package godf

import (
	"math"
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	records := []map[string]any{
		{"a": 1.0, "b": "x"},
		{"a": 2.0, "b": "y"},
		{"a": 3.0, "b": "z"},
	}
	df := NewDataFrame(records)

	if df.NRows() != 3 {
		t.Fatalf("NRows: got %d, want 3", df.NRows())
	}
	if df.NCols() != 2 {
		t.Fatalf("NCols: got %d, want 2", df.NCols())
	}
	if df.Empty() {
		t.Error("Empty: should not be empty")
	}
}

func TestDataFrameEmpty(t *testing.T) {
	df := NewDataFrame(nil)
	if !df.Empty() {
		t.Error("Empty: should be empty")
	}
	if df.NRows() != 0 {
		t.Errorf("NRows: got %d, want 0", df.NRows())
	}
}

func TestDataFrameCol(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"x": 10.0, "y": 20.0},
		{"x": 30.0, "y": 40.0},
	})
	col := df.Col("x")
	assertFloat(t, "Col[0]", col.Float(0), 10.0)
	assertFloat(t, "Col[1]", col.Float(1), 30.0)
}

func TestDataFrameSetCol(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0},
		{"a": 2.0},
	})
	s := NewFloat64Series("b", []float64{10, 20})
	df.SetCol("b", s)

	if df.NCols() != 2 {
		t.Errorf("NCols after SetCol: got %d, want 2", df.NCols())
	}
	assertFloat(t, "SetCol[0]", df.Col("b").Float(0), 10.0)
}

func TestDataFrameDropCol(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 2.0, "c": 3.0},
	})
	df.DropCol("b")
	if df.NCols() != 2 {
		t.Errorf("NCols after DropCol: got %d, want 2", df.NCols())
	}
	if df.HasColumn("b") {
		t.Error("HasColumn('b') should be false after DropCol")
	}
}

func TestDataFrameFilter(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": "x"},
		{"a": 2.0, "b": "y"},
		{"a": 3.0, "b": "z"},
		{"a": 4.0, "b": "w"},
	})
	mask := []bool{true, false, true, false}
	filtered := df.Filter(mask)

	if filtered.NRows() != 2 {
		t.Fatalf("Filter NRows: got %d, want 2", filtered.NRows())
	}
	assertFloat(t, "Filter[0]", filtered.Col("a").Float(0), 1.0)
	assertFloat(t, "Filter[1]", filtered.Col("a").Float(1), 3.0)
}

func TestDataFrameILoc(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": "hello"},
	})
	row := df.ILoc(0)
	if row["a"] != 1.0 {
		t.Errorf("ILoc a: got %v, want 1.0", row["a"])
	}
	if row["b"] != "hello" {
		t.Errorf("ILoc b: got %v, want 'hello'", row["b"])
	}
}

func TestDataFrameCopy(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0},
		{"a": 2.0},
	})
	cp := df.Copy()
	cp.Col("a").floats[0] = 99
	if df.Col("a").Float(0) != 1.0 {
		t.Error("Copy: mutation leaked to original")
	}
}

func TestDataFrameSelectCols(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 2.0, "c": 3.0},
	})
	selected := df.SelectCols([]string{"c", "a"})
	cols := selected.Columns()
	if len(cols) != 2 || cols[0] != "c" || cols[1] != "a" {
		t.Errorf("SelectCols: got %v, want [c, a]", cols)
	}
}

func TestDataFrameReorderCols(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 2.0, "c": 3.0},
	})
	reordered := df.ReorderCols([]string{"c", "a"})
	cols := reordered.Columns()
	if cols[0] != "c" || cols[1] != "a" || cols[2] != "b" {
		t.Errorf("ReorderCols: got %v, want [c, a, b]", cols)
	}
}

func TestDataFrameDropDuplicates(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": "x", "b": 1.0},
		{"a": "y", "b": 2.0},
		{"a": "x", "b": 1.0},
		{"a": "y", "b": 2.0},
		{"a": "z", "b": 3.0},
	})
	deduped := df.DropDuplicates()
	if deduped.NRows() != 3 {
		t.Errorf("DropDuplicates NRows: got %d, want 3", deduped.NRows())
	}
}

func TestDataFrameApply(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 10.0},
		{"a": 2.0, "b": 20.0},
		{"a": 3.0, "b": 30.0},
	})
	result := df.Apply(func(row map[string]any) any {
		return row["a"].(float64) + row["b"].(float64)
	})
	assertFloatSlice(t, "Apply", result, []float64{11, 22, 33})
}

func TestDataFrameHeadTail(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"v": 1.0}, {"v": 2.0}, {"v": 3.0}, {"v": 4.0}, {"v": 5.0},
	})

	head := df.Head(3)
	if head.NRows() != 3 {
		t.Errorf("Head NRows: got %d, want 3", head.NRows())
	}
	assertFloat(t, "Head[0]", head.Col("v").Float(0), 1.0)

	tail := df.Tail(2)
	if tail.NRows() != 2 {
		t.Errorf("Tail NRows: got %d, want 2", tail.NRows())
	}
	assertFloat(t, "Tail[0]", tail.Col("v").Float(0), 4.0)
}

func TestDataFrameRenameCol(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"old": 1.0},
	})
	df.RenameCol("old", "new")
	if !df.HasColumn("new") {
		t.Error("RenameCol: 'new' column not found")
	}
	if df.HasColumn("old") {
		t.Error("RenameCol: 'old' column still present")
	}
}

func TestDataFrameRecords(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": "x"},
		{"a": 2.0, "b": "y"},
	})
	recs := df.Records()
	if len(recs) != 2 {
		t.Fatalf("Records: got %d, want 2", len(recs))
	}
	if recs[0]["a"] != 1.0 {
		t.Errorf("Records[0][a]: got %v, want 1.0", recs[0]["a"])
	}
}

func TestDataFrameNilValues(t *testing.T) {
	records := []map[string]any{
		{"a": 1.0, "b": "x"},
		{"a": nil, "b": nil},
	}
	df := NewDataFrame(records)
	if !math.IsNaN(df.Col("a").Float(1)) {
		t.Error("nil should become NaN for float64")
	}
	if !df.Col("b").IsNull(1) {
		t.Error("nil should be null for string")
	}
}

func TestDataFrameMissingKeys(t *testing.T) {
	records := []map[string]any{
		{"a": 1.0, "b": 2.0},
		{"a": 3.0},
	}
	df := NewDataFrame(records)
	if !math.IsNaN(df.Col("b").Float(1)) {
		t.Error("Missing key should become NaN for float64")
	}
}
