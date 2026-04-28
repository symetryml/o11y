package godf

import (
	"math"
	"testing"
)

func TestGroupByAgg(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"group": "X", "val": 1.0},
		{"group": "X", "val": 2.0},
		{"group": "Y", "val": 3.0},
		{"group": "Y", "val": 4.0},
		{"group": "Y", "val": 5.0},
	})

	result := df.GroupBy("group").Agg(map[string]string{
		"val": "mean",
	})

	if result.NRows() != 2 {
		t.Fatalf("GroupBy Agg NRows: got %d, want 2", result.NRows())
	}

	// Find X and Y rows
	for i := 0; i < result.NRows(); i++ {
		grp := result.Col("group").Str(i)
		val := result.Col("val").Float(i)
		switch grp {
		case "X":
			assertFloat(t, "X mean", val, 1.5)
		case "Y":
			assertFloat(t, "Y mean", val, 4.0)
		}
	}
}

func TestGroupByAggMulti(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"group": "X", "val": 1.0},
		{"group": "X", "val": 2.0},
		{"group": "Y", "val": 3.0},
		{"group": "Y", "val": 4.0},
		{"group": "Y", "val": 5.0},
	})

	result := df.GroupBy("group").AggMulti(map[string][]string{
		"val": {"mean", "sum", "min", "max", "std", "count"},
	})

	if result.NRows() != 2 {
		t.Fatalf("AggMulti NRows: got %d, want 2", result.NRows())
	}

	// Verify X group
	for i := 0; i < result.NRows(); i++ {
		grp := result.Col("group").Str(i)
		if grp == "X" {
			assertFloat(t, "X mean", result.Col("val__mean").Float(i), 1.5)
			assertFloat(t, "X sum", result.Col("val__sum").Float(i), 3.0)
			assertFloat(t, "X min", result.Col("val__min").Float(i), 1.0)
			assertFloat(t, "X max", result.Col("val__max").Float(i), 2.0)
			assertFloat(t, "X std", result.Col("val__std").Float(i), 0.7071067811865476)
			assertFloat(t, "X count", result.Col("val__count").Float(i), 2.0)
		}
		if grp == "Y" {
			assertFloat(t, "Y mean", result.Col("val__mean").Float(i), 4.0)
			assertFloat(t, "Y sum", result.Col("val__sum").Float(i), 12.0)
			assertFloat(t, "Y min", result.Col("val__min").Float(i), 3.0)
			assertFloat(t, "Y max", result.Col("val__max").Float(i), 5.0)
			assertFloat(t, "Y std", result.Col("val__std").Float(i), 1.0)
			assertFloat(t, "Y count", result.Col("val__count").Float(i), 3.0)
		}
	}
}

func TestGroupByShift(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"entity": "A", "value": 10.0},
		{"entity": "A", "value": 20.0},
		{"entity": "A", "value": 30.0},
		{"entity": "B", "value": 100.0},
		{"entity": "B", "value": 200.0},
		{"entity": "B", "value": 300.0},
	})

	shifted := df.GroupBy("entity").Shift("value", 1)
	// pandas: [NaN, 10, 20, NaN, 100, 200]
	want := []float64{math.NaN(), 10, 20, math.NaN(), 100, 200}
	assertFloatSlice(t, "GroupByShift1", shifted, want)
}

func TestGroupByShift2(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"entity": "A", "value": 10.0},
		{"entity": "A", "value": 20.0},
		{"entity": "A", "value": 30.0},
		{"entity": "A", "value": 40.0},
		{"entity": "B", "value": 100.0},
		{"entity": "B", "value": 200.0},
		{"entity": "B", "value": 300.0},
		{"entity": "B", "value": 400.0},
	})

	shifted := df.GroupBy("entity").Shift("value", 2)
	// pandas: [NaN, NaN, 10, 20, NaN, NaN, 100, 200]
	want := []float64{math.NaN(), math.NaN(), 10, 20, math.NaN(), math.NaN(), 100, 200}
	assertFloatSlice(t, "GroupByShift2", shifted, want)
}

func TestGroupByDeltaPattern(t *testing.T) {
	// Test the common pattern: delta = value - groupby("entity").shift(value, 2)
	df := NewDataFrame([]map[string]any{
		{"entity": "A", "value": 10.0},
		{"entity": "A", "value": 20.0},
		{"entity": "A", "value": 30.0},
		{"entity": "A", "value": 40.0},
		{"entity": "B", "value": 100.0},
		{"entity": "B", "value": 200.0},
		{"entity": "B", "value": 300.0},
		{"entity": "B", "value": 400.0},
	})

	shifted := df.GroupBy("entity").Shift("value", 2)
	delta := df.Col("value").Sub(shifted)
	// pandas: [NaN, NaN, 20, 20, NaN, NaN, 200, 200]
	want := []float64{math.NaN(), math.NaN(), 20, 20, math.NaN(), math.NaN(), 200, 200}
	assertFloatSlice(t, "DeltaShift2", delta, want)
}

func TestGroupByTransform(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"entity": "A", "value": 1.0},
		{"entity": "A", "value": 2.0},
		{"entity": "A", "value": 3.0},
		{"entity": "B", "value": 10.0},
		{"entity": "B", "value": 20.0},
	})

	// Rolling mean within each group
	result := df.GroupBy("entity").Transform("value", func(s *Series) *Series {
		return s.Rolling(2, 1).Mean()
	})

	// A: [1, 1.5, 2.5], B: [10, 15]
	assertFloat(t, "Transform A[0]", result.Float(0), 1.0)
	assertFloat(t, "Transform A[1]", result.Float(1), 1.5)
	assertFloat(t, "Transform A[2]", result.Float(2), 2.5)
	assertFloat(t, "Transform B[0]", result.Float(3), 10.0)
	assertFloat(t, "Transform B[1]", result.Float(4), 15.0)
}

func TestGroupByForEach(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"g": "A", "v": 1.0},
		{"g": "B", "v": 2.0},
		{"g": "A", "v": 3.0},
	})

	count := 0
	df.GroupBy("g").ForEach(func(key []any, group *DataFrame) {
		count++
		if key[0].(string) == "A" && group.NRows() != 2 {
			t.Errorf("Group A should have 2 rows, got %d", group.NRows())
		}
	})
	if count != 2 {
		t.Errorf("ForEach: expected 2 groups, got %d", count)
	}
}

func TestGroupByNGroups(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"g": "A", "v": 1.0},
		{"g": "B", "v": 2.0},
		{"g": "C", "v": 3.0},
		{"g": "A", "v": 4.0},
	})
	g := df.GroupBy("g")
	if g.NGroups() != 3 {
		t.Errorf("NGroups: got %d, want 3", g.NGroups())
	}
}

func TestGroupByMultipleKeys(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": "X", "b": "1", "val": 10.0},
		{"a": "X", "b": "2", "val": 20.0},
		{"a": "Y", "b": "1", "val": 30.0},
		{"a": "X", "b": "1", "val": 40.0},
	})

	g := df.GroupBy("a", "b")
	if g.NGroups() != 3 {
		t.Errorf("MultiKey NGroups: got %d, want 3", g.NGroups())
	}

	result := g.Agg(map[string]string{"val": "sum"})
	if result.NRows() != 3 {
		t.Errorf("MultiKey Agg NRows: got %d, want 3", result.NRows())
	}
}
