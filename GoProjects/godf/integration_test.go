package godf

import (
	"encoding/json"
	"math"
	"os"
	"testing"
)

// loadExpected loads the pandas-generated expected results.
func loadExpected(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile("testdata/expected.json")
	if err != nil {
		t.Fatalf("Failed to read testdata/expected.json: %v", err)
	}
	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatalf("Failed to parse expected.json: %v", err)
	}
	return result
}

// getFloatSlice extracts a float slice from JSON (null → NaN).
func getFloatSlice(v any) []float64 {
	arr := v.([]any)
	result := make([]float64, len(arr))
	for i, val := range arr {
		if val == nil {
			result[i] = math.NaN()
		} else {
			result[i] = val.(float64)
		}
	}
	return result
}

func getBoolSlice(v any) []bool {
	arr := v.([]any)
	result := make([]bool, len(arr))
	for i, val := range arr {
		result[i] = val.(bool)
	}
	return result
}

func getStringSlice(v any) []string {
	arr := v.([]any)
	result := make([]string, len(arr))
	for i, val := range arr {
		result[i] = val.(string)
	}
	return result
}

// TestIntegrationSeriesAgg validates aggregation against pandas output.
func TestIntegrationSeriesAgg(t *testing.T) {
	expected := loadExpected(t)
	agg := expected["series_agg"].(map[string]any)

	s := NewFloat64Series("test", []float64{1, 2, 3, math.NaN(), 5})
	assertFloat(t, "mean", s.Mean(), agg["mean"].(float64))
	assertFloat(t, "sum", s.Sum(), agg["sum"].(float64))
	assertFloat(t, "min", s.Min(), agg["min"].(float64))
	assertFloat(t, "max", s.Max(), agg["max"].(float64))
	assertFloat(t, "std", s.Std(), agg["std"].(float64))
	if s.Count() != int(agg["count"].(float64)) {
		t.Errorf("count: got %d, want %v", s.Count(), agg["count"])
	}
}

func TestIntegrationShift(t *testing.T) {
	expected := loadExpected(t)

	s := NewFloat64Series("val", []float64{10, 20, 30, 40, 50})

	fwd := s.Shift(2)
	wantFwd := getFloatSlice(expected["shift_forward_2"])
	assertFloatSlice(t, "ShiftFwd", fwd, wantFwd)

	bwd := s.Shift(-1)
	wantBwd := getFloatSlice(expected["shift_backward_1"])
	assertFloatSlice(t, "ShiftBwd", bwd, wantBwd)
}

func TestIntegrationRolling(t *testing.T) {
	expected := loadExpected(t)

	s := NewFloat64Series("val", []float64{1, 2, 3, 4, 5})

	mean := s.Rolling(3, 1).Mean()
	wantMean := getFloatSlice(expected["rolling_mean_3_1"])
	assertFloatSlice(t, "RollingMean", mean, wantMean)

	std := s.Rolling(3, 2).Std()
	wantStd := getFloatSlice(expected["rolling_std_3_2"])
	assertFloatSlice(t, "RollingStd", std, wantStd)
}

func TestIntegrationGroupByShift(t *testing.T) {
	expected := loadExpected(t)

	df := NewDataFrame([]map[string]any{
		{"entity": "A", "value": 10.0},
		{"entity": "A", "value": 20.0},
		{"entity": "A", "value": 30.0},
		{"entity": "B", "value": 100.0},
		{"entity": "B", "value": 200.0},
		{"entity": "B", "value": 300.0},
	})

	shifted := df.GroupBy("entity").Shift("value", 1)
	want := getFloatSlice(expected["groupby_shift_1"])
	assertFloatSlice(t, "GBShift1", shifted, want)
}

func TestIntegrationGroupByAgg(t *testing.T) {
	expected := loadExpected(t)
	aggExp := expected["groupby_agg"].(map[string]any)

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

	groups := getStringSlice(aggExp["groups"])
	wantMean := getFloatSlice(aggExp["mean"])
	wantSum := getFloatSlice(aggExp["sum"])
	wantMin := getFloatSlice(aggExp["min"])
	wantMax := getFloatSlice(aggExp["max"])
	wantStd := getFloatSlice(aggExp["std"])

	for i := 0; i < result.NRows(); i++ {
		grp := result.Col("group").Str(i)
		for gi, g := range groups {
			if grp == g {
				assertFloat(t, grp+" mean", result.Col("val__mean").Float(i), wantMean[gi])
				assertFloat(t, grp+" sum", result.Col("val__sum").Float(i), wantSum[gi])
				assertFloat(t, grp+" min", result.Col("val__min").Float(i), wantMin[gi])
				assertFloat(t, grp+" max", result.Col("val__max").Float(i), wantMax[gi])
				assertFloat(t, grp+" std", result.Col("val__std").Float(i), wantStd[gi])
			}
		}
	}
}

func TestIntegrationPivot(t *testing.T) {
	expected := loadExpected(t)
	pivotExp := expected["pivot"].(map[string]any)

	df := NewDataFrame([]map[string]any{
		{"timestamp": "t1", "entity": "A", "feature": "cpu", "value": 0.5},
		{"timestamp": "t1", "entity": "A", "feature": "mem", "value": 0.8},
		{"timestamp": "t1", "entity": "A", "feature": "disk", "value": 0.3},
		{"timestamp": "t2", "entity": "A", "feature": "cpu", "value": 0.6},
		{"timestamp": "t2", "entity": "A", "feature": "mem", "value": 0.7},
		{"timestamp": "t2", "entity": "A", "feature": "disk", "value": 0.4},
	})

	pivoted := df.PivotTable(
		[]string{"timestamp", "entity"},
		"feature", "value", "first",
	)

	wantNRows := int(pivotExp["nrows"].(float64))
	if pivoted.NRows() != wantNRows {
		t.Fatalf("Pivot NRows: got %d, want %d", pivoted.NRows(), wantNRows)
	}

	wantCPU := getFloatSlice(pivotExp["cpu"])
	wantMem := getFloatSlice(pivotExp["mem"])
	wantDisk := getFloatSlice(pivotExp["disk"])

	for i := range wantCPU {
		assertFloat(t, "cpu", pivoted.Col("cpu").Float(i), wantCPU[i])
		assertFloat(t, "mem", pivoted.Col("mem").Float(i), wantMem[i])
		assertFloat(t, "disk", pivoted.Col("disk").Float(i), wantDisk[i])
	}
}

func TestIntegrationMelt(t *testing.T) {
	expected := loadExpected(t)
	meltExp := expected["melt"].(map[string]any)

	df := NewDataFrame([]map[string]any{
		{"timestamp": "t1", "cpu": 0.5, "mem": 0.8},
		{"timestamp": "t2", "cpu": 0.6, "mem": 0.7},
	})

	melted := df.Melt([]string{"timestamp"}, "feature", "value")

	wantNRows := int(meltExp["nrows"].(float64))
	if melted.NRows() != wantNRows {
		t.Fatalf("Melt NRows: got %d, want %d", melted.NRows(), wantNRows)
	}

	// Check value sum matches
	wantValues := getFloatSlice(meltExp["value"])
	wantSum := 0.0
	for _, v := range wantValues {
		if !math.IsNaN(v) {
			wantSum += v
		}
	}
	gotSum := melted.Col("value").Sum()
	assertFloat(t, "Melt value sum", gotSum, wantSum)
}

func TestIntegrationDeltaShift2(t *testing.T) {
	expected := loadExpected(t)

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

	df = df.SortBy("entity")
	shifted := df.GroupBy("entity").Shift("value", 2)
	delta := df.Col("value").Sub(shifted)

	want := getFloatSlice(expected["delta_shift2"])
	assertFloatSlice(t, "DeltaShift2", delta, want)
}

func TestIntegrationReplaceInf(t *testing.T) {
	expected := loadExpected(t)

	s := NewFloat64Series("val", []float64{1, math.Inf(1), math.Inf(-1), 3})
	replaced := s.ReplaceInf(math.NaN())

	want := getFloatSlice(expected["replace_inf"])
	assertFloatSlice(t, "ReplaceInf", replaced, want)
}

func TestIntegrationDropDuplicates(t *testing.T) {
	expected := loadExpected(t)
	ddExp := expected["drop_duplicates"].(map[string]any)

	df := NewDataFrame([]map[string]any{
		{"a": "x", "b": 1.0},
		{"a": "y", "b": 2.0},
		{"a": "x", "b": 1.0},
		{"a": "y", "b": 2.0},
		{"a": "z", "b": 3.0},
	})

	deduped := df.DropDuplicates()
	wantNRows := int(ddExp["nrows"].(float64))
	if deduped.NRows() != wantNRows {
		t.Errorf("DropDuplicates NRows: got %d, want %d", deduped.NRows(), wantNRows)
	}
}

func TestIntegrationEndsWith(t *testing.T) {
	expected := loadExpected(t)

	s := NewStringSeries("metric", "metric_total", "metric_bucket", "metric_count", "other_total")
	got := s.EndsWith("_total")
	want := getBoolSlice(expected["endswith_total"])
	assertBoolSlice(t, "EndsWith", got, want)
}

func TestIntegrationSort(t *testing.T) {
	expected := loadExpected(t)
	sortExp := expected["sort_by_a_b"].(map[string]any)

	df := NewDataFrame([]map[string]any{
		{"a": "B", "b": 2.0},
		{"a": "A", "b": 1.0},
		{"a": "C", "b": 3.0},
		{"a": "A", "b": 0.0},
	})

	sorted := df.SortBy("a", "b")

	wantA := getStringSlice(sortExp["a"])
	wantB := getFloatSlice(sortExp["b"])

	for i, w := range wantA {
		if sorted.Col("a").Str(i) != w {
			t.Errorf("Sort a[%d]: got %s, want %s", i, sorted.Col("a").Str(i), w)
		}
	}
	assertFloatSlice(t, "Sort b", sorted.Col("b"), wantB)
}

func TestIntegrationConcatCols(t *testing.T) {
	expected := loadExpected(t)
	ccExp := expected["concat_cols"].(map[string]any)

	df1 := NewDataFrame([]map[string]any{
		{"a": 1.0, "b": 3.0},
		{"a": 2.0, "b": 4.0},
	})
	df2 := NewDataFrame([]map[string]any{
		{"c": 5.0, "d": 7.0},
		{"c": 6.0, "d": 8.0},
	})

	result := Concat([]*DataFrame{df1, df2}, 1)

	wantCols := getStringSlice(ccExp["columns"])
	gotCols := result.Columns()

	if len(gotCols) != len(wantCols) {
		t.Fatalf("ConcatCols: got %d cols, want %d", len(gotCols), len(wantCols))
	}

	wantNRows := int(ccExp["nrows"].(float64))
	if result.NRows() != wantNRows {
		t.Errorf("ConcatCols NRows: got %d, want %d", result.NRows(), wantNRows)
	}
}
