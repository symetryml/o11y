package godf

import (
	"math"
	"testing"
)

func TestPivotTable(t *testing.T) {
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

	if pivoted.NRows() != 2 {
		t.Fatalf("Pivot NRows: got %d, want 2", pivoted.NRows())
	}

	// Check t1 row
	assertFloat(t, "t1 cpu", pivoted.Col("cpu").Float(0), 0.5)
	assertFloat(t, "t1 mem", pivoted.Col("mem").Float(0), 0.8)
	assertFloat(t, "t1 disk", pivoted.Col("disk").Float(0), 0.3)

	// Check t2 row
	assertFloat(t, "t2 cpu", pivoted.Col("cpu").Float(1), 0.6)
	assertFloat(t, "t2 mem", pivoted.Col("mem").Float(1), 0.7)
	assertFloat(t, "t2 disk", pivoted.Col("disk").Float(1), 0.4)
}

func TestPivotTableWithAggregation(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"ts": "t1", "feature": "cpu", "value": 1.0},
		{"ts": "t1", "feature": "cpu", "value": 3.0},
		{"ts": "t1", "feature": "mem", "value": 10.0},
	})

	pivoted := df.PivotTable([]string{"ts"}, "feature", "value", "mean")
	assertFloat(t, "mean cpu", pivoted.Col("cpu").Float(0), 2.0)
	assertFloat(t, "mean mem", pivoted.Col("mem").Float(0), 10.0)
}

func TestPivotTableMissingValues(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"ts": "t1", "feature": "cpu", "value": 1.0},
		{"ts": "t2", "feature": "mem", "value": 2.0},
	})

	pivoted := df.PivotTable([]string{"ts"}, "feature", "value", "first")
	if pivoted.NRows() != 2 {
		t.Fatalf("Pivot NRows: got %d, want 2", pivoted.NRows())
	}

	// t1 has cpu but not mem
	assertFloat(t, "t1 cpu", pivoted.Col("cpu").Float(0), 1.0)
	assertFloat(t, "t1 mem", pivoted.Col("mem").Float(0), math.NaN())

	// t2 has mem but not cpu
	assertFloat(t, "t2 cpu", pivoted.Col("cpu").Float(1), math.NaN())
	assertFloat(t, "t2 mem", pivoted.Col("mem").Float(1), 2.0)
}

func TestPivotTableEmpty(t *testing.T) {
	df := NewDataFrame(nil)
	pivoted := df.PivotTable([]string{"ts"}, "feature", "value", "first")
	if !pivoted.Empty() {
		t.Error("Pivot of empty DataFrame should be empty")
	}
}

func TestMelt(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"timestamp": "t1", "cpu": 0.5, "mem": 0.8},
		{"timestamp": "t2", "cpu": 0.6, "mem": 0.7},
	})

	melted := df.Melt([]string{"timestamp"}, "feature", "value")

	if melted.NRows() != 4 {
		t.Fatalf("Melt NRows: got %d, want 4", melted.NRows())
	}

	// Verify all values present
	features := melted.Col("feature")
	values := melted.Col("value")

	featureSet := make(map[string]bool)
	for i := 0; i < melted.NRows(); i++ {
		featureSet[features.Str(i)] = true
	}
	if !featureSet["cpu"] || !featureSet["mem"] {
		t.Error("Melt: missing expected features")
	}

	// Check values sum
	total := values.Sum()
	assertFloat(t, "Melt values sum", total, 2.6)
}

func TestMeltDefaultNames(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"id": "A", "x": 1.0, "y": 2.0},
	})
	melted := df.Melt([]string{"id"}, "", "")
	if !melted.HasColumn("variable") {
		t.Error("Default var_name should be 'variable'")
	}
	if !melted.HasColumn("value") {
		t.Error("Default value_name should be 'value'")
	}
}

func TestPivotThenMeltRoundTrip(t *testing.T) {
	// Long → Wide → Long should preserve data
	long := NewDataFrame([]map[string]any{
		{"ts": "t1", "feature": "a", "value": 1.0},
		{"ts": "t1", "feature": "b", "value": 2.0},
		{"ts": "t2", "feature": "a", "value": 3.0},
		{"ts": "t2", "feature": "b", "value": 4.0},
	})

	wide := long.PivotTable([]string{"ts"}, "feature", "value", "first")
	if wide.NRows() != 2 {
		t.Fatalf("Wide NRows: got %d, want 2", wide.NRows())
	}

	melted := wide.Melt([]string{"ts"}, "feature", "value")
	if melted.NRows() != 4 {
		t.Fatalf("Melted NRows: got %d, want 4", melted.NRows())
	}

	// Sum should be preserved
	assertFloat(t, "RoundTrip sum", melted.Col("value").Sum(), 10.0)
}
