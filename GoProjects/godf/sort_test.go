package godf

import (
	"testing"
)

func TestSortBy(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"a": "B", "b": 2.0},
		{"a": "A", "b": 1.0},
		{"a": "C", "b": 3.0},
		{"a": "A", "b": 0.0},
	})

	sorted := df.SortBy("a", "b")
	// pandas: A/0, A/1, B/2, C/3
	wantA := []string{"A", "A", "B", "C"}
	wantB := []float64{0, 1, 2, 3}

	for i, w := range wantA {
		if sorted.Col("a").Str(i) != w {
			t.Errorf("Sort a[%d]: got %s, want %s", i, sorted.Col("a").Str(i), w)
		}
	}
	assertFloatSlice(t, "Sort b", sorted.Col("b"), wantB)
}

func TestSortByDesc(t *testing.T) {
	df := NewDataFrame([]map[string]any{
		{"v": 1.0},
		{"v": 3.0},
		{"v": 2.0},
	})
	sorted := df.SortByDesc("v")
	assertFloatSlice(t, "SortDesc", sorted.Col("v"), []float64{3, 2, 1})
}

func TestSortByStable(t *testing.T) {
	// Stability: equal elements preserve original order
	df := NewDataFrame([]map[string]any{
		{"key": "A", "order": 1.0},
		{"key": "B", "order": 2.0},
		{"key": "A", "order": 3.0},
		{"key": "B", "order": 4.0},
	})
	sorted := df.SortBy("key")
	// A rows should maintain order: 1, 3
	assertFloat(t, "Stable A[0]", sorted.Col("order").Float(0), 1.0)
	assertFloat(t, "Stable A[1]", sorted.Col("order").Float(1), 3.0)
	assertFloat(t, "Stable B[0]", sorted.Col("order").Float(2), 2.0)
	assertFloat(t, "Stable B[1]", sorted.Col("order").Float(3), 4.0)
}
