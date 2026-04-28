package godf

import (
	"fmt"
	"math"
	"testing"
)

const epsilon = 1e-10

func assertFloat(t *testing.T, name string, got, want float64) {
	t.Helper()
	if math.IsNaN(want) {
		if !math.IsNaN(got) {
			t.Errorf("%s: got %f, want NaN", name, got)
		}
		return
	}
	if math.Abs(got-want) > epsilon {
		t.Errorf("%s: got %f, want %f", name, got, want)
	}
}

func assertFloatSlice(t *testing.T, name string, got *Series, want []float64) {
	t.Helper()
	if got.Len() != len(want) {
		t.Fatalf("%s: length mismatch: got %d, want %d", name, got.Len(), len(want))
	}
	for i, w := range want {
		assertFloat(t, fmt.Sprintf("%s[%d]", name, i), got.Float(i), w)
	}
}

func assertBoolSlice(t *testing.T, name string, got, want []bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: length mismatch: got %d, want %d", name, len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s[%d]: got %v, want %v", name, i, got[i], want[i])
		}
	}
}

func TestSeriesAggregation(t *testing.T) {
	s := NewFloat64Series("test", []float64{1, 2, 3, math.NaN(), 5})

	assertFloat(t, "Mean", s.Mean(), 2.75)
	assertFloat(t, "Sum", s.Sum(), 11.0)
	assertFloat(t, "Min", s.Min(), 1.0)
	assertFloat(t, "Max", s.Max(), 5.0)
	assertFloat(t, "Std", s.Std(), 1.707825127659933)
	if s.Count() != 4 {
		t.Errorf("Count: got %d, want 4", s.Count())
	}
}

func TestSeriesAggregationEmpty(t *testing.T) {
	s := NewFloat64Series("empty", []float64{})
	assertFloat(t, "Mean(empty)", s.Mean(), math.NaN())
	assertFloat(t, "Min(empty)", s.Min(), math.NaN())
	assertFloat(t, "Max(empty)", s.Max(), math.NaN())
	assertFloat(t, "Std(empty)", s.Std(), math.NaN())
}

func TestSeriesAggregationAllNaN(t *testing.T) {
	s := NewFloat64Series("allnan", []float64{math.NaN(), math.NaN()})
	assertFloat(t, "Mean(allnan)", s.Mean(), math.NaN())
	assertFloat(t, "Min(allnan)", s.Min(), math.NaN())
}

func TestSeriesShiftForward(t *testing.T) {
	s := NewFloat64Series("val", []float64{10, 20, 30, 40, 50})
	shifted := s.Shift(2)
	want := []float64{math.NaN(), math.NaN(), 10, 20, 30}
	assertFloatSlice(t, "ShiftForward2", shifted, want)
}

func TestSeriesShiftBackward(t *testing.T) {
	s := NewFloat64Series("val", []float64{10, 20, 30, 40, 50})
	shifted := s.Shift(-1)
	want := []float64{20, 30, 40, 50, math.NaN()}
	assertFloatSlice(t, "ShiftBackward1", shifted, want)
}

func TestSeriesArithmetic(t *testing.T) {
	a := NewFloat64Series("a", []float64{1, 2, 3, math.NaN()})
	b := NewFloat64Series("b", []float64{4, 0, 6, 7})

	sub := a.Sub(b)
	assertFloatSlice(t, "Sub", sub, []float64{-3, 2, -3, math.NaN()})

	// Div: [0.25, NaN (div by 0), 0.5, NaN]
	div := a.Div(b)
	assertFloat(t, "Div[0]", div.Float(0), 0.25)
	assertFloat(t, "Div[1]", div.Float(1), math.NaN())
	assertFloat(t, "Div[2]", div.Float(2), 0.5)
	assertFloat(t, "Div[3]", div.Float(3), math.NaN())

	abs := sub.Abs()
	assertFloatSlice(t, "Abs", abs, []float64{3, 2, 3, math.NaN()})
}

func TestSeriesReplaceInf(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, math.Inf(1), math.Inf(-1), 3})
	replaced := s.ReplaceInf(math.NaN())
	want := []float64{1, math.NaN(), math.NaN(), 3}
	assertFloatSlice(t, "ReplaceInf", replaced, want)
}

func TestSeriesDropNA(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, math.NaN(), 3, math.NaN(), 5})
	dropped := s.DropNA()
	if dropped.Len() != 3 {
		t.Fatalf("DropNA: got length %d, want 3", dropped.Len())
	}
	assertFloatSlice(t, "DropNA", dropped, []float64{1, 3, 5})
}

func TestSeriesFillNA(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, math.NaN(), 3})
	filled := s.FillNA(0.0)
	assertFloatSlice(t, "FillNA", filled, []float64{1, 0, 3})
}

func TestSeriesEndsWith(t *testing.T) {
	s := NewStringSeries("metric", "metric_total", "metric_bucket", "metric_count", "other_total")
	got := s.EndsWith("_total")
	want := []bool{true, false, false, true}
	assertBoolSlice(t, "EndsWith", got, want)
}

func TestSeriesEq(t *testing.T) {
	s := NewStringSeries("grp", "A", "B", "A", "C")
	got := s.Eq("A")
	want := []bool{true, false, true, false}
	assertBoolSlice(t, "Eq", got, want)
}

func TestSeriesCopy(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, 2, 3})
	c := s.Copy()
	c.floats[0] = 99
	if s.Float(0) != 1 {
		t.Error("Copy: mutation leaked to original")
	}
}

func TestSeriesNUnique(t *testing.T) {
	s := NewStringSeries("val", "a", "b", "a", "c", "b")
	if s.NUnique() != 3 {
		t.Errorf("NUnique: got %d, want 3", s.NUnique())
	}
}

func TestSeriesUniqueStrings(t *testing.T) {
	s := NewStringSeries("val", "x", "y", "x", "z")
	uniq := s.UniqueStrings()
	if len(uniq) != 3 {
		t.Errorf("UniqueStrings: got %d unique, want 3", len(uniq))
	}
}

func TestSeriesArgsort(t *testing.T) {
	s := NewFloat64Series("val", []float64{30, 10, 20})
	idx := s.Argsort()
	if idx[0] != 1 || idx[1] != 2 || idx[2] != 0 {
		t.Errorf("Argsort: got %v, want [1, 2, 0]", idx)
	}
}

func TestSeriesApplyFloat(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, 2, 3})
	doubled := s.ApplyFloat(func(v float64) float64 { return v * 2 })
	assertFloatSlice(t, "ApplyFloat", doubled, []float64{2, 4, 6})
}

func TestSeriesApplyString(t *testing.T) {
	s := NewStringSeries("val", "hello", "world")
	upper := s.ApplyString(func(v string) string { return v + "!" })
	if upper.Str(0) != "hello!" || upper.Str(1) != "world!" {
		t.Errorf("ApplyString: unexpected result")
	}
}

func TestSeriesFilter(t *testing.T) {
	s := NewFloat64Series("val", []float64{10, 20, 30, 40})
	mask := []bool{true, false, true, false}
	filtered := s.Filter(mask)
	assertFloatSlice(t, "Filter", filtered, []float64{10, 30})
}

func TestSeriesFirstLast(t *testing.T) {
	s := NewFloat64Series("val", []float64{math.NaN(), 2, 3, math.NaN()})
	assertFloat(t, "First", s.First(), 2.0)
	assertFloat(t, "Last", s.Last(), 3.0)
}

func TestSeriesNotNA(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, math.NaN(), 3})
	got := s.NotNA()
	want := []bool{true, false, true}
	assertBoolSlice(t, "NotNA", got, want)
}

func TestSeriesStringFilter(t *testing.T) {
	s := NewStringSeries("s", "abc", "def", "abx")
	mask := s.StartsWith("ab")
	want := []bool{true, false, true}
	assertBoolSlice(t, "StartsWith", mask, want)

	mask2 := s.Contains("b")
	want2 := []bool{true, false, true}
	assertBoolSlice(t, "Contains", mask2, want2)
}

func TestSeriesTake(t *testing.T) {
	s := NewFloat64Series("val", []float64{10, 20, 30, 40, 50})
	taken := s.Take([]int{4, 2, 0})
	assertFloatSlice(t, "Take", taken, []float64{50, 30, 10})
}
