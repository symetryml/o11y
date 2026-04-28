package godf

import (
	"math"
	"testing"
)

func TestRollingMean(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, 2, 3, 4, 5})
	result := s.Rolling(3, 1).Mean()
	// pandas: [1.0, 1.5, 2.0, 3.0, 4.0]
	want := []float64{1.0, 1.5, 2.0, 3.0, 4.0}
	assertFloatSlice(t, "RollingMean", result, want)
}

func TestRollingStd(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, 2, 3, 4, 5})
	result := s.Rolling(3, 2).Std()
	// pandas: [NaN, 0.7071, 1.0, 1.0, 1.0]
	assertFloat(t, "RollingStd[0]", result.Float(0), math.NaN())
	assertFloat(t, "RollingStd[1]", result.Float(1), 0.7071067811865476)
	assertFloat(t, "RollingStd[2]", result.Float(2), 1.0)
	assertFloat(t, "RollingStd[3]", result.Float(3), 1.0)
	assertFloat(t, "RollingStd[4]", result.Float(4), 1.0)
}

func TestRollingSum(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, 2, 3, 4, 5})
	result := s.Rolling(3, 3).Sum()
	// [NaN, NaN, 6, 9, 12]
	assertFloat(t, "RollingSum[0]", result.Float(0), math.NaN())
	assertFloat(t, "RollingSum[1]", result.Float(1), math.NaN())
	assertFloat(t, "RollingSum[2]", result.Float(2), 6.0)
	assertFloat(t, "RollingSum[3]", result.Float(3), 9.0)
	assertFloat(t, "RollingSum[4]", result.Float(4), 12.0)
}

func TestRollingMin(t *testing.T) {
	s := NewFloat64Series("val", []float64{3, 1, 4, 1, 5})
	result := s.Rolling(3, 1).Min()
	// [3, 1, 1, 1, 1]
	want := []float64{3, 1, 1, 1, 1}
	assertFloatSlice(t, "RollingMin", result, want)
}

func TestRollingMax(t *testing.T) {
	s := NewFloat64Series("val", []float64{3, 1, 4, 1, 5})
	result := s.Rolling(3, 1).Max()
	// [3, 3, 4, 4, 5]
	want := []float64{3, 3, 4, 4, 5}
	assertFloatSlice(t, "RollingMax", result, want)
}

func TestRollingWithNaN(t *testing.T) {
	s := NewFloat64Series("val", []float64{1, math.NaN(), 3, 4, 5})
	result := s.Rolling(3, 2).Mean()
	// Window [1, NaN, 3]: 2 non-null → (1+3)/2 = 2.0
	assertFloat(t, "RollingNaN[2]", result.Float(2), 2.0)
}

func TestRollingWindowSize1(t *testing.T) {
	s := NewFloat64Series("val", []float64{10, 20, 30})
	result := s.Rolling(1, 1).Mean()
	assertFloatSlice(t, "RollingW1", result, []float64{10, 20, 30})
}
