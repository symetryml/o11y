package godf

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"time"
)

// Series is a typed, nullable column of data.
//
// Exactly one of the typed slices (floats, strings, times, bools, anys) is
// populated based on the DType. Null values are tracked per-element: for
// Float64 series, math.NaN represents null; for other types, the nulls
// bitmap is authoritative.
type Series struct {
	name   string
	dtype  DType
	length int

	floats  []float64
	strings []string
	times   []time.Time
	bools   []bool
	anys    []any

	nulls []bool // true = null (only used for non-Float64 types)
}

// NewFloat64Series creates a Series of Float64 values.
// Use math.NaN() to represent null values.
func NewFloat64Series(name string, vals []float64) *Series {
	data := make([]float64, len(vals))
	copy(data, vals)
	return &Series{
		name:   name,
		dtype:  Float64,
		length: len(vals),
		floats: data,
	}
}

// NewStringSeriesFromSlice creates a Series of String values.
// Use nil in the optional nulls mask to indicate no nulls.
func NewStringSeriesFromSlice(name string, vals []string, nulls []bool) *Series {
	data := make([]string, len(vals))
	copy(data, vals)
	var nm []bool
	if nulls != nil {
		nm = make([]bool, len(nulls))
		copy(nm, nulls)
	} else {
		nm = make([]bool, len(vals))
	}
	return &Series{
		name:    name,
		dtype:   String,
		length:  len(vals),
		strings: data,
		nulls:   nm,
	}
}

// NewStringSeries creates a Series of String values (no nulls).
func NewStringSeries(name string, vals ...string) *Series {
	return NewStringSeriesFromSlice(name, vals, nil)
}

// NewDateTimeSeries creates a Series of DateTime values.
func NewDateTimeSeries(name string, vals []time.Time, nulls []bool) *Series {
	data := make([]time.Time, len(vals))
	copy(data, vals)
	var nm []bool
	if nulls != nil {
		nm = make([]bool, len(nulls))
		copy(nm, nulls)
	} else {
		nm = make([]bool, len(vals))
	}
	return &Series{
		name:   name,
		dtype:  DateTime,
		length: len(vals),
		times:  data,
		nulls:  nm,
	}
}

// NewBoolSeries creates a Series of Bool values.
func NewBoolSeries(name string, vals []bool, nulls []bool) *Series {
	data := make([]bool, len(vals))
	copy(data, vals)
	var nm []bool
	if nulls != nil {
		nm = make([]bool, len(nulls))
		copy(nm, nulls)
	} else {
		nm = make([]bool, len(vals))
	}
	return &Series{
		name:   name,
		dtype:  Bool,
		length: len(vals),
		bools:  data,
		nulls:  nm,
	}
}

// NewAnySeries creates a Series of mixed-type values.
// nil values are treated as null.
func NewAnySeries(name string, vals []any) *Series {
	data := make([]any, len(vals))
	copy(data, vals)
	nm := make([]bool, len(vals))
	for i, v := range vals {
		if v == nil {
			nm[i] = true
		}
	}
	return &Series{
		name:   name,
		dtype:  Any,
		length: len(vals),
		anys:   data,
		nulls:  nm,
	}
}

// Name returns the series name.
func (s *Series) Name() string { return s.name }

// DType returns the data type.
func (s *Series) Dtype() DType { return s.dtype }

// Len returns the number of elements.
func (s *Series) Len() int { return s.length }

// IsNull reports whether element i is null.
func (s *Series) IsNull(i int) bool {
	if s.dtype == Float64 {
		return math.IsNaN(s.floats[i])
	}
	return s.nulls[i]
}

// NotNA returns a boolean slice: true where element is not null.
func (s *Series) NotNA() []bool {
	result := make([]bool, s.length)
	for i := 0; i < s.length; i++ {
		result[i] = !s.IsNull(i)
	}
	return result
}

// CountNotNull returns the count of non-null elements.
func (s *Series) CountNotNull() int {
	n := 0
	for i := 0; i < s.length; i++ {
		if !s.IsNull(i) {
			n++
		}
	}
	return n
}

// Float returns the float64 value at index i. Panics if dtype != Float64.
func (s *Series) Float(i int) float64 {
	return s.floats[i]
}

// Str returns the string value at index i. Panics if dtype != String.
func (s *Series) Str(i int) string {
	return s.strings[i]
}

// Time returns the time.Time value at index i. Panics if dtype != DateTime.
func (s *Series) Time(i int) time.Time {
	return s.times[i]
}

// Bool returns the bool value at index i. Panics if dtype != Bool.
func (s *Series) Bool(i int) bool {
	return s.bools[i]
}

// Any returns the value at index i as interface{}.
func (s *Series) Any(i int) any {
	switch s.dtype {
	case Float64:
		if math.IsNaN(s.floats[i]) {
			return nil
		}
		return s.floats[i]
	case String:
		if s.nulls[i] {
			return nil
		}
		return s.strings[i]
	case DateTime:
		if s.nulls[i] {
			return nil
		}
		return s.times[i]
	case Bool:
		if s.nulls[i] {
			return nil
		}
		return s.bools[i]
	default:
		return s.anys[i]
	}
}

// Floats returns a copy of the underlying float64 slice.
func (s *Series) Floats() []float64 {
	out := make([]float64, len(s.floats))
	copy(out, s.floats)
	return out
}

// Strings returns a copy of the underlying string slice.
func (s *Series) Strings() []string {
	out := make([]string, len(s.strings))
	copy(out, s.strings)
	return out
}

// Times returns a copy of the underlying time.Time slice.
func (s *Series) Times() []time.Time {
	out := make([]time.Time, len(s.times))
	copy(out, s.times)
	return out
}

// Bools returns a copy of the underlying bool slice.
func (s *Series) Bools() []bool {
	out := make([]bool, len(s.bools))
	copy(out, s.bools)
	return out
}

// Copy returns a deep copy of the series.
func (s *Series) Copy() *Series {
	ns := &Series{
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}
	if s.floats != nil {
		ns.floats = make([]float64, len(s.floats))
		copy(ns.floats, s.floats)
	}
	if s.strings != nil {
		ns.strings = make([]string, len(s.strings))
		copy(ns.strings, s.strings)
	}
	if s.times != nil {
		ns.times = make([]time.Time, len(s.times))
		copy(ns.times, s.times)
	}
	if s.bools != nil {
		ns.bools = make([]bool, len(s.bools))
		copy(ns.bools, s.bools)
	}
	if s.anys != nil {
		ns.anys = make([]any, len(s.anys))
		copy(ns.anys, s.anys)
	}
	if s.nulls != nil {
		ns.nulls = make([]bool, len(s.nulls))
		copy(ns.nulls, s.nulls)
	}
	return ns
}

// Rename returns a copy of the series with a new name.
func (s *Series) Rename(name string) *Series {
	ns := s.Copy()
	ns.name = name
	return ns
}

// Filter returns a new series containing only elements where mask[i] is true.
func (s *Series) Filter(mask []bool) *Series {
	count := 0
	for _, v := range mask {
		if v {
			count++
		}
	}

	ns := &Series{
		name:   s.name,
		dtype:  s.dtype,
		length: count,
	}

	switch s.dtype {
	case Float64:
		ns.floats = make([]float64, 0, count)
		for i, m := range mask {
			if m {
				ns.floats = append(ns.floats, s.floats[i])
			}
		}
	case String:
		ns.strings = make([]string, 0, count)
		ns.nulls = make([]bool, 0, count)
		for i, m := range mask {
			if m {
				ns.strings = append(ns.strings, s.strings[i])
				ns.nulls = append(ns.nulls, s.nulls[i])
			}
		}
	case DateTime:
		ns.times = make([]time.Time, 0, count)
		ns.nulls = make([]bool, 0, count)
		for i, m := range mask {
			if m {
				ns.times = append(ns.times, s.times[i])
				ns.nulls = append(ns.nulls, s.nulls[i])
			}
		}
	case Bool:
		ns.bools = make([]bool, 0, count)
		ns.nulls = make([]bool, 0, count)
		for i, m := range mask {
			if m {
				ns.bools = append(ns.bools, s.bools[i])
				ns.nulls = append(ns.nulls, s.nulls[i])
			}
		}
	case Any:
		ns.anys = make([]any, 0, count)
		ns.nulls = make([]bool, 0, count)
		for i, m := range mask {
			if m {
				ns.anys = append(ns.anys, s.anys[i])
				ns.nulls = append(ns.nulls, s.nulls[i])
			}
		}
	}

	return ns
}

// Take returns a new series with elements at the given indices.
func (s *Series) Take(indices []int) *Series {
	ns := &Series{
		name:   s.name,
		dtype:  s.dtype,
		length: len(indices),
	}

	switch s.dtype {
	case Float64:
		ns.floats = make([]float64, len(indices))
		for j, i := range indices {
			ns.floats[j] = s.floats[i]
		}
	case String:
		ns.strings = make([]string, len(indices))
		ns.nulls = make([]bool, len(indices))
		for j, i := range indices {
			ns.strings[j] = s.strings[i]
			ns.nulls[j] = s.nulls[i]
		}
	case DateTime:
		ns.times = make([]time.Time, len(indices))
		ns.nulls = make([]bool, len(indices))
		for j, i := range indices {
			ns.times[j] = s.times[i]
			ns.nulls[j] = s.nulls[i]
		}
	case Bool:
		ns.bools = make([]bool, len(indices))
		ns.nulls = make([]bool, len(indices))
		for j, i := range indices {
			ns.bools[j] = s.bools[i]
			ns.nulls[j] = s.nulls[i]
		}
	case Any:
		ns.anys = make([]any, len(indices))
		ns.nulls = make([]bool, len(indices))
		for j, i := range indices {
			ns.anys[j] = s.anys[i]
			ns.nulls[j] = s.nulls[i]
		}
	}

	return ns
}

// --- Aggregation (Float64) ---

// Mean returns the arithmetic mean of non-null values.
func (s *Series) Mean() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	sum := 0.0
	n := 0
	for _, v := range s.floats {
		if !math.IsNaN(v) {
			sum += v
			n++
		}
	}
	if n == 0 {
		return math.NaN()
	}
	return sum / float64(n)
}

// Sum returns the sum of non-null values.
func (s *Series) Sum() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	sum := 0.0
	for _, v := range s.floats {
		if !math.IsNaN(v) {
			sum += v
		}
	}
	return sum
}

// Min returns the minimum non-null value.
func (s *Series) Min() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	min := math.Inf(1)
	found := false
	for _, v := range s.floats {
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
}

// Max returns the maximum non-null value.
func (s *Series) Max() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	max := math.Inf(-1)
	found := false
	for _, v := range s.floats {
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
}

// Std returns the sample standard deviation (ddof=1) of non-null values.
func (s *Series) Std() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	mean := s.Mean()
	if math.IsNaN(mean) {
		return math.NaN()
	}
	sumSq := 0.0
	n := 0
	for _, v := range s.floats {
		if !math.IsNaN(v) {
			d := v - mean
			sumSq += d * d
			n++
		}
	}
	if n < 2 {
		return 0.0
	}
	return math.Sqrt(sumSq / float64(n-1))
}

// Count returns the number of non-null elements.
func (s *Series) Count() int {
	return s.CountNotNull()
}

// First returns the first non-null value or NaN.
func (s *Series) First() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	for _, v := range s.floats {
		if !math.IsNaN(v) {
			return v
		}
	}
	return math.NaN()
}

// Last returns the last non-null value or NaN.
func (s *Series) Last() float64 {
	if s.dtype != Float64 {
		return math.NaN()
	}
	for i := len(s.floats) - 1; i >= 0; i-- {
		if !math.IsNaN(s.floats[i]) {
			return s.floats[i]
		}
	}
	return math.NaN()
}

// --- Unique / NUnique ---

// NUnique returns the number of unique non-null values.
func (s *Series) NUnique() int {
	seen := make(map[string]struct{})
	for i := 0; i < s.length; i++ {
		if s.IsNull(i) {
			continue
		}
		seen[s.stringKey(i)] = struct{}{}
	}
	return len(seen)
}

// UniqueStrings returns unique non-null string values. Panics if dtype != String.
func (s *Series) UniqueStrings() []string {
	seen := make(map[string]struct{})
	var result []string
	for i, v := range s.strings {
		if s.nulls[i] {
			continue
		}
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// UniqueFloats returns unique non-null float64 values.
func (s *Series) UniqueFloats() []float64 {
	seen := make(map[float64]struct{})
	var result []float64
	for _, v := range s.floats {
		if math.IsNaN(v) {
			continue
		}
		if _, ok := seen[v]; !ok {
			seen[v] = struct{}{}
			result = append(result, v)
		}
	}
	return result
}

// --- Shift ---

// Shift returns a new series with values shifted by n positions.
// Positive n shifts forward (introduces NaN/null at the beginning).
// Negative n shifts backward (introduces NaN/null at the end).
func (s *Series) Shift(n int) *Series {
	ns := &Series{
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	abs := n
	if abs < 0 {
		abs = -abs
	}

	switch s.dtype {
	case Float64:
		ns.floats = make([]float64, s.length)
		for i := range ns.floats {
			ns.floats[i] = math.NaN()
		}
		if n >= 0 {
			for i := n; i < s.length; i++ {
				ns.floats[i] = s.floats[i-n]
			}
		} else {
			for i := 0; i < s.length+n; i++ {
				ns.floats[i] = s.floats[i-n]
			}
		}
	case String:
		ns.strings = make([]string, s.length)
		ns.nulls = make([]bool, s.length)
		for i := range ns.nulls {
			ns.nulls[i] = true
		}
		if n >= 0 {
			for i := n; i < s.length; i++ {
				ns.strings[i] = s.strings[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		} else {
			for i := 0; i < s.length+n; i++ {
				ns.strings[i] = s.strings[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		}
	case DateTime:
		ns.times = make([]time.Time, s.length)
		ns.nulls = make([]bool, s.length)
		for i := range ns.nulls {
			ns.nulls[i] = true
		}
		if n >= 0 {
			for i := n; i < s.length; i++ {
				ns.times[i] = s.times[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		} else {
			for i := 0; i < s.length+n; i++ {
				ns.times[i] = s.times[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		}
	default:
		ns.anys = make([]any, s.length)
		ns.nulls = make([]bool, s.length)
		for i := range ns.nulls {
			ns.nulls[i] = true
		}
		if n >= 0 {
			for i := n; i < s.length; i++ {
				ns.anys[i] = s.anys[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		} else {
			for i := 0; i < s.length+n; i++ {
				ns.anys[i] = s.anys[i-n]
				ns.nulls[i] = s.nulls[i-n]
			}
		}
	}

	return ns
}

// --- Arithmetic (Float64) ---

// Sub returns a new Float64 series: s[i] - other[i]. NaN propagates.
func (s *Series) Sub(other *Series) *Series {
	if s.dtype != Float64 || other.dtype != Float64 || s.length != other.length {
		panic(fmt.Sprintf("Sub: incompatible series (dtype=%v/%v, len=%d/%d)",
			s.dtype, other.dtype, s.length, other.length))
	}
	result := make([]float64, s.length)
	for i := range result {
		result[i] = s.floats[i] - other.floats[i]
	}
	return NewFloat64Series(s.name, result)
}

// Div returns a new Float64 series: s[i] / other[i]. Division by zero yields NaN.
func (s *Series) Div(other *Series) *Series {
	if s.dtype != Float64 || other.dtype != Float64 || s.length != other.length {
		panic(fmt.Sprintf("Div: incompatible series (dtype=%v/%v, len=%d/%d)",
			s.dtype, other.dtype, s.length, other.length))
	}
	result := make([]float64, s.length)
	for i := range result {
		if other.floats[i] == 0 || math.IsNaN(other.floats[i]) || math.IsNaN(s.floats[i]) {
			result[i] = math.NaN()
		} else {
			result[i] = s.floats[i] / other.floats[i]
		}
	}
	return NewFloat64Series(s.name, result)
}

// Mul returns a new Float64 series: s[i] * other[i]. NaN propagates.
func (s *Series) Mul(other *Series) *Series {
	if s.dtype != Float64 || other.dtype != Float64 || s.length != other.length {
		panic(fmt.Sprintf("Mul: incompatible series (dtype=%v/%v, len=%d/%d)",
			s.dtype, other.dtype, s.length, other.length))
	}
	result := make([]float64, s.length)
	for i := range result {
		result[i] = s.floats[i] * other.floats[i]
	}
	return NewFloat64Series(s.name, result)
}

// Add returns a new Float64 series: s[i] + other[i]. NaN propagates.
func (s *Series) Add(other *Series) *Series {
	if s.dtype != Float64 || other.dtype != Float64 || s.length != other.length {
		panic(fmt.Sprintf("Add: incompatible series (dtype=%v/%v, len=%d/%d)",
			s.dtype, other.dtype, s.length, other.length))
	}
	result := make([]float64, s.length)
	for i := range result {
		result[i] = s.floats[i] + other.floats[i]
	}
	return NewFloat64Series(s.name, result)
}

// Abs returns a new Float64 series with absolute values.
func (s *Series) Abs() *Series {
	if s.dtype != Float64 {
		panic("Abs requires Float64 series")
	}
	result := make([]float64, s.length)
	for i, v := range s.floats {
		result[i] = math.Abs(v)
	}
	return NewFloat64Series(s.name, result)
}

// ReplaceInf replaces +Inf and -Inf with the given value.
func (s *Series) ReplaceInf(replacement float64) *Series {
	if s.dtype != Float64 {
		return s.Copy()
	}
	result := make([]float64, s.length)
	for i, v := range s.floats {
		if math.IsInf(v, 0) {
			result[i] = replacement
		} else {
			result[i] = v
		}
	}
	return NewFloat64Series(s.name, result)
}

// DropNA returns a new series with null values removed.
func (s *Series) DropNA() *Series {
	mask := s.NotNA()
	return s.Filter(mask)
}

// FillNA returns a new series with null values replaced by fillValue.
func (s *Series) FillNA(fillValue any) *Series {
	ns := s.Copy()
	switch s.dtype {
	case Float64:
		fv, ok := toFloat64(fillValue)
		if !ok {
			return ns
		}
		for i, v := range ns.floats {
			if math.IsNaN(v) {
				ns.floats[i] = fv
			}
		}
	case String:
		fv, ok := fillValue.(string)
		if !ok {
			return ns
		}
		for i := range ns.strings {
			if ns.nulls[i] {
				ns.strings[i] = fv
				ns.nulls[i] = false
			}
		}
	}
	return ns
}

// --- String operations ---

// EndsWith returns a boolean slice: true where the string ends with suffix.
// For non-string series, returns all false.
func (s *Series) EndsWith(suffix string) []bool {
	result := make([]bool, s.length)
	if s.dtype != String {
		return result
	}
	for i, v := range s.strings {
		if !s.nulls[i] {
			result[i] = strings.HasSuffix(v, suffix)
		}
	}
	return result
}

// StartsWith returns a boolean slice: true where the string starts with prefix.
func (s *Series) StartsWith(prefix string) []bool {
	result := make([]bool, s.length)
	if s.dtype != String {
		return result
	}
	for i, v := range s.strings {
		if !s.nulls[i] {
			result[i] = strings.HasPrefix(v, prefix)
		}
	}
	return result
}

// Contains returns a boolean slice: true where the string contains substr.
func (s *Series) Contains(substr string) []bool {
	result := make([]bool, s.length)
	if s.dtype != String {
		return result
	}
	for i, v := range s.strings {
		if !s.nulls[i] {
			result[i] = strings.Contains(v, substr)
		}
	}
	return result
}

// Eq returns a boolean slice: true where the value equals val.
func (s *Series) Eq(val any) []bool {
	result := make([]bool, s.length)
	switch s.dtype {
	case Float64:
		fv, ok := toFloat64(val)
		if !ok {
			return result
		}
		for i, v := range s.floats {
			result[i] = v == fv
		}
	case String:
		sv, ok := val.(string)
		if !ok {
			return result
		}
		for i, v := range s.strings {
			if !s.nulls[i] {
				result[i] = v == sv
			}
		}
	}
	return result
}

// --- Argsort ---

// Argsort returns indices that would sort the series in ascending order.
// NaN values are placed at the end.
func (s *Series) Argsort() []int {
	indices := make([]int, s.length)
	for i := range indices {
		indices[i] = i
	}

	switch s.dtype {
	case Float64:
		sort.SliceStable(indices, func(a, b int) bool {
			ai, bi := indices[a], indices[b]
			aNaN := math.IsNaN(s.floats[ai])
			bNaN := math.IsNaN(s.floats[bi])
			if aNaN && bNaN {
				return false
			}
			if aNaN {
				return false
			}
			if bNaN {
				return true
			}
			return s.floats[ai] < s.floats[bi]
		})
	case String:
		sort.SliceStable(indices, func(a, b int) bool {
			ai, bi := indices[a], indices[b]
			if s.nulls[ai] && s.nulls[bi] {
				return false
			}
			if s.nulls[ai] {
				return false
			}
			if s.nulls[bi] {
				return true
			}
			return s.strings[ai] < s.strings[bi]
		})
	case DateTime:
		sort.SliceStable(indices, func(a, b int) bool {
			ai, bi := indices[a], indices[b]
			if s.nulls[ai] && s.nulls[bi] {
				return false
			}
			if s.nulls[ai] {
				return false
			}
			if s.nulls[bi] {
				return true
			}
			return s.times[ai].Before(s.times[bi])
		})
	}

	return indices
}

// --- Apply ---

// Apply applies fn to each non-null element and returns a new series.
// The returned series has the same dtype. fn receives and returns any.
func (s *Series) Apply(fn func(val any) any) *Series {
	ns := &Series{
		name:   s.name,
		dtype:  s.dtype,
		length: s.length,
	}

	switch s.dtype {
	case Float64:
		ns.floats = make([]float64, s.length)
		for i, v := range s.floats {
			if math.IsNaN(v) {
				ns.floats[i] = math.NaN()
			} else {
				result := fn(v)
				if f, ok := toFloat64(result); ok {
					ns.floats[i] = f
				} else {
					ns.floats[i] = math.NaN()
				}
			}
		}
	case String:
		ns.strings = make([]string, s.length)
		ns.nulls = make([]bool, s.length)
		for i, v := range s.strings {
			if s.nulls[i] {
				ns.nulls[i] = true
			} else {
				result := fn(v)
				if sv, ok := result.(string); ok {
					ns.strings[i] = sv
				} else {
					ns.strings[i] = fmt.Sprintf("%v", result)
				}
			}
		}
	default:
		ns.anys = make([]any, s.length)
		ns.nulls = make([]bool, s.length)
		copy(ns.nulls, s.nulls)
		for i := 0; i < s.length; i++ {
			if s.IsNull(i) {
				ns.nulls[i] = true
			} else {
				ns.anys[i] = fn(s.Any(i))
			}
		}
	}

	return ns
}

// ApplyString applies fn to each non-null string element and returns a new String series.
func (s *Series) ApplyString(fn func(string) string) *Series {
	if s.dtype != String {
		panic("ApplyString requires String series")
	}
	result := make([]string, s.length)
	nulls := make([]bool, s.length)
	copy(nulls, s.nulls)
	for i, v := range s.strings {
		if !s.nulls[i] {
			result[i] = fn(v)
		}
	}
	return NewStringSeriesFromSlice(s.name, result, nulls)
}

// ApplyFloat applies fn to each non-null float64 element and returns a new Float64 series.
func (s *Series) ApplyFloat(fn func(float64) float64) *Series {
	if s.dtype != Float64 {
		panic("ApplyFloat requires Float64 series")
	}
	result := make([]float64, s.length)
	for i, v := range s.floats {
		if math.IsNaN(v) {
			result[i] = math.NaN()
		} else {
			result[i] = fn(v)
		}
	}
	return NewFloat64Series(s.name, result)
}

// --- ToList ---

// ToFloat64Slice returns the float values as a slice. Alias for Floats().
func (s *Series) ToFloat64Slice() []float64 { return s.Floats() }

// ToStringSlice returns the string values as a slice. Alias for Strings().
func (s *Series) ToStringSlice() []string { return s.Strings() }

// --- Internal helpers ---

// stringKey returns a string representation of element i, used for hashing.
func (s *Series) stringKey(i int) string {
	switch s.dtype {
	case Float64:
		return fmt.Sprintf("%v", s.floats[i])
	case String:
		return s.strings[i]
	case DateTime:
		return s.times[i].Format(time.RFC3339Nano)
	case Bool:
		if s.bools[i] {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", s.anys[i])
	}
}

// toFloat64 converts any to float64 if possible.
func toFloat64(v any) (float64, bool) {
	switch val := v.(type) {
	case float64:
		return val, true
	case float32:
		return float64(val), true
	case int:
		return float64(val), true
	case int64:
		return float64(val), true
	case int32:
		return float64(val), true
	case int16:
		return float64(val), true
	case int8:
		return float64(val), true
	case uint:
		return float64(val), true
	case uint64:
		return float64(val), true
	case uint32:
		return float64(val), true
	default:
		return math.NaN(), false
	}
}

// newFloat64SeriesNaN creates a Float64 series filled with NaN.
func newFloat64SeriesNaN(name string, length int) *Series {
	vals := make([]float64, length)
	for i := range vals {
		vals[i] = math.NaN()
	}
	return &Series{
		name:   name,
		dtype:  Float64,
		length: length,
		floats: vals,
	}
}
