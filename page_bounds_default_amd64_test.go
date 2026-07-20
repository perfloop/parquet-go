//go:build !purego

package parquet

import (
	"math"
	"testing"
)

func TestInt64PageBoundsDefaultPageSizeWithoutAVX512(t *testing.T) {
	savedHasAVX512VL := hasAVX512VL
	hasAVX512VL = false
	t.Cleanup(func() { hasAVX512VL = savedHasAVX512VL })

	testInt64PageBoundsDefaultPageSize(t)
}

func requireAVX512VL(b *testing.B) {
	b.Helper()
	if !hasAVX512VL {
		b.Skip("requires AVX-512VL")
	}
}

func benchmarkInt64PageBounds(b *testing.B, numValues int) {
	requireAVX512VL(b)

	page := newInt64BoundsPage(numValues, uint64(b.N))
	wantMin, wantMax := int64(math.MinInt64), int64(math.MaxInt64)

	b.SetBytes(page.Size())

	var min, max Value
	var ok bool
	for b.Loop() {
		min, max, ok = page.Bounds()
	}
	b.ReportMetric(1, "avx512vl")

	if !ok || min.Int64() != wantMin || max.Int64() != wantMax {
		b.Fatalf("unexpected bounds: min=%d max=%d ok=%t", min.Int64(), max.Int64(), ok)
	}
}

func BenchmarkInt64PageBoundsBelowDefault(b *testing.B) {
	benchmarkInt64PageBounds(b, defaultInt64PageValueCount/2)
}

func BenchmarkInt64PageBoundsDefault(b *testing.B) {
	benchmarkInt64PageBounds(b, defaultInt64PageValueCount)
}

func BenchmarkInt64PageBoundsAboveDefault(b *testing.B) {
	benchmarkInt64PageBounds(b, 2*defaultInt64PageValueCount)
}
