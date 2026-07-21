//go:build !purego

package parquet

import (
	"math"
	"testing"
)

func benchmarkInt64PageBoundsWithoutAVX512(b *testing.B, numValues int) {
	savedHasAVX512VL := hasAVX512VL
	hasAVX512VL = false
	b.Cleanup(func() { hasAVX512VL = savedHasAVX512VL })

	page := newInt64BoundsPage(numValues, uint64(b.N))
	wantMin, wantMax := int64(math.MinInt64), int64(math.MaxInt64)

	b.SetBytes(page.Size())

	var min, max Value
	var ok bool
	for b.Loop() {
		min, max, ok = page.Bounds()
	}
	b.ReportMetric(0, "avx512vl")

	if !ok || min.Int64() != wantMin || max.Int64() != wantMax {
		b.Fatalf("unexpected bounds: min=%d max=%d ok=%t", min.Int64(), max.Int64(), ok)
	}
}

func BenchmarkInt64PageBoundsDefaultWithoutAVX512(b *testing.B) {
	benchmarkInt64PageBoundsWithoutAVX512(b, defaultInt64PageValueCount)
}

func BenchmarkInt64PageBoundsBeforeCombinedThresholdWithoutAVX512(b *testing.B) {
	benchmarkInt64PageBoundsWithoutAVX512(b, combinedBoundsThreshold/8-1)
}

func BenchmarkInt64PageBoundsAtCombinedThresholdWithoutAVX512(b *testing.B) {
	benchmarkInt64PageBoundsWithoutAVX512(b, combinedBoundsThreshold/8)
}
