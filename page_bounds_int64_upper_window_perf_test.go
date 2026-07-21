//go:build !purego && amd64

package parquet

import "testing"

func BenchmarkInt64PageBoundsUpperWindow(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBoundsUpperWindow requires AVX-512VL")
	}

	count := int64PageBoundsAboveWindowCount - 1
	values := int64PageBoundsValues(count, count-1, count-2)
	page := newInt64PageBoundsPage(values)
	wantMin, wantMax := int64PageBoundsOracle(values)

	b.SetBytes(int64(len(values)) * 8)
	b.ReportAllocs()

	var min, max Value
	var ok bool
	for b.Loop() {
		min, max, ok = page.Bounds()
	}

	if !ok || min.Int64() != wantMin || max.Int64() != wantMax {
		b.Fatalf("Bounds() = (%d, %d, %t), want (%d, %d, true)", min.Int64(), max.Int64(), ok, wantMin, wantMax)
	}
}
