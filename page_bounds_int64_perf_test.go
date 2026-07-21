//go:build !purego && amd64

package parquet

import "testing"

func BenchmarkInt64PageBounds(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBounds requires AVX-512VL")
	}

	for _, benchmark := range []struct {
		name  string
		count int
	}{
		{name: "default-page", count: int64PageBoundsDefaultValueCount},
		{name: "below-window", count: int64PageBoundsBelowWindowCount},
		{name: "above-window", count: int64PageBoundsAboveWindowCount},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			values := int64PageBoundsValues(benchmark.count, benchmark.count-1, benchmark.count-2)
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
		})
	}
}
