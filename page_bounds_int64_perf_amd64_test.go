//go:build amd64 && !purego

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

func BenchmarkInt64PageBounds(b *testing.B) {
	if !hasAVX512VL {
		b.Skip("requires AVX-512VL")
	}

	values := int64BoundsValuesWithSeed(defaultInt64PageValueCount, uint64(b.N)^0x9E3779B97F4A7C15)
	wantMin, wantMax, wantOK := int64BoundsReference(values)
	page := Page(&int64Page{values: memory.SliceBufferFrom(values)})

	b.Run("default-page", func(b *testing.B) {
		b.SetBytes(int64(len(values) * 8))

		var min, max Value
		var ok bool
		for b.Loop() {
			min, max, ok = page.Bounds()
		}

		if ok != wantOK || min.Int64() != wantMin || max.Int64() != wantMax {
			b.Fatalf("Bounds() = (%d, %d, %t), want (%d, %d, %t)", min.Int64(), max.Int64(), ok, wantMin, wantMax, wantOK)
		}
	})
}
