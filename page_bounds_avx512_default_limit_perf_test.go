//go:build amd64 && !purego

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

func BenchmarkInt64PageBoundsDefaultLimit(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBoundsDefaultLimit requires AVX-512VL")
	}

	values, wantMin, wantMax := int64PageBoundsValues(DefaultPageBufferSize / 8)
	page := int64Page{values: memory.SliceBufferFrom(values)}
	wantChecksum := wantMin ^ wantMax
	var checksum int64

	b.SetBytes(int64(len(values)) * 8)
	b.ReportAllocs()

	for b.Loop() {
		min, max, ok := page.Bounds()
		if !ok {
			b.Fatal("Bounds returned no values")
		}
		checksum ^= min.Int64() ^ max.Int64()
	}

	if b.N%2 == 0 {
		wantChecksum = 0
	}
	if checksum != wantChecksum {
		b.Fatalf("bounds checksum = %d, want %d", checksum, wantChecksum)
	}
}
