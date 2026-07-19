//go:build amd64 && !purego

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

func BenchmarkInt64PageBounds(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBounds requires AVX-512VL")
	}

	for _, benchmark := range []struct {
		name string
		n    int
	}{
		{name: "below-default", n: defaultInt64PageValues - 1},
		{name: "default-page", n: defaultInt64PageValues},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			values, wantMin, wantMax := int64PageBoundsValues(benchmark.n)
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
		})
	}
}
