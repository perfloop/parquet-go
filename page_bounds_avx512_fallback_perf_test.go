//go:build amd64 && !purego

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

func BenchmarkInt64PageBoundsAboveDefaultLimit(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBoundsAboveDefaultLimit requires AVX-512VL")
	}
	benchmarkInt64PageBoundsFallback(b, DefaultPageBufferSize/8+1)
}

func BenchmarkInt64PageBoundsAVX512DisabledSmall(b *testing.B) {
	if hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBoundsAVX512DisabledSmall requires AVX-512VL disabled")
	}
	benchmarkInt64PageBoundsFallback(b, 4*1024/8)
}

func BenchmarkInt64PageBoundsAVX512DisabledAboveDefaultLimit(b *testing.B) {
	if hasAVX512VL {
		b.Fatal("BenchmarkInt64PageBoundsAVX512DisabledAboveDefaultLimit requires AVX-512VL disabled")
	}
	benchmarkInt64PageBoundsFallback(b, DefaultPageBufferSize/8+1)
}

func benchmarkInt64PageBoundsFallback(b *testing.B, n int) {
	values, wantMin, wantMax := int64PageBoundsValues(n)
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
