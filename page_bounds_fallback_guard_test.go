//go:build amd64 && !purego

package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

// These selectors deliberately do not inspect hasAVX512VL. Their commands
// choose the execution mode: the upper-cap selector runs with the host default,
// while the two fallback selectors set GODEBUG=cpu.avx512vl=off before process
// startup. Keeping the workload identical lets the paired measurements bound
// only the new dispatch predicate on the retained fallback paths.
func BenchmarkInt64PageBoundsAboveDefaultLimit(b *testing.B) {
	benchmarkInt64PageBoundsFallback(b, DefaultPageBufferSize/8+1)
}

func BenchmarkInt64PageBoundsAVX512DisabledSmall(b *testing.B) {
	benchmarkInt64PageBoundsFallback(b, 4*1024/8)
}

func BenchmarkInt64PageBoundsAVX512DisabledAboveDefaultLimit(b *testing.B) {
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
