package parquet

import (
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

const (
	// ColumnWriter starts a default 256 KiB INT64 page at this value count: its
	// page buffer target is 98% of DefaultPageBufferSize, rounded down.
	int64PageBoundsDefaultValueCount = (DefaultPageBufferSize*98/100)/8 + 1
	int64PageBoundsBelowWindowCount  = int64PageBoundsDefaultValueCount - 1

	// combinedBoundsThreshold is the existing 1 MiB handoff from separate min
	// and max scans to the general combined kernel. Keep the benchmark boundary
	// independent from the implementation constant so purego test builds compile.
	int64PageBoundsAboveWindowCount = (1024 * 1024) / 8
)

func BenchmarkInt64PageBounds(b *testing.B) {
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

func newInt64PageBoundsPage(values []int64) *int64Page {
	return newInt64Page(Int64Type, 0, int32(len(values)), encoding.Int64Values(values))
}

func int64PageBoundsValues(count, minIndex, maxIndex int) []int64 {
	values := make([]int64, count)
	state := uint64(count) + 0x9E3779B97F4A7C15
	for i := range values {
		state = state*6364136223846793005 + 1442695040888963407
		values[i] = int64(state >> 1)
	}
	values[minIndex] = math.MinInt64
	values[maxIndex] = math.MaxInt64
	return values
}

func int64PageBoundsOracle(values []int64) (min, max int64) {
	if len(values) == 0 {
		return 0, 0
	}

	min, max = values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}
