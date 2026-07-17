//go:build !purego

package parquet

import (
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

const (
	benchmarkPageBoundsInt64BeforeWriterCutoff = DefaultPageBufferSize * 98 / 100 / 8
	benchmarkPageBoundsInt64AfterWriterCutoff  = benchmarkPageBoundsInt64BeforeWriterCutoff + 1
)

var (
	benchmarkPageBoundsInt64WriterCutoffMin int64
	benchmarkPageBoundsInt64WriterCutoffMax int64
)

func BenchmarkPageBoundsInt64WriterCutoff(b *testing.B) {
	for _, numValues := range [...]int{
		benchmarkPageBoundsInt64BeforeWriterCutoff,
		benchmarkPageBoundsInt64AfterWriterCutoff,
	} {
		b.Run(fmt.Sprintf("%d-values", numValues), func(b *testing.B) {
			values := makeBenchmarkPageBoundsInt64WriterCutoffValues(numValues)
			wantMin, wantMax := benchmarkPageBoundsInt64WriterCutoffBounds(values)
			page := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))

			gotMin, gotMax, ok := page.Bounds()
			if !ok || gotMin.Int64() != wantMin || gotMax.Int64() != wantMax {
				b.Fatalf("page.Bounds() = (%d, %d, %t), want (%d, %d, true)", gotMin.Int64(), gotMax.Int64(), ok, wantMin, wantMax)
			}

			b.SetBytes(int64(len(values)) * 8)
			b.ResetTimer()
			for b.Loop() {
				min, max, _ := page.Bounds()
				benchmarkPageBoundsInt64WriterCutoffMin = min.Int64()
				benchmarkPageBoundsInt64WriterCutoffMax = max.Int64()
			}
			b.StopTimer()

			if benchmarkPageBoundsInt64WriterCutoffMin != wantMin || benchmarkPageBoundsInt64WriterCutoffMax != wantMax {
				b.Fatalf("page.Bounds() = (%d, %d), want (%d, %d)", benchmarkPageBoundsInt64WriterCutoffMin, benchmarkPageBoundsInt64WriterCutoffMax, wantMin, wantMax)
			}
		})
	}
}

func makeBenchmarkPageBoundsInt64WriterCutoffValues(size int) []int64 {
	values := make([]int64, size)
	state := uint64(size)
	for i := range values {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		values[i] = int64(state)
	}
	values[len(values)/3] = math.MinInt64
	values[2*len(values)/3] = math.MaxInt64
	return values
}

func benchmarkPageBoundsInt64WriterCutoffBounds(values []int64) (min, max int64) {
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
