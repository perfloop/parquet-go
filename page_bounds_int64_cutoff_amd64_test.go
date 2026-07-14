//go:build !purego

package parquet

import (
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestBoundsInt64DefaultPageSizeBoundary(t *testing.T) {
	for _, size := range []int{
		0,
		1,
		DefaultPageBufferSize/8 - 1,
		DefaultPageBufferSize / 8,
	} {
		t.Run(fmt.Sprintf("%d-values", size), func(t *testing.T) {
			if size == 0 {
				gotMin, gotMax := boundsInt64(nil)
				if gotMin != 0 || gotMax != 0 {
					t.Fatalf("boundsInt64(nil) = (%d, %d), want (0, 0)", gotMin, gotMax)
				}
				return
			}

			assertBoundsInt64(t, makeBenchmarkBoundsInt64Values(size))
		})
	}

	if !hasAVX512VL {
		return
	}

	const vectorLoopValues = DefaultPageBufferSize / 8
	for offset := range 32 {
		t.Run(fmt.Sprintf("vector-lane-%d", offset), func(t *testing.T) {
			values := make([]int64, vectorLoopValues)
			values[offset] = math.MinInt64
			values[(offset+1)%32] = math.MaxInt64
			assertBoundsInt64(t, values)
		})
	}

	for tail := range 32 {
		t.Run(fmt.Sprintf("tail-%d", tail), func(t *testing.T) {
			values := make([]int64, vectorLoopValues+tail)
			if tail == 0 {
				values[0] = math.MinInt64
				values[1] = math.MaxInt64
			} else {
				values[len(values)-1] = math.MinInt64
				if tail == 1 {
					values[0] = math.MaxInt64
				} else {
					values[len(values)-2] = math.MaxInt64
				}
			}
			assertBoundsInt64(t, values)
		})
	}
}

func assertBoundsInt64(t *testing.T, values []int64) {
	t.Helper()
	wantMin, wantMax := scalarBoundsInt64(values)
	gotMin, gotMax := boundsInt64(values)
	if gotMin != wantMin || gotMax != wantMax {
		t.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}
}

func BenchmarkPageBoundsInt64BelowDefaultPageSize(b *testing.B) {
	const bufferSize = DefaultPageBufferSize - 8

	values := makeBenchmarkBoundsInt64Values(bufferSize / 8)
	wantMin, wantMax := scalarBoundsInt64(values)
	page := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))

	gotMin, gotMax, ok := page.Bounds()
	if !ok || gotMin.Int64() != wantMin || gotMax.Int64() != wantMax {
		b.Fatalf("page.Bounds() = (%d, %d, %t), want (%d, %d, true)", gotMin.Int64(), gotMax.Int64(), ok, wantMin, wantMax)
	}

	b.SetBytes(bufferSize)
	b.ResetTimer()
	for b.Loop() {
		min, max, _ := page.Bounds()
		benchmarkPageBoundsInt64MinSink = min.Int64()
		benchmarkPageBoundsInt64MaxSink = max.Int64()
	}
	b.StopTimer()

	if benchmarkPageBoundsInt64MinSink != wantMin || benchmarkPageBoundsInt64MaxSink != wantMax {
		b.Fatalf("page.Bounds() = (%d, %d), want (%d, %d)", benchmarkPageBoundsInt64MinSink, benchmarkPageBoundsInt64MaxSink, wantMin, wantMax)
	}
}
