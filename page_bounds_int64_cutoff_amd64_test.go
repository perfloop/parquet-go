//go:build !purego

package parquet

import (
	"fmt"
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

			values := makeBenchmarkBoundsInt64Values(size)
			wantMin, wantMax := scalarBoundsInt64(values)
			gotMin, gotMax := boundsInt64(values)
			if gotMin != wantMin || gotMax != wantMax {
				t.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
			}
		})
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
