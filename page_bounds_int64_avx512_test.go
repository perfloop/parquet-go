//go:build amd64 && !purego

package parquet

import (
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestBoundsInt64AVX512VectorPrefix(t *testing.T) {
	if !hasAVX512VL {
		t.Skip("requires AVX-512VL")
	}

	const pageValueCount = 32_113
	const vectorWidth = 32
	const vectorPrefix = pageValueCount - pageValueCount%vectorWidth

	for lane := range vectorWidth {
		values := make([]int64, pageValueCount)
		for i := range values {
			values[i] = int64(i%1024 - 512)
		}
		for i := vectorPrefix; i < len(values); i++ {
			values[i] = 0
		}

		values[lane] = math.MinInt64
		values[(lane+vectorWidth/2)%vectorWidth] = math.MaxInt64

		wantMin, wantMax := scalarBoundsInt64(values)
		page := int64Type{}.NewPage(0, len(values), encoding.Int64Values(values))
		gotMin, gotMax, gotOK := page.Bounds()
		if !gotOK || gotMin.Int64() != wantMin || gotMax.Int64() != wantMax {
			t.Fatalf("lane=%d: Bounds=(%d,%d,%t), want=(%d,%d,true)", lane, gotMin.Int64(), gotMax.Int64(), gotOK, wantMin, wantMax)
		}
	}
}

func scalarBoundsInt64(values []int64) (min, max int64) {
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
