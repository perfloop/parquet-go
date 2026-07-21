//go:build !purego

package parquet

import (
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

const (
	int64BoundsAVX512FloorValueCount = (combinedBoundsInt64AVX512Floor + 7) / 8
	int64BoundsAVX512BeforeThreshold = combinedBoundsThreshold/8 - 1
)

func TestInt64PageBoundsAVX512Window(t *testing.T) {
	if !hasAVX512VL {
		t.Skip("requires AVX-512VL")
	}

	layouts := []struct {
		name         string
		minIndex     func(int) int
		maxIndex     func(int) int
		requiresTail bool
	}{
		{
			name:     "z1-min-z2-max",
			minIndex: func(int) int { return 10 },
			maxIndex: func(int) int { return 3 },
		},
		{
			name:     "z0-min-z3-max",
			minIndex: func(int) int { return 4 },
			maxIndex: func(int) int { return 13 },
		},
		{
			name:         "tail-min",
			minIndex:     func(n int) int { return n - 1 },
			maxIndex:     func(int) int { return 3 },
			requiresTail: true,
		},
		{
			name:         "tail-max",
			minIndex:     func(int) int { return 4 },
			maxIndex:     func(n int) int { return n - 1 },
			requiresTail: true,
		},
	}
	boundaries := []struct {
		name        string
		first, last int
	}{
		{
			name:  "floor",
			first: int64BoundsAVX512FloorValueCount,
			last:  int64BoundsAVX512FloorValueCount + 15,
		},
		{
			name:  "before-combined-threshold",
			first: int64BoundsAVX512BeforeThreshold - 15,
			last:  int64BoundsAVX512BeforeThreshold,
		},
	}

	for _, boundary := range boundaries {
		for numValues := boundary.first; numValues <= boundary.last; numValues++ {
			tail := numValues % 16
			if size := 8 * numValues; size < combinedBoundsInt64AVX512Floor || size >= combinedBoundsThreshold {
				t.Fatalf("%s tail=%d has size %d outside AVX-512 window", boundary.name, tail, size)
			}

			for _, layout := range layouts {
				if layout.requiresTail && tail == 0 {
					continue
				}

				t.Run(fmt.Sprintf("%s/tail=%d/%s", boundary.name, tail, layout.name), func(t *testing.T) {
					values := make([]int64, numValues)
					values[layout.minIndex(numValues)] = math.MinInt64
					values[layout.maxIndex(numValues)] = math.MaxInt64

					wantMin, wantMax := scalarInt64Bounds(values)
					page := &int64Page{
						typ:         Int64Type,
						values:      memory.SliceBufferFrom(values),
						columnIndex: ^uint16(0),
					}
					min, max, ok := page.Bounds()
					if !ok || min.Int64() != wantMin || max.Int64() != wantMax {
						t.Fatalf("bounds = (%d, %d, %t), want (%d, %d, true)", min.Int64(), max.Int64(), ok, wantMin, wantMax)
					}
				})
			}
		}
	}
}

func scalarInt64Bounds(values []int64) (min, max int64) {
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
