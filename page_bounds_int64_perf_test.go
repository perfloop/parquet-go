package parquet

import (
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

// defaultInt64PageValueCount is the first INT64 page to exceed the writer's
// 98%-of-DefaultPageBufferSize target: ceil(0.98*256KiB/8) == 32113.
const defaultInt64PageValueCount = 32113

func int64BoundsValues(n int) []int64 {
	return int64BoundsValuesWithSeed(n, uint64(n)^0x9E3779B97F4A7C15)
}

func int64BoundsValuesWithSeed(n int, state uint64) []int64 {
	values := make([]int64, n)

	for i := range values {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		values[i] = int64(state)
	}

	if n > 0 {
		values[n/3] = math.MinInt64
		values[(2*n)/3] = math.MaxInt64
	}
	return values
}

func int64BoundsReference(values []int64) (min, max int64, ok bool) {
	if len(values) == 0 {
		return 0, 0, false
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
	return min, max, true
}

func testInt64PageBounds(t *testing.T) {
	t.Helper()

	for _, size := range []int{
		0,
		1,
		15,
		16,
		31,
		32,
		defaultInt64PageValueCount - 1,
		defaultInt64PageValueCount,
		131071,
		131072,
		131073,
	} {
		t.Run(fmt.Sprintf("len=%d", size), func(t *testing.T) {
			values := int64BoundsValues(size)
			wantMin, wantMax, wantOK := int64BoundsReference(values)
			page := Page(&int64Page{values: memory.SliceBufferFrom(values)})
			gotMin, gotMax, gotOK := page.Bounds()

			if gotOK != wantOK {
				t.Fatalf("len=%d: Bounds() ok=%t, want %t", size, gotOK, wantOK)
			}
			if !wantOK {
				return
			}
			if gotMin.Int64() != wantMin {
				t.Errorf("len=%d: Bounds() min=%d, want %d", size, gotMin.Int64(), wantMin)
			}
			if gotMax.Int64() != wantMax {
				t.Errorf("len=%d: Bounds() max=%d, want %d", size, gotMax.Int64(), wantMax)
			}
		})
	}
}

func TestInt64PageBounds(t *testing.T) {
	testInt64PageBounds(t)
}
