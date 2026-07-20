package parquet

import (
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

const (
	defaultPageBufferedBytes   = 98 * DefaultPageBufferSize / 100
	defaultInt64PageValueCount = (defaultPageBufferedBytes + 7) / 8
)

func newInt64BoundsPage(numValues int, seed uint64) *int64Page {
	values := make([]int64, numValues)

	for i := range values {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		values[i] = int64(seed)
	}

	values[0] = math.MinInt64
	values[len(values)-1] = math.MaxInt64

	return &int64Page{
		typ:         Int64Type,
		values:      memory.SliceBufferFrom(values),
		columnIndex: ^uint16(0),
	}
}

func testInt64PageBoundsDefaultPageSize(t *testing.T) {
	t.Helper()

	page := newInt64BoundsPage(defaultInt64PageValueCount, 1)
	min, max, ok := page.Bounds()
	if !ok {
		t.Fatal("Bounds reported no values")
	}
	if got := min.Int64(); got != math.MinInt64 {
		t.Errorf("min = %d, want %d", got, int64(math.MinInt64))
	}
	if got := max.Int64(); got != math.MaxInt64 {
		t.Errorf("max = %d, want %d", got, int64(math.MaxInt64))
	}
}

func TestInt64PageBoundsDefaultPageSize(t *testing.T) {
	testInt64PageBoundsDefaultPageSize(t)
}
