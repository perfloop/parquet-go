package parquet

import (
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

// A default 256 KiB page flushes after 32,113 INT64 values: ColumnWriter uses
// 98% of the configured page buffer, so 32,112 values fit and the next one
// triggers the flush.
const defaultInt64PageValueCount = 32_113

func int64BoundsValues(n int) []int64 {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(17*i - 11*n)
	}
	if n > 0 {
		// Keep both extrema in the scalar tail for the non-multiple-of-32
		// dispatching sizes exercised below.
		values[n-1] = -1 << 63
	}
	if n > 1 {
		values[n-2] = 1<<63 - 1
	}
	return values
}

func int64BoundsPage(values []int64) Page {
	return int64Type{}.NewPage(0, len(values), encoding.Int64Values(values))
}

func scalarInt64Bounds(values []int64) (min, max int64, ok bool) {
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

func testInt64PageBoundsDifferential(t testing.TB) {
	t.Helper()
	for _, n := range []int{
		0,
		1,
		31,
		32,
		defaultInt64PageValueCount - 1,
		defaultInt64PageValueCount,
		defaultInt64PageValueCount + 1,
		(1 << 17) - 1,
		1 << 17,
	} {
		values := int64BoundsValues(n)
		wantMin, wantMax, wantOK := scalarInt64Bounds(values)
		gotMin, gotMax, gotOK := int64BoundsPage(values).Bounds()

		if gotOK != wantOK {
			t.Fatalf("len=%d: Bounds ok=%t, want %t", n, gotOK, wantOK)
		}
		if !gotOK {
			continue
		}
		if gotMin.int64() != wantMin || gotMax.int64() != wantMax {
			t.Fatalf("len=%d: Bounds=(%d,%d), want (%d,%d)", n, gotMin.int64(), gotMax.int64(), wantMin, wantMax)
		}
	}
}

func TestInt64PageBoundsDifferential(t *testing.T) {
	testInt64PageBoundsDifferential(t)
}
