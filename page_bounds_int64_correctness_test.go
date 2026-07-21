package parquet

import "testing"

func TestInt64PageBoundsWindow(t *testing.T) {
	testInt64PageBoundsWindow(t)
}

func TestInt64PageBoundsStatistics(t *testing.T) {
	values := int64PageBoundsValues(
		int64PageBoundsDefaultValueCount,
		int64PageBoundsDefaultValueCount-1,
		int64PageBoundsDefaultValueCount-2,
	)
	wantMin, wantMax := int64PageBoundsOracle(values)
	statistics := (&ColumnWriter{}).makePageStatistics(newInt64PageBoundsPage(values))

	if got := Int64.Value(statistics.MinValue).Int64(); got != wantMin {
		t.Errorf("MinValue = %d, want %d", got, wantMin)
	}
	if got := Int64.Value(statistics.MaxValue).Int64(); got != wantMax {
		t.Errorf("MaxValue = %d, want %d", got, wantMax)
	}
	if got := Int64.Value(statistics.Min).Int64(); got != wantMin {
		t.Errorf("Min = %d, want %d", got, wantMin)
	}
	if got := Int64.Value(statistics.Max).Int64(); got != wantMax {
		t.Errorf("Max = %d, want %d", got, wantMax)
	}
}

func testInt64PageBoundsWindow(t *testing.T) {
	t.Helper()

	for _, test := range []struct {
		name     string
		count    int
		minIndex int
		maxIndex int
	}{
		{
			name:     "below-window",
			count:    int64PageBoundsBelowWindowCount,
			minIndex: int64PageBoundsBelowWindowCount - 1,
			maxIndex: int64PageBoundsBelowWindowCount - 2,
		},
		{
			name:     "default-page-tail-extrema",
			count:    int64PageBoundsDefaultValueCount,
			minIndex: int64PageBoundsDefaultValueCount - 1,
			maxIndex: int64PageBoundsDefaultValueCount - 2,
		},
		{
			// 32,129 has a one-value scalar tail after a 32-wide vector prefix.
			name:     "one-element-tail-minimum",
			count:    int64PageBoundsDefaultValueCount + 16,
			minIndex: int64PageBoundsDefaultValueCount + 15,
			maxIndex: 0,
		},
		{
			name:     "one-element-tail-maximum",
			count:    int64PageBoundsDefaultValueCount + 16,
			minIndex: 0,
			maxIndex: int64PageBoundsDefaultValueCount + 15,
		},
		{
			// 32,127 leaves 31 values after the vector prefix.
			name:     "thirty-one-element-tail-extrema",
			count:    int64PageBoundsDefaultValueCount + 14,
			minIndex: int64PageBoundsDefaultValueCount + 13,
			maxIndex: int64PageBoundsDefaultValueCount + 12,
		},
		{
			name:     "upper-window-tail-extrema",
			count:    int64PageBoundsAboveWindowCount - 1,
			minIndex: int64PageBoundsAboveWindowCount - 2,
			maxIndex: int64PageBoundsAboveWindowCount - 3,
		},
		{
			name:     "upper-handoff",
			count:    int64PageBoundsAboveWindowCount,
			minIndex: int64PageBoundsAboveWindowCount - 1,
			maxIndex: int64PageBoundsAboveWindowCount - 2,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			values := int64PageBoundsValues(test.count, test.minIndex, test.maxIndex)
			wantMin, wantMax := int64PageBoundsOracle(values)
			min, max, ok := newInt64PageBoundsPage(values).Bounds()

			if !ok {
				t.Fatal("Bounds() returned ok=false")
			}
			if got := min.Int64(); got != wantMin {
				t.Errorf("min = %d, want %d", got, wantMin)
			}
			if got := max.Int64(); got != wantMax {
				t.Errorf("max = %d, want %d", got, wantMax)
			}
		})
	}
}
