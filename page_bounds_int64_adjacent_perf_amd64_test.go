//go:build amd64 && !purego

package parquet

import "testing"

func BenchmarkInt64PageBoundsBelowDefaultPage(b *testing.B) {
	benchmarkInt64PageBounds(b, defaultInt64PageValueCount-1)
}

func BenchmarkInt64PageBoundsAboveDefaultPage(b *testing.B) {
	benchmarkInt64PageBounds(b, defaultInt64PageValueCount+1)
}

func benchmarkInt64PageBounds(b *testing.B, numValues int) {
	if !hasAVX512VL {
		b.Skip("requires AVX-512VL")
	}

	values := int64BoundsValues(numValues)
	page := int64BoundsPage(values)
	wantMin, wantMax, wantOK := scalarInt64Bounds(values)

	gotMin, gotMax, gotOK := page.Bounds()
	if gotOK != wantOK || gotMin.int64() != wantMin || gotMax.int64() != wantMax {
		b.Fatal("page bounds do not match scalar oracle")
	}

	for b.Loop() {
		gotMin, gotMax, gotOK := page.Bounds()
		if gotOK != wantOK || gotMin.int64() != wantMin || gotMax.int64() != wantMax {
			b.Fatal("page bounds do not match scalar oracle")
		}
	}
}
