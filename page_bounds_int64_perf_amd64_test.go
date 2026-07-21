//go:build amd64 && !purego

package parquet

import "testing"

func TestInt64PageBoundsAVX512Disabled(t *testing.T) {
	savedHasAVX512VL := hasAVX512VL
	hasAVX512VL = false
	t.Cleanup(func() { hasAVX512VL = savedHasAVX512VL })

	testInt64PageBoundsDifferential(t)
}

func BenchmarkInt64PageBounds(b *testing.B) {
	if !hasAVX512VL {
		b.Skip("requires AVX-512VL")
	}

	values := int64BoundsValues(defaultInt64PageValueCount)
	page := int64BoundsPage(values)
	wantMin, wantMax, wantOK := scalarInt64Bounds(values)

	gotMin, gotMax, gotOK := page.Bounds()
	if gotOK != wantOK || gotMin.int64() != wantMin || gotMax.int64() != wantMax {
		b.Fatal("default-page bounds do not match scalar oracle")
	}

	b.Run("default-page", func(b *testing.B) {
		for b.Loop() {
			gotMin, gotMax, gotOK := page.Bounds()
			if gotOK != wantOK || gotMin.int64() != wantMin || gotMax.int64() != wantMax {
				b.Fatal("default-page bounds do not match scalar oracle")
			}
		}
	})
}
