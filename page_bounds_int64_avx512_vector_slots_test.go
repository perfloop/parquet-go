//go:build !purego && amd64

package parquet

import "testing"

func TestInt64PageBoundsAVX512VectorSlots(t *testing.T) {
	if !hasAVX512VL {
		t.Fatal("TestInt64PageBoundsAVX512VectorSlots requires AVX-512VL")
	}

	const (
		// 32,129 has a one-value scalar tail after its 32-wide vector prefix.
		count       = int64PageBoundsDefaultValueCount + 16
		vectorStart = 32 // Avoid the first iteration's broadcast seed.
		vectorWidth = 32
	)

	t.Run("all-lanes", func(t *testing.T) {
		for slot := 0; slot < vectorWidth; slot++ {
			// The rotating max placement makes both extrema visit every lane of the
			// second 32-value iteration, covering all four ZMM load streams.
			minIndex := vectorStart + slot
			maxIndex := vectorStart + (slot+1)%vectorWidth
			values := int64PageBoundsValues(count, minIndex, maxIndex)
			wantMin, wantMax := int64PageBoundsOracle(values)
			min, max, ok := newInt64PageBoundsPage(values).Bounds()

			if !ok {
				t.Errorf("slot %d: Bounds() returned ok=false", slot)
				continue
			}
			if got := min.Int64(); got != wantMin {
				t.Errorf("slot %d: min = %d, want %d", slot, got, wantMin)
			}
			if got := max.Int64(); got != wantMax {
				t.Errorf("slot %d: max = %d, want %d", slot, got, wantMax)
			}
		}
	})
}
