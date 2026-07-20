//go:build !purego

package parquet

import "testing"

const (
	int64ValuesBeforeCombinedThreshold = combinedBoundsThreshold/8 - 1
	int64ValuesAtCombinedThreshold     = combinedBoundsThreshold / 8
)

func BenchmarkInt64PageBoundsBeforeCombinedThreshold(b *testing.B) {
	benchmarkInt64PageBounds(b, int64ValuesBeforeCombinedThreshold)
}

func BenchmarkInt64PageBoundsAtCombinedThreshold(b *testing.B) {
	benchmarkInt64PageBounds(b, int64ValuesAtCombinedThreshold)
}
