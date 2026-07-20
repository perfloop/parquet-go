//go:build !purego && amd64

package parquet

import "testing"

func TestInt64PageBoundsWithoutAVX512(t *testing.T) {
	hasAVX512VLBeforeTest := hasAVX512VL
	hasAVX512VL = false
	t.Cleanup(func() {
		hasAVX512VL = hasAVX512VLBeforeTest
	})
	checkInt64PageBounds(t)
}
