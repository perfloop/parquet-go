//go:build !purego && amd64

package parquet

import "testing"

func TestInt64PageBoundsAVX512Disabled(t *testing.T) {
	wasAVX512VL := hasAVX512VL
	hasAVX512VL = false
	t.Cleanup(func() { hasAVX512VL = wasAVX512VL })

	testInt64PageBoundsWindow(t)
}
