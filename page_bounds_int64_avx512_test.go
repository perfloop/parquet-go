//go:build amd64 && !purego

package parquet

import "testing"

func TestInt64PageBoundsWithoutAVX512(t *testing.T) {
	saved := hasAVX512VL
	hasAVX512VL = false
	t.Cleanup(func() { hasAVX512VL = saved })

	testInt64PageBounds(t)
}
