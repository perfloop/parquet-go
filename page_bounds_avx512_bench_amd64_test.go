//go:build !purego && amd64

package parquet

func pageBoundsBenchmarkHasAVX512VL() bool {
	return hasAVX512VL
}
