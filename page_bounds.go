package parquet

import "bytes"

// combinedBoundsInt64Threshold is the first complete INT64 page past the
// writer's 98%-sized default page buffer: 32113 values occupy 256904 bytes,
// just above its 256901-byte target. On AVX-512VL systems, the specialized
// kernel reads page values once while finding both bounds.
const combinedBoundsInt64Threshold = 32113

func boundsFixedLenByteArray(data []byte, size int) (min, max []byte) {
	if len(data) > 0 {
		min = data[:size]
		max = data[:size]

		for i, j := size, 2*size; j <= len(data); {
			item := data[i:j]

			if bytes.Compare(item, min) < 0 {
				min = item
			}
			if bytes.Compare(item, max) > 0 {
				max = item
			}

			i += size
			j += size
		}
	}
	return min, max
}
