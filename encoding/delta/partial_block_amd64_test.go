//go:build amd64 && !purego

package delta

import (
	"bytes"
	"testing"
)

func TestEncodeInt64AVX2PartialBlockParityCoverage(t *testing.T) {
	requireAVX2(t)

	for blockLength := 1; blockLength < blockSize; blockLength++ {
		values := make([]int64, blockLength+1)
		for i := range values {
			values[i] = 1_000_000_000_000 + int64(i)
		}

		defaultOutput := encodeInt64Default(nil, values)
		avx2Output := encodeInt64AVX2(nil, values)
		if !bytes.Equal(avx2Output, defaultOutput) {
			t.Fatalf("partial block output mismatch for length %d", blockLength)
		}
	}
}
