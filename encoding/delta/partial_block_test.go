package delta_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/delta"
)

func TestBinaryPackedEncodingInt64PartialBlockSize(t *testing.T) {
	encoder := new(delta.BinaryPackedEncoding)
	for blockLength := 1; blockLength < 128; blockLength++ {
		t.Run(fmt.Sprintf("blockLength=%d", blockLength), func(t *testing.T) {
			values := partialBlockInt64ValuesForTest(blockLength)

			encoded, err := encoder.EncodeInt64(nil, values)
			if err != nil {
				t.Fatal(err)
			}
			if blockLength == 5 {
				if got, want := len(encoded), 15; got != want {
					t.Fatalf("encoded size mismatch: want=%d got=%d", want, got)
				}
			}

			decoded, err := encoder.DecodeInt64(nil, encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(decoded, values) {
				t.Fatalf("decoded values mismatch: want=%v got=%v", values, decoded)
			}
		})
	}
}

func partialBlockInt64ValuesForTest(blockLength int) []int64 {
	values := make([]int64, blockLength+1)
	for i := range values {
		values[i] = 1_000_000_000_000 + int64(i)
	}
	return values
}
