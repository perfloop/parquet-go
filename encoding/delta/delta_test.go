package delta_test

import (
	"fmt"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/delta"
	"github.com/parquet-go/parquet-go/encoding/fuzz"
	"github.com/parquet-go/parquet-go/encoding/test"
)

func FuzzDeltaBinaryPackedInt32(f *testing.F) {
	fuzz.EncodeInt32(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaBinaryPackedInt64(f *testing.F) {
	fuzz.EncodeInt64(f, new(delta.BinaryPackedEncoding))
}

func FuzzDeltaLengthByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.LengthByteArrayEncoding))
}

func FuzzDeltaByteArray(f *testing.F) {
	fuzz.EncodeByteArray(f, new(delta.ByteArrayEncoding))
}

const (
	encodeMinNumValues = 0
	encodeMaxNumValues = 200
)

func TestEncodeInt32(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 32; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt32(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}

func TestEncodeInt64(t *testing.T) {
	for bitWidth := uint(0); bitWidth <= 64; bitWidth++ {
		t.Run(fmt.Sprintf("bitWidth=%d", bitWidth), func(t *testing.T) {
			test.EncodeInt64(t,
				new(delta.BinaryPackedEncoding),
				encodeMinNumValues,
				encodeMaxNumValues,
				bitWidth,
			)
		})
	}
}

func TestBinaryPackedEncodingInt64PartialBlock(t *testing.T) {
	encoder := new(delta.BinaryPackedEncoding)
	for blockLength := 1; blockLength < 128; blockLength++ {
		t.Run(fmt.Sprintf("blockLength=%d", blockLength), func(t *testing.T) {
			want := partialBlockInt64Values(blockLength, 1_000_000_000_000)

			encoded, err := encoder.EncodeInt64(nil, want)
			if err != nil {
				t.Fatal(err)
			}

			got, err := encoder.DecodeInt64(nil, encoded)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(got, want) {
				t.Fatalf("decoded values mismatch: want=%v got=%v", want, got)
			}
		})
	}
}

func partialBlockInt64Values(blockLength int, firstValue int64) []int64 {
	values := make([]int64, blockLength+1)
	for i := range values {
		values[i] = firstValue + int64(i)
	}
	return values
}
