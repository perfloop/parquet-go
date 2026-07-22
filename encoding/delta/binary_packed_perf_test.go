package delta

import (
	"bytes"
	"slices"
	"testing"

	"github.com/parquet-go/bitpack"
	"github.com/parquet-go/bitpack/unsafecast"
)

const decodeInt64BenchmarkValueCount = 8193

func decodeInt64BenchmarkValues() []int64 {
	values := make([]int64, decodeInt64BenchmarkValueCount)
	for i := range values {
		values[i] = int64(i) * int64(i)
	}
	return values
}

func paddedDecodeInt64Input(encoded []byte) []byte {
	input := make([]byte, len(encoded), len(encoded)+bitpack.PaddingInt64)
	copy(input, encoded)
	return input
}

func assertDecodedInt64(t testing.TB, values []int64, decoded []byte) {
	t.Helper()
	if got := unsafecast.Slice[int64](decoded); !slices.Equal(got, values) {
		t.Fatalf("decoded values differ: got %d values, want %d", len(got), len(values))
	}
}

func TestDecodeInt64PaddingBoundaries(t *testing.T) {
	values := decodeInt64BenchmarkValues()
	encoded := encodeInt64(nil, values)

	t.Run("capacity-padded trailing input", func(t *testing.T) {
		trailing := []byte{0xDE, 0xAD, 0xBE, 0xEF}
		input := make([]byte, len(encoded)+len(trailing), len(encoded)+len(trailing)+bitpack.PaddingInt64)
		copy(input, encoded)
		copy(input[len(encoded):], trailing)

		decoded, remain, err := decodeInt64(nil, input)
		if err != nil {
			t.Fatalf("decode padded input: %v", err)
		}
		assertDecodedInt64(t, values, decoded)
		if !bytes.Equal(remain, trailing) {
			t.Fatalf("trailing input changed: got %x, want %x", remain, trailing)
		}
	})

	t.Run("capacity-clamped input", func(t *testing.T) {
		input := append([]byte(nil), encoded...)

		decoded, remain, err := decodeInt64(nil, input)
		if err != nil {
			t.Fatalf("decode clamped input: %v", err)
		}
		assertDecodedInt64(t, values, decoded)
		if len(remain) != 0 {
			t.Fatalf("unexpected remaining input: %x", remain)
		}
	})

	t.Run("truncated input", func(t *testing.T) {
		truncated := append([]byte(nil), encoded[:len(encoded)-1]...)
		zeroExtended := make([]byte, len(truncated)+1+bitpack.PaddingInt64)
		copy(zeroExtended, truncated)

		want, _, err := decodeInt64(nil, zeroExtended)
		if err != nil {
			t.Fatalf("decode zero-extended reference: %v", err)
		}
		got, _, err := decodeInt64(nil, truncated)
		if err != nil {
			t.Fatalf("decode truncated input: %v", err)
		}
		if !bytes.Equal(got, want) {
			t.Fatal("truncated input did not preserve zero-padding behavior")
		}
	})
}

func BenchmarkDecodeInt64Padded(b *testing.B) {
	values := decodeInt64BenchmarkValues()
	input := paddedDecodeInt64Input(encodeInt64(nil, values))
	decoder := new(BinaryPackedEncoding)
	decoded := make([]int64, 0, len(values))

	var err error
	decoded, err = decoder.DecodeInt64(decoded, input)
	if err != nil {
		b.Fatalf("decode benchmark input: %v", err)
	}
	if !slices.Equal(decoded, values) {
		b.Fatal("decode benchmark input produced wrong values")
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(values)) * 8)
	b.ResetTimer()
	for b.Loop() {
		decoded, _ = decoder.DecodeInt64(decoded[:0], input)
	}
	b.StopTimer()

	if !slices.Equal(decoded, values) {
		b.Fatal("decode benchmark produced wrong values")
	}
}
