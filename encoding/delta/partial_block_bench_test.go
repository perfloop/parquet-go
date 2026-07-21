package delta_test

import (
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/delta"
)

func BenchmarkBinaryPackedEncodingInt64PartialBlockPayload(b *testing.B) {
	values := partialBlockInt64Values(5, 1_000_000_000_000+int64(b.N&1))
	encoder := new(delta.BinaryPackedEncoding)
	var encoded []byte
	var err error

	for b.Loop() {
		encoded, err = encoder.EncodeInt64(encoded[:0], values)
		if err != nil {
			b.Fatal(err)
		}
	}

	decoded, err := encoder.DecodeInt64(nil, encoded)
	if err != nil {
		b.Fatal(err)
	}
	if !slices.Equal(decoded, values) {
		b.Fatalf("decoded values mismatch: want=%v got=%v", values, decoded)
	}
	b.ReportMetric(float64(len(encoded)), "encoded_bytes/op")
}
