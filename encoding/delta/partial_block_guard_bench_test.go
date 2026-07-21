package delta_test

import (
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/delta"
	"golang.org/x/sys/cpu"
)

func BenchmarkBinaryPackedEncodingInt64NativePartialFive(b *testing.B) {
	requireAVX2ForBenchmark(b)
	benchmarkBinaryPackedInt64Guard(b, 5)
}

func BenchmarkBinaryPackedEncodingInt64NativePartial127(b *testing.B) {
	requireAVX2ForBenchmark(b)
	benchmarkBinaryPackedInt64Guard(b, 127)
}

func BenchmarkBinaryPackedEncodingInt64PortableFullBlocks(b *testing.B) {
	benchmarkBinaryPackedInt64Guard(b, 2*128)
}

func BenchmarkBinaryPackedEncodingInt64NativeFullBlocks(b *testing.B) {
	requireAVX2ForBenchmark(b)
	benchmarkBinaryPackedInt64Guard(b, 2*128)
}

func requireAVX2ForBenchmark(b testing.TB) {
	if !cpu.X86.HasAVX2 {
		b.Skip("CPU does not support AVX2")
	}
}

func benchmarkBinaryPackedInt64Guard(b *testing.B, numDeltas int) {
	values := guardInt64Values(numDeltas, 1_000_000_000_000+int64(b.N&1))
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

func guardInt64Values(numDeltas int, firstValue int64) []int64 {
	values := make([]int64, numDeltas+1)
	for i := range values {
		values[i] = firstValue + int64(i)
	}
	return values
}
