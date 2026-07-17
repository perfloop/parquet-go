//go:build !purego

package parquet

import (
	"io"
	"strconv"
	"testing"
)

func BenchmarkBoundsInt64DispatchRange(b *testing.B) {
	for _, numValues := range [...]int{65536, 131071} {
		b.Run(strconv.Itoa(numValues)+"-values", func(b *testing.B) {
			benchmarkBoundsInt64(b, numValues)
		})
	}
}

func BenchmarkBoundsInt64DispatchUpperCutoff(b *testing.B) {
	benchmarkBoundsInt64(b, combinedBoundsThreshold/8)
}

func BenchmarkBoundsInt64WriterDefaultPages(b *testing.B) {
	values := boundsInt64CutoffValues(8 * combinedBoundsInt64Threshold)
	rows := boundsInt64PageFillRows(values)

	for _, statistics := range [...]bool{true, false} {
		name := "WithoutStatistics"
		if statistics {
			name = "Statistics"
		}
		b.Run(name, func(b *testing.B) {
			b.SetBytes(int64(len(values) * 8))
			for b.Loop() {
				writer := NewGenericWriter[boundsInt64WriterRow](io.Discard,
					PageBufferSize(DefaultPageBufferSize),
					DataPageStatistics(statistics),
				)
				n, err := writer.Write(rows)
				if err != nil {
					b.Fatal(err)
				}
				if n != len(rows) {
					b.Fatalf("writer.Write wrote %d rows, want %d", n, len(rows))
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
