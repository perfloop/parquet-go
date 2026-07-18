package parquet

import (
	"io"
	"math/rand"
	"testing"
)

const boundsBenchmarkPageWrites = 8

type boundsBenchmarkInt64Row struct {
	Value int64
}

func int64ValuesAtPageFlush(pageBufferSize int) int {
	return 98*pageBufferSize/100/8 + 1
}

func benchmarkBoundsInt64Values(b *testing.B, numValues int) {
	values := make([]int64, numValues)
	prng := rand.New(rand.NewSource(1))
	for i := range values {
		values[i] = prng.Int63()
	}

	wantMin, wantMax := values[0], values[0]
	for _, value := range values[1:] {
		if value < wantMin {
			wantMin = value
		}
		if value > wantMax {
			wantMax = value
		}
	}

	min, max := boundsInt64(values)
	if min != wantMin || max != wantMax {
		b.Fatalf("boundsInt64 returned (%d, %d), want (%d, %d)", min, max, wantMin, wantMax)
	}

	b.SetBytes(int64(8 * len(values)))
	for b.Loop() {
		boundsInt64(values)
	}
}

func BenchmarkBoundsInt64Crossover(b *testing.B) {
	defaultPageValues := int64ValuesAtPageFlush(DefaultPageBufferSize)

	for _, benchmark := range []struct {
		name      string
		numValues int
	}{
		{name: "DefaultMinusOne", numValues: defaultPageValues - 1},
		{name: "DefaultFlush", numValues: defaultPageValues},
		{name: "DefaultPlusOne", numValues: defaultPageValues + 1},
		{name: "512KiB", numValues: int64ValuesAtPageFlush(512 * 1024)},
		{name: "1MiB", numValues: int64ValuesAtPageFlush(1024 * 1024)},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkBoundsInt64Values(b, benchmark.numValues)
		})
	}
}

func BenchmarkWriterInt64PageBounds(b *testing.B) {
	for _, benchmark := range []struct {
		name           string
		pageBufferSize int
	}{
		{name: "Default", pageBufferSize: DefaultPageBufferSize},
		{name: "64KiB", pageBufferSize: 64 * 1024},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			rows := make([]boundsBenchmarkInt64Row, int64ValuesAtPageFlush(benchmark.pageBufferSize))
			prng := rand.New(rand.NewSource(1))
			for i := range rows {
				rows[i].Value = prng.Int63()
			}

			b.SetBytes(int64(boundsBenchmarkPageWrites * len(rows) * 8))
			for b.Loop() {
				writer := NewGenericWriter[boundsBenchmarkInt64Row](io.Discard,
					PageBufferSize(benchmark.pageBufferSize),
					WriteBufferSize(0),
				)
				for range boundsBenchmarkPageWrites {
					n, err := writer.Write(rows)
					if err != nil {
						b.Fatal(err)
					}
					if n != len(rows) {
						b.Fatalf("writer wrote %d rows, want %d", n, len(rows))
					}
				}
				if err := writer.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
