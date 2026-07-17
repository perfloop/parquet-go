package parquet

import (
	"io"
	"math/rand"
	"strconv"
	"testing"
)

const boundsInt64CombinedCutoff = 32113

func boundsInt64CutoffValues(n int) []int64 {
	values := make([]int64, n)
	prng := rand.New(rand.NewSource(int64(n)))
	for i := range values {
		values[i] = int64(prng.Uint64())
	}
	return values
}

func boundsInt64CutoffOracle(values []int64) (min, max int64) {
	min, max = values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max
}

func checkBoundsInt64Cutoff(t *testing.T, values []int64) {
	t.Helper()

	wantMin, wantMax := boundsInt64CutoffOracle(values)
	gotMin, gotMax := boundsInt64(values)
	if gotMin != wantMin || gotMax != wantMax {
		t.Fatalf("boundsInt64(%d values) = (%d, %d), want (%d, %d)", len(values), gotMin, gotMax, wantMin, wantMax)
	}
}

func TestBoundsInt64CombinedCutoff(t *testing.T) {
	checkBoundsInt64Cutoff(t, boundsInt64CutoffValues(boundsInt64CombinedCutoff-1))

	for lane := range 32 {
		values := make([]int64, boundsInt64CombinedCutoff)
		values[lane] = -1
		values[(lane+1)%32] = 1
		checkBoundsInt64Cutoff(t, values)
	}

	for tail := range 32 {
		values := boundsInt64CutoffValues(boundsInt64CombinedCutoff + tail)
		values[len(values)-2] = -1 << 63
		values[len(values)-1] = 1<<63 - 1
		checkBoundsInt64Cutoff(t, values)
	}
}

func BenchmarkBoundsInt64CombinedCutoff(b *testing.B) {
	for _, numValues := range [...]int{boundsInt64CombinedCutoff - 1, boundsInt64CombinedCutoff} {
		b.Run(strconv.Itoa(numValues)+"-values", func(b *testing.B) {
			values := boundsInt64CutoffValues(numValues)
			wantMin, wantMax := boundsInt64CutoffOracle(values)
			b.SetBytes(int64(len(values) * 8))

			var gotMin, gotMax int64
			for b.Loop() {
				gotMin, gotMax = boundsInt64(values)
			}
			if gotMin != wantMin || gotMax != wantMax {
				b.Fatalf("boundsInt64 = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
			}
		})
	}
}

type boundsInt64WriterRow struct {
	Value int64
}

func BenchmarkBoundsInt64WriterDefaultPage(b *testing.B) {
	values := boundsInt64CutoffValues(boundsInt64CombinedCutoff)
	rows := make([]boundsInt64WriterRow, len(values))
	for i, value := range values {
		rows[i].Value = value
	}
	b.SetBytes(int64(len(values) * 8))

	for b.Loop() {
		writer := NewGenericWriter[boundsInt64WriterRow](io.Discard,
			PageBufferSize(DefaultPageBufferSize),
			DataPageStatistics(true),
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
}
