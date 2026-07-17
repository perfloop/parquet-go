package parquet

import (
	"math/rand"
	"strconv"
	"testing"
)

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
	checkBoundsInt64Cutoff(t, boundsInt64CutoffValues(combinedBoundsInt64Threshold-1))

	for lane := range 32 {
		values := make([]int64, combinedBoundsInt64Threshold)
		values[lane] = -1
		values[(lane+1)%32] = 1
		checkBoundsInt64Cutoff(t, values)
	}

	for tail := range 32 {
		values := boundsInt64CutoffValues(combinedBoundsInt64Threshold + tail)
		values[len(values)-2] = -1 << 63
		values[len(values)-1] = 1<<63 - 1
		checkBoundsInt64Cutoff(t, values)
	}
}

func BenchmarkBoundsInt64CombinedCutoff(b *testing.B) {
	for _, numValues := range [...]int{combinedBoundsInt64Threshold - 1, combinedBoundsInt64Threshold} {
		b.Run(strconv.Itoa(numValues)+"-values", func(b *testing.B) {
			benchmarkBoundsInt64(b, numValues)
		})
	}
}

func benchmarkBoundsInt64(b *testing.B, numValues int) {
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
}

type boundsInt64WriterRow struct {
	Value int64
}

func BenchmarkBoundsInt64WriterDefaultPage(b *testing.B) {
	benchmarkBoundsInt64WriterPageFill(b, combinedBoundsInt64Threshold)
}
