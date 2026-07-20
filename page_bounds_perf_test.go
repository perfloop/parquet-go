package parquet

import (
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

// ColumnWriter flushes when a page crosses 98% of DefaultPageBufferSize. INT64
// values are eight bytes, so this is the first value count that reaches a
// default-sized page.
const (
	defaultInt64PageValues = (DefaultPageBufferSize*98/100)/8 + 1
	// combinedInt64PageValues is the first INT64 page size above the 1 MiB
	// combinedBoundsThreshold dispatch boundary in page_bounds_amd64.go.
	combinedInt64PageValues = 1024*1024/8 + 1
)

func int64PageBoundsInput(n int) []int64 {
	values := make([]int64, n)
	prng := rand.New(rand.NewSource(int64(n) + 1))
	for i := range values {
		values[i] = prng.Int63()
	}
	if len(values) > 1 {
		// Put the extrema at opposite ends so both the vector body and its
		// scalar tail contribute to the expected result.
		values[0] = 1<<63 - 1
		values[len(values)-1] = -1 << 63
	}
	return values
}

func referenceInt64Bounds(values []int64) (min, max int64, ok bool) {
	if len(values) == 0 {
		return 0, 0, false
	}

	min, max = values[0], values[0]
	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return min, max, true
}

func checkInt64PageBounds(t testing.TB) {
	t.Helper()
	for _, n := range []int{0, 1, 15, 16, 31, 32, defaultInt64PageValues - 1, defaultInt64PageValues, defaultInt64PageValues + 1} {
		values := int64PageBoundsInput(n)
		wantMin, wantMax, wantOK := referenceInt64Bounds(values)
		page := int64Page{values: memory.SliceBufferFrom(values)}
		gotMin, gotMax, gotOK := page.Bounds()

		if gotOK != wantOK {
			t.Fatalf("Bounds() ok for %d values = %t, want %t", n, gotOK, wantOK)
		}
		if gotOK && (gotMin.Int64() != wantMin || gotMax.Int64() != wantMax) {
			t.Fatalf("Bounds() for %d values = (%d, %d), want (%d, %d)", n, gotMin.Int64(), gotMax.Int64(), wantMin, wantMax)
		}
	}
}

func TestInt64PageBoundsDefault(t *testing.T) {
	checkInt64PageBounds(t)
}

func benchmarkInt64PageBounds(b *testing.B, numValues int) {
	values := int64PageBoundsInput(numValues)
	wantMin, wantMax, wantOK := referenceInt64Bounds(values)
	page := int64Page{values: memory.SliceBufferFrom(values)}
	b.SetBytes(int64(len(values) * 8))

	var min, max Value
	var ok bool
	for b.Loop() {
		min, max, ok = page.Bounds()
	}

	if ok != wantOK || (ok && (min.Int64() != wantMin || max.Int64() != wantMax)) {
		b.Fatalf("Bounds() = (%d, %d, %t), want (%d, %d, %t)", min.Int64(), max.Int64(), ok, wantMin, wantMax, wantOK)
	}
}

func BenchmarkInt64PageBounds(b *testing.B) {
	if !pageBoundsBenchmarkHasAVX512VL() {
		b.Fatal("requires AVX-512VL")
	}

	b.Run("default-page", func(b *testing.B) {
		benchmarkInt64PageBounds(b, defaultInt64PageValues)
	})
	b.Run("above-combined-threshold", func(b *testing.B) {
		benchmarkInt64PageBounds(b, combinedInt64PageValues)
	})
}
