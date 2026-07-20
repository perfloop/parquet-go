//go:build !purego && amd64

package parquet

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

const (
	avx512DispatchThresholdValues = (DefaultPageBufferSize*98/100)/8 + 1
	belowCombinedThresholdValues  = 1024*1024/8 - 1
)

func avx512DispatchValues(n int) []int64 {
	values := make([]int64, n)
	for i := range values {
		values[i] = int64(i + 1)
	}
	return values
}

func assertAVX512DispatchBounds(t *testing.T, values []int64) {
	t.Helper()
	wantMin, wantMax, wantOK := referenceInt64Bounds(values)
	page := int64Page{values: memory.SliceBufferFrom(values)}

	min, max, ok := page.Bounds()
	if ok != wantOK || (ok && (min.Int64() != wantMin || max.Int64() != wantMax)) {
		t.Fatalf("AVX-512 Bounds() = (%d, %d, %t), want (%d, %d, %t)", min.Int64(), max.Int64(), ok, wantMin, wantMax, wantOK)
	}

	hasAVX512VLBeforeFallback := hasAVX512VL
	hasAVX512VL = false
	fallbackMin, fallbackMax, fallbackOK := page.Bounds()
	hasAVX512VL = hasAVX512VLBeforeFallback
	if fallbackOK != wantOK || (fallbackOK && (fallbackMin.Int64() != wantMin || fallbackMax.Int64() != wantMax)) {
		t.Fatalf("fallback Bounds() = (%d, %d, %t), want (%d, %d, %t)", fallbackMin.Int64(), fallbackMax.Int64(), fallbackOK, wantMin, wantMax, wantOK)
	}
	if ok != fallbackOK || (ok && (min.Int64() != fallbackMin.Int64() || max.Int64() != fallbackMax.Int64())) {
		t.Fatalf("AVX-512 Bounds() = (%d, %d, %t), fallback = (%d, %d, %t)", min.Int64(), max.Int64(), ok, fallbackMin.Int64(), fallbackMax.Int64(), fallbackOK)
	}
}

func TestInt64PageBoundsAVX512Dispatch(t *testing.T) {
	if !hasAVX512VL {
		t.Skip("requires AVX-512VL")
	}

	const (
		minInt64     = -1 << 63
		maxInt64     = 1<<63 - 1
		vectorValues = 32 * 1024
	)
	for _, index := range []int{1, 9, 17, 25} {
		t.Run(fmt.Sprintf("minimum-in-vector-%d", index/8), func(t *testing.T) {
			values := avx512DispatchValues(vectorValues)
			values[index] = minInt64
			assertAVX512DispatchBounds(t, values)
		})
		t.Run(fmt.Sprintf("maximum-in-vector-%d", index/8), func(t *testing.T) {
			values := avx512DispatchValues(vectorValues)
			values[index] = maxInt64
			assertAVX512DispatchBounds(t, values)
		})
	}

	t.Run("signed-vector-comparisons", func(t *testing.T) {
		values := avx512DispatchValues(vectorValues)
		values[9] = -1
		values[17] = minInt64
		assertAVX512DispatchBounds(t, values)
	})
	t.Run("vector-minimum-and-tail-maximum", func(t *testing.T) {
		values := avx512DispatchValues(vectorValues + 1)
		values[9] = minInt64
		values[len(values)-1] = maxInt64
		assertAVX512DispatchBounds(t, values)
	})
}

func avx512DispatchBenchmarkValues(n int) []int64 {
	values := make([]int64, n)
	prng := rand.New(rand.NewSource(int64(n) + 1))
	for i := range values {
		values[i] = prng.Int63()
	}
	values[9] = -1 << 63
	values[len(values)-1] = 1<<63 - 1
	return values
}

func benchmarkInt64PageBoundsDispatch(b *testing.B, numValues int) {
	values := avx512DispatchBenchmarkValues(numValues)
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

func BenchmarkInt64PageBoundsDispatch(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("requires AVX-512VL")
	}

	for _, benchmark := range []struct {
		name      string
		numValues int
	}{
		{name: "below-avx512-threshold", numValues: avx512DispatchThresholdValues - 1},
		{name: "window-512KiB", numValues: 512 * 1024 / 8},
		{name: "window-768KiB", numValues: 768 * 1024 / 8},
		{name: "below-combined-threshold", numValues: belowCombinedThresholdValues},
	} {
		b.Run(benchmark.name, func(b *testing.B) {
			benchmarkInt64PageBoundsDispatch(b, benchmark.numValues)
		})
	}

	b.Run("avx512-disabled-default", func(b *testing.B) {
		hasAVX512VLBeforeBenchmark := hasAVX512VL
		hasAVX512VL = false
		b.Cleanup(func() {
			hasAVX512VL = hasAVX512VLBeforeBenchmark
		})
		benchmarkInt64PageBoundsDispatch(b, avx512DispatchThresholdValues)
	})
}
