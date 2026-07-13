//go:build !purego

package parquet

import (
	"fmt"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestBoundsThresholdBoundary(t *testing.T) {
	t.Run("Int32", testBoundsInt32ThresholdBoundary)
	t.Run("Int64", testBoundsInt64ThresholdBoundary)
	t.Run("Uint32", testBoundsUint32ThresholdBoundary)
	t.Run("Uint64", testBoundsUint64ThresholdBoundary)
	t.Run("Float32", testBoundsFloat32ThresholdBoundary)
	t.Run("Float64", testBoundsFloat64ThresholdBoundary)
}

func testBoundsInt32ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 4, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsInt32Values(size)
		wantMin, wantMax := scalarBoundsInt32(values)
		gotMin, gotMax := boundsInt32(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsInt32() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func testBoundsInt64ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 8, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsInt64Values(size)
		wantMin, wantMax := scalarBoundsInt64(values)
		gotMin, gotMax := boundsInt64(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func testBoundsUint32ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 4, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsUint32Values(size)
		wantMin, wantMax := scalarBoundsUint32(values)
		gotMin, gotMax := boundsUint32(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsUint32() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func testBoundsUint64ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 8, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsUint64Values(size)
		wantMin, wantMax := scalarBoundsUint64(values)
		gotMin, gotMax := boundsUint64(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsUint64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func testBoundsFloat32ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 4, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsFloat32Values(size)
		wantMin, wantMax := scalarBoundsFloat32(values)
		gotMin, gotMax := boundsFloat32(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsFloat32() = (%g, %g), want (%g, %g)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func testBoundsFloat64ThresholdBoundary(t *testing.T) {
	forEachBoundsThresholdSize(t, 8, func(t *testing.T, size int) {
		values := makeBenchmarkBoundsFloat64Values(size)
		wantMin, wantMax := scalarBoundsFloat64(values)
		gotMin, gotMax := boundsFloat64(values)
		if gotMin != wantMin || gotMax != wantMax {
			t.Fatalf("boundsFloat64() = (%g, %g), want (%g, %g)", gotMin, gotMax, wantMin, wantMax)
		}
	})
}

func forEachBoundsThresholdSize(t *testing.T, elementSize int, test func(*testing.T, int)) {
	t.Helper()
	for _, byteSize := range []int{
		combinedBoundsThreshold - elementSize,
		combinedBoundsThreshold,
	} {
		t.Run(fmt.Sprintf("%dKiB", byteSize/1024), func(t *testing.T) {
			test(t, byteSize/elementSize)
		})
	}
}

var (
	benchmarkPageBoundsInt64MinSink int64
	benchmarkPageBoundsInt64MaxSink int64
	benchmarkBoundsInt32MinSink     int32
	benchmarkBoundsInt32MaxSink     int32
	benchmarkBoundsInt64MinSink     int64
	benchmarkBoundsInt64MaxSink     int64
	benchmarkBoundsUint32MinSink    uint32
	benchmarkBoundsUint32MaxSink    uint32
	benchmarkBoundsUint64MinSink    uint64
	benchmarkBoundsUint64MaxSink    uint64
	benchmarkBoundsFloat32MinSink   float32
	benchmarkBoundsFloat32MaxSink   float32
	benchmarkBoundsFloat64MinSink   float64
	benchmarkBoundsFloat64MaxSink   float64
)

func BenchmarkPageBoundsInt64(b *testing.B) {
	for _, bufferSize := range []int{
		4 * 1024,
		DefaultPageBufferSize,
		512 * 1024,
		combinedBoundsThreshold,
	} {
		b.Run(fmt.Sprintf("%dKiB", bufferSize/1024), func(b *testing.B) {
			values := makeBenchmarkBoundsInt64Values(bufferSize / 8)
			wantMin, wantMax := scalarBoundsInt64(values)
			page := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))

			gotMin, gotMax, ok := page.Bounds()
			if !ok || gotMin.Int64() != wantMin || gotMax.Int64() != wantMax {
				b.Fatalf("page.Bounds() = (%d, %d, %t), want (%d, %d, true)", gotMin.Int64(), gotMax.Int64(), ok, wantMin, wantMax)
			}

			b.SetBytes(int64(bufferSize))
			b.ResetTimer()
			for b.Loop() {
				min, max, _ := page.Bounds()
				benchmarkPageBoundsInt64MinSink = min.Int64()
				benchmarkPageBoundsInt64MaxSink = max.Int64()
			}
			b.StopTimer()

			if benchmarkPageBoundsInt64MinSink != wantMin || benchmarkPageBoundsInt64MaxSink != wantMax {
				b.Fatalf("page.Bounds() = (%d, %d), want (%d, %d)", benchmarkPageBoundsInt64MinSink, benchmarkPageBoundsInt64MaxSink, wantMin, wantMax)
			}
		})
	}
}

func BenchmarkBoundsInt32DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsInt32Values(DefaultPageBufferSize / 4)
	wantMin, wantMax := scalarBoundsInt32(values)
	gotMin, gotMax := boundsInt32(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsInt32() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsInt32MinSink, benchmarkBoundsInt32MaxSink = boundsInt32(values)
	}
	b.StopTimer()

	if benchmarkBoundsInt32MinSink != wantMin || benchmarkBoundsInt32MaxSink != wantMax {
		b.Fatalf("boundsInt32() = (%d, %d), want (%d, %d)", benchmarkBoundsInt32MinSink, benchmarkBoundsInt32MaxSink, wantMin, wantMax)
	}
}

func BenchmarkBoundsInt64DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsInt64Values(DefaultPageBufferSize / 8)
	wantMin, wantMax := scalarBoundsInt64(values)
	gotMin, gotMax := boundsInt64(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsInt64MinSink, benchmarkBoundsInt64MaxSink = boundsInt64(values)
	}
	b.StopTimer()

	if benchmarkBoundsInt64MinSink != wantMin || benchmarkBoundsInt64MaxSink != wantMax {
		b.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", benchmarkBoundsInt64MinSink, benchmarkBoundsInt64MaxSink, wantMin, wantMax)
	}
}

func BenchmarkBoundsUint32DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsUint32Values(DefaultPageBufferSize / 4)
	wantMin, wantMax := scalarBoundsUint32(values)
	gotMin, gotMax := boundsUint32(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsUint32() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsUint32MinSink, benchmarkBoundsUint32MaxSink = boundsUint32(values)
	}
	b.StopTimer()

	if benchmarkBoundsUint32MinSink != wantMin || benchmarkBoundsUint32MaxSink != wantMax {
		b.Fatalf("boundsUint32() = (%d, %d), want (%d, %d)", benchmarkBoundsUint32MinSink, benchmarkBoundsUint32MaxSink, wantMin, wantMax)
	}
}

func BenchmarkBoundsUint64DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsUint64Values(DefaultPageBufferSize / 8)
	wantMin, wantMax := scalarBoundsUint64(values)
	gotMin, gotMax := boundsUint64(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsUint64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsUint64MinSink, benchmarkBoundsUint64MaxSink = boundsUint64(values)
	}
	b.StopTimer()

	if benchmarkBoundsUint64MinSink != wantMin || benchmarkBoundsUint64MaxSink != wantMax {
		b.Fatalf("boundsUint64() = (%d, %d), want (%d, %d)", benchmarkBoundsUint64MinSink, benchmarkBoundsUint64MaxSink, wantMin, wantMax)
	}
}

func BenchmarkBoundsFloat32DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsFloat32Values(DefaultPageBufferSize / 4)
	wantMin, wantMax := scalarBoundsFloat32(values)
	gotMin, gotMax := boundsFloat32(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsFloat32() = (%g, %g), want (%g, %g)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsFloat32MinSink, benchmarkBoundsFloat32MaxSink = boundsFloat32(values)
	}
	b.StopTimer()

	if benchmarkBoundsFloat32MinSink != wantMin || benchmarkBoundsFloat32MaxSink != wantMax {
		b.Fatalf("boundsFloat32() = (%g, %g), want (%g, %g)", benchmarkBoundsFloat32MinSink, benchmarkBoundsFloat32MaxSink, wantMin, wantMax)
	}
}

func BenchmarkBoundsFloat64DefaultPageSize(b *testing.B) {
	values := makeBenchmarkBoundsFloat64Values(DefaultPageBufferSize / 8)
	wantMin, wantMax := scalarBoundsFloat64(values)
	gotMin, gotMax := boundsFloat64(values)
	if gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("boundsFloat64() = (%g, %g), want (%g, %g)", gotMin, gotMax, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		benchmarkBoundsFloat64MinSink, benchmarkBoundsFloat64MaxSink = boundsFloat64(values)
	}
	b.StopTimer()

	if benchmarkBoundsFloat64MinSink != wantMin || benchmarkBoundsFloat64MaxSink != wantMax {
		b.Fatalf("boundsFloat64() = (%g, %g), want (%g, %g)", benchmarkBoundsFloat64MinSink, benchmarkBoundsFloat64MaxSink, wantMin, wantMax)
	}
}

func makeBenchmarkBoundsInt32Values(size int) []int32 {
	values := make([]int32, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = int32(state)
	}
	values[len(values)/3] = math.MinInt32
	values[2*len(values)/3] = math.MaxInt32
	return values
}

func makeBenchmarkBoundsInt64Values(size int) []int64 {
	values := make([]int64, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = int64(state)
	}
	values[len(values)/3] = math.MinInt64
	values[2*len(values)/3] = math.MaxInt64
	return values
}

func makeBenchmarkBoundsUint32Values(size int) []uint32 {
	values := make([]uint32, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = uint32(state)
	}
	values[len(values)/3] = 0
	values[2*len(values)/3] = math.MaxUint32
	return values
}

func makeBenchmarkBoundsUint64Values(size int) []uint64 {
	values := make([]uint64, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = state
	}
	values[len(values)/3] = 0
	values[2*len(values)/3] = math.MaxUint64
	return values
}

func makeBenchmarkBoundsFloat32Values(size int) []float32 {
	values := make([]float32, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = float32(int32(state))
	}
	values[len(values)/3] = -math.MaxFloat32
	values[2*len(values)/3] = math.MaxFloat32
	return values
}

func makeBenchmarkBoundsFloat64Values(size int) []float64 {
	values := make([]float64, size)
	state := uint64(size)
	for i := range values {
		state = nextBenchmarkBoundsState(state)
		values[i] = float64(int64(state))
	}
	values[len(values)/3] = -math.MaxFloat64
	values[2*len(values)/3] = math.MaxFloat64
	return values
}

func nextBenchmarkBoundsState(state uint64) uint64 {
	state ^= state << 13
	state ^= state >> 7
	state ^= state << 17
	return state
}

func scalarBoundsInt32(values []int32) (min, max int32) {
	min = values[0]
	max = values[0]
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

func scalarBoundsInt64(values []int64) (min, max int64) {
	min = values[0]
	max = values[0]
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

func scalarBoundsUint32(values []uint32) (min, max uint32) {
	min = values[0]
	max = values[0]
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

func scalarBoundsUint64(values []uint64) (min, max uint64) {
	min = values[0]
	max = values[0]
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

func scalarBoundsFloat32(values []float32) (min, max float32) {
	min = values[0]
	max = values[0]
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

func scalarBoundsFloat64(values []float64) (min, max float64) {
	min = values[0]
	max = values[0]
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
