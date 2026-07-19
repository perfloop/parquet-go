package parquet

import (
	"math/rand"
	"testing"
)

// ColumnWriter flushes pages at 98% of DefaultPageBufferSize. Rounding that
// boundary up to an INT64 value gives the default-size page this benchmark
// models.
const defaultInt64PageValues = (DefaultPageBufferSize*98/100 + 8 - 1) / 8

func int64PageBoundsValues(n int) (values []int64, min, max int64) {
	values = make([]int64, n)
	prng := rand.New(rand.NewSource(1))

	for i := range values {
		values[i] = int64(prng.Uint64())
	}

	values[n/3] = -1 << 63
	values[2*n/3] = 1<<63 - 1
	min, max = values[0], values[0]

	for _, value := range values[1:] {
		if value < min {
			min = value
		}
		if value > max {
			max = value
		}
	}
	return values, min, max
}

func TestBoundsInt64DefaultPageSize(t *testing.T) {
	for _, test := range []struct {
		name string
		n    int
	}{
		{name: "below-default", n: defaultInt64PageValues - 1},
		{name: "default-page", n: defaultInt64PageValues},
	} {
		t.Run(test.name, func(t *testing.T) {
			values, wantMin, wantMax := int64PageBoundsValues(test.n)
			gotMin, gotMax := boundsInt64(values)

			if gotMin != wantMin || gotMax != wantMax {
				t.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
			}
		})
	}
}
