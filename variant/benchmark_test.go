package variant

import (
	"testing"
)

func BenchmarkMarshalInt64Slice(b *testing.B) {
	slice := make([]int64, 1000)
	for i := range slice {
		slice[i] = int64(i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		meta, val, err := Marshal(slice)
		if err != nil {
			b.Fatal(err)
		}
		_ = meta
		_ = val
	}
}
