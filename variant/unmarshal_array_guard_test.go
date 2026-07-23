package variant

import "testing"

type unmarshalArrayBenchmarkCase struct {
	metadata []byte
	value    []byte
	elements int
}

// BenchmarkUnmarshalArray guards the standalone multi-element array path,
// where offset-table traversal is a larger fraction of public Unmarshal work.
func BenchmarkUnmarshalArray(b *testing.B) {
	sizes := [...]int{256, 512, 1024, 2048}
	cases := make([]unmarshalArrayBenchmarkCase, len(sizes))
	for i, elements := range sizes {
		metadata, value, err := Marshal(benchmarkInt64Slice(elements))
		if err != nil {
			b.Fatalf("Marshal(%d elements): %v", elements, err)
		}
		cases[i] = unmarshalArrayBenchmarkCase{
			metadata: metadata,
			value:    value,
			elements: elements,
		}
	}

	b.ReportAllocs()
	totalElements := 0
	for i := 0; b.Loop(); i++ {
		c := &cases[i&3]
		got, err := Unmarshal(c.metadata, c.value)
		if err != nil {
			b.Fatal(err)
		}
		values, ok := got.([]any)
		if !ok {
			b.Fatalf("Unmarshal type = %T, want []any", got)
		}
		if len(values) != c.elements {
			b.Fatalf("array length = %d, want %d", len(values), c.elements)
		}
		if first, ok := values[0].(int64); !ok || first != 0 {
			b.Fatalf("first value = %v (%T), want int64(0)", values[0], values[0])
		}
		if last, ok := values[len(values)-1].(int64); !ok || last != int64(c.elements-1) {
			b.Fatalf("last value = %v (%T), want int64(%d)", values[len(values)-1], values[len(values)-1], c.elements-1)
		}
		totalElements += len(values)
	}
	if totalElements == 0 {
		b.Fatal("Unmarshal results were not consumed")
	}
}
