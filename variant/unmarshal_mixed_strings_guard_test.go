package variant

import "testing"

type unmarshalMixedStringsBenchmarkCase struct {
	metadata []byte
	value    []byte
	elements int
	last     string
}

// BenchmarkUnmarshalMixedStrings uses the package's existing representative
// mixed-slice shape. Its 80 distinct string values exceed the decoder's small
// reuse cache, exercising string-cache misses across multi-element inputs.
func BenchmarkUnmarshalMixedStrings(b *testing.B) {
	sizes := [...]int{256, 512, 1024, 2048}
	cases := make([]unmarshalMixedStringsBenchmarkCase, len(sizes))
	for i, elements := range sizes {
		input := benchmarkMixedSlice(elements)
		metadata, value, err := Marshal(input)
		if err != nil {
			b.Fatalf("Marshal(%d elements): %v", elements, err)
		}
		last := input[len(input)-1].(map[string]any)["s"].(string)
		cases[i] = unmarshalMixedStringsBenchmarkCase{
			metadata: metadata,
			value:    value,
			elements: elements,
			last:     last,
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
		last, ok := values[len(values)-1].(map[string]any)
		if !ok {
			b.Fatalf("last element type = %T, want map[string]any", values[len(values)-1])
		}
		if text, ok := last["s"].(string); !ok || text != c.last {
			b.Fatalf("last string = %v (%T), want %q", last["s"], last["s"], c.last)
		}
		totalElements += len(values)
	}
	if totalElements == 0 {
		b.Fatal("Unmarshal results were not consumed")
	}
}
