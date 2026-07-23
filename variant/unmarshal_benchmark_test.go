package variant

import (
	"strings"
	"testing"
)

type unmarshalBenchmarkCase struct {
	metadata []byte
	value    []byte
	records  int
}

// BenchmarkUnmarshalNested measures the public Unmarshal path on nested
// objects containing arrays of objects and primitive arrays. Four payload
// sizes make the timed input selection runtime-varying while keeping setup
// outside the measured loop.
func BenchmarkUnmarshalNested(b *testing.B) {
	sizes := [...]int{64, 96, 128, 160}
	cases := make([]unmarshalBenchmarkCase, len(sizes))
	for i, records := range sizes {
		metadata, value, err := Marshal(benchmarkUnmarshalInput(records))
		if err != nil {
			b.Fatalf("Marshal(%d records): %v", records, err)
		}
		cases[i] = unmarshalBenchmarkCase{
			metadata: metadata,
			value:    value,
			records:  records,
		}
	}

	b.ReportAllocs()
	totalRecords := 0
	for i := 0; b.Loop(); i++ {
		c := &cases[i&3]
		got, err := Unmarshal(c.metadata, c.value)
		if err != nil {
			b.Fatal(err)
		}
		root, ok := got.(map[string]any)
		if !ok {
			b.Fatalf("Unmarshal type = %T, want map[string]any", got)
		}
		records, ok := root["records"].([]any)
		if !ok || len(records) != c.records {
			b.Fatalf("records = %T with length %d, want []any with length %d", root["records"], len(records), c.records)
		}
		ids, ok := root["ids"].([]any)
		if !ok || len(ids) != c.records*4 {
			b.Fatalf("ids = %T with length %d, want []any with length %d", root["ids"], len(ids), c.records*4)
		}
		totalRecords += len(records)
	}
	if totalRecords == 0 {
		b.Fatal("Unmarshal results were not consumed")
	}
}

func benchmarkUnmarshalInput(records int) map[string]any {
	items := make([]any, records)
	ids := make([]int64, records*4)
	for i := range ids {
		ids[i] = int64(i*3 - records)
	}
	for i := range items {
		items[i] = map[string]any{
			"id":     int64(i),
			"active": i%2 == 0,
			"tags":   []any{"parquet", "variant", int64(i % 7)},
			"details": map[string]any{
				"name":  strings.Repeat("x", 32+i%16),
				"score": int32(i * 17),
			},
		}
	}
	return map[string]any{
		"records": items,
		"ids":     ids,
		"summary": map[string]any{
			"source":  "benchmark",
			"version": int16(1),
		},
	}
}
