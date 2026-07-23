package variant

import (
	"strings"
	"testing"
)

type unmarshalFlatObjectBenchmarkCase struct {
	metadata []byte
	value    []byte
	fields   int
}

// BenchmarkUnmarshalFlatObject guards the direct decoder's large-object path
// separately from the nested object-and-array workload.
func BenchmarkUnmarshalFlatObject(b *testing.B) {
	sizes := [...]int{128, 192, 256, 320}
	cases := make([]unmarshalFlatObjectBenchmarkCase, len(sizes))
	for i, fields := range sizes {
		metadata, value, err := Marshal(benchmarkLargeObject(fields))
		if err != nil {
			b.Fatalf("Marshal(%d fields): %v", fields, err)
		}
		cases[i] = unmarshalFlatObjectBenchmarkCase{
			metadata: metadata,
			value:    value,
			fields:   fields,
		}
	}

	b.ReportAllocs()
	totalFields := 0
	for i := 0; b.Loop(); i++ {
		c := &cases[i&3]
		got, err := Unmarshal(c.metadata, c.value)
		if err != nil {
			b.Fatal(err)
		}
		object, ok := got.(map[string]any)
		if !ok {
			b.Fatalf("Unmarshal type = %T, want map[string]any", got)
		}
		if len(object) != c.fields {
			b.Fatalf("object length = %d, want %d", len(object), c.fields)
		}
		if _, ok := object["k000"]; !ok {
			b.Fatal("Unmarshal result is missing k000")
		}
		totalFields += len(object)
	}
	if totalFields == 0 {
		b.Fatal("Unmarshal results were not consumed")
	}
}

// TestUnmarshalMalformedStructure exercises the direct object and array
// parsers with attacker-controlled counts and offset tables through the public
// Unmarshal API.
func TestUnmarshalMalformedStructure(t *testing.T) {
	var empty MetadataBuilder
	_, emptyMetadata := empty.Build()
	var names MetadataBuilder
	names.Add("a")
	_, objectMetadata := names.Build()

	for _, test := range []struct {
		name     string
		metadata []byte
		value    []byte
		wantErr  string
	}{
		{
			name:     "object element count exceeds input",
			metadata: objectMetadata,
			value:    []byte{0x42, 0xFF, 0xFF, 0xFF, 0x7F},
			wantErr:  "object element count",
		},
		{
			name:     "array element count exceeds input",
			metadata: emptyMetadata,
			value:    []byte{0x13, 0xFF, 0xFF, 0xFF, 0x7F},
			wantErr:  "array element count",
		},
		{
			// Header, count, field ID, and only the first of two offsets.
			name:     "truncated object offset table",
			metadata: objectMetadata,
			value:    []byte{0x02, 0x01, 0x00, 0x00},
			wantErr:  "reading object offset 1",
		},
		{
			// The field starts after the declared zero-length value region.
			name:     "object invalid value offset",
			metadata: objectMetadata,
			value:    []byte{0x02, 0x01, 0x00, 0x01, 0x00},
			wantErr:  "invalid value offset",
		},
		{
			// The first element offset is greater than its end offset.
			name:     "array invalid element offset",
			metadata: emptyMetadata,
			value:    []byte{0x03, 0x01, 0x01, 0x00},
			wantErr:  "invalid offset",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Unmarshal(test.metadata, test.value)
			if err == nil {
				t.Fatalf("Unmarshal succeeded, want error containing %q", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("Unmarshal error = %q, want substring %q", err, test.wantErr)
			}
		})
	}
}
