package parquet

import (
	"io"
	"testing"
)

func BenchmarkCopyRowsRowBufferToWriterDistinctSchema(b *testing.B) {
	sourceSchema := copyRowsDistinctWideSchema()
	targetSchema := copyRowsDistinctWideSchema()
	if sourceSchema == targetSchema {
		b.Fatal("source and target schemas must have distinct pointers")
	}
	if !EqualNodes(sourceSchema, targetSchema) {
		b.Fatal("source and target schemas must be structurally equal")
	}

	input := makeWideRows(1)
	buffer := NewRowBuffer[wideRow](sourceSchema)
	if _, err := buffer.Write(input); err != nil {
		b.Fatal(err)
	}
	writer := NewGenericWriter[wideRow](io.Discard, targetSchema)
	if buffer.Schema() == writer.Schema() {
		b.Fatal("row buffer and writer must retain distinct schema pointers")
	}
	b.Cleanup(func() {
		if err := writer.Close(); err != nil {
			b.Error(err)
		}
	})

	for b.Loop() {
		b.StopTimer()
		writer.Reset(io.Discard)
		reader := buffer.Rows()
		if _, ok := reader.(RowWriterTo); !ok {
			b.Fatal("row buffer reader must retain its direct-copy path")
		}
		b.StartTimer()
		n, err := CopyRows(writer, reader)
		b.StopTimer()
		closeErr := reader.Close()
		if err != nil {
			b.Fatal(err)
		}
		if closeErr != nil {
			b.Fatal(closeErr)
		}
		if n != int64(len(input)) {
			b.Fatalf("copied %d rows, want %d", n, len(input))
		}
		b.StartTimer()
	}
}

func copyRowsDistinctWideSchema() *Schema {
	cached := SchemaOf(wideRow{})
	fields := make(Group, len(cached.Fields()))
	for _, field := range cached.Fields() {
		fields[field.Name()] = field
	}
	return NewSchema(cached.Name(), fields)
}
