package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

var bufferWriteRowsMixedSchema = parquet.NewSchema("buffer_write_rows_mixed", parquet.Group{
	"first":  parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
	"second": parquet.Required(parquet.Leaf(parquet.Int32Type)),
})

var bufferColumnChunksSchema = parquet.NewSchema("buffer_column_chunks", parquet.Group{
	"a": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"b": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"c": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"d": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"e": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"f": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"g": parquet.Required(parquet.Leaf(parquet.Int32Type)),
	"h": parquet.Required(parquet.Leaf(parquet.Int32Type)),
})

func BenchmarkBufferWriteRowsExposedByteArrays(b *testing.B) {
	rows, _ := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
	buffer := parquet.NewBuffer(bufferWriteRowsByteArraySchema)
	buffer.ColumnBuffers()
	batchesSinceReset := 0

	b.ReportAllocs()
	for b.Loop() {
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}

		batchesSinceReset++
		if batchesSinceReset == bufferWriteRowsResetBatches {
			buffer.Reset()
			batchesSinceReset = 0
		}
	}

	if got, want := buffer.NumRows(), int64(batchesSinceReset*len(rows)); got != want {
		b.Fatalf("buffer holds %d rows, want %d", got, want)
	}
}

type bufferWriteRowsGenericByteArrayRow struct {
	First  []byte
	Second []byte
}

func makeBufferWriteRowsGenericByteArrayRows(n int) []bufferWriteRowsGenericByteArrayRow {
	rows := make([]bufferWriteRowsGenericByteArrayRow, n)
	for i := range rows {
		rows[i].First = make([]byte, bufferWriteRowsPayloadSize)
		rows[i].Second = make([]byte, bufferWriteRowsPayloadSize)
		for j := range rows[i].First {
			rows[i].First[j] = byte(i + j)
			rows[i].Second[j] = byte(i*3 + j)
		}
	}
	return rows
}

func BenchmarkGenericBufferWriteExposedByteArrays(b *testing.B) {
	rows := makeBufferWriteRowsGenericByteArrayRows(bufferWriteRowsBatchSize)
	buffer := parquet.NewGenericBuffer[bufferWriteRowsGenericByteArrayRow]()
	buffer.ColumnBuffers()
	batchesSinceReset := 0

	b.ReportAllocs()
	for b.Loop() {
		n, err := buffer.Write(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("Write wrote %d rows, want %d", n, len(rows))
		}

		batchesSinceReset++
		if batchesSinceReset == bufferWriteRowsResetBatches {
			buffer.Reset()
			batchesSinceReset = 0
		}
	}

	if got, want := buffer.NumRows(), int64(batchesSinceReset*len(rows)); got != want {
		b.Fatalf("buffer holds %d rows, want %d", got, want)
	}
}

func BenchmarkBufferColumnChunksExposedEightColumns(b *testing.B) {
	buffer := parquet.NewBuffer(bufferColumnChunksSchema)
	buffer.ColumnBuffers()
	var chunks []parquet.ColumnChunk

	b.ReportAllocs()
	for b.Loop() {
		chunks = buffer.ColumnChunks()
	}

	if got, want := len(chunks), 8; got != want {
		b.Fatalf("ColumnChunks returned %d columns, want %d", got, want)
	}
}

func makeBufferWriteRowsMixedRows(n int) []parquet.Row {
	payloads := make([]byte, n*bufferWriteRowsPayloadSize)
	rows := make([]parquet.Row, n)
	for i := range rows {
		start := i * bufferWriteRowsPayloadSize
		payload := payloads[start : start+bufferWriteRowsPayloadSize]
		for j := range payload {
			payload[j] = byte(i + j)
		}
		rows[i] = parquet.Row{
			parquet.ValueOf(payload).Level(0, 0, 0),
			parquet.ValueOf(int32(i)).Level(0, 0, 1),
		}
	}
	return rows
}

func BenchmarkBufferWriteRowsMixedByteArrayInt32(b *testing.B) {
	rows := makeBufferWriteRowsMixedRows(bufferWriteRowsBatchSize)
	buffer := parquet.NewBuffer(bufferWriteRowsMixedSchema)
	batchesSinceReset := 0

	b.ReportAllocs()
	for b.Loop() {
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}

		batchesSinceReset++
		if batchesSinceReset == bufferWriteRowsResetBatches {
			buffer.Reset()
			batchesSinceReset = 0
		}
	}

	if got, want := buffer.NumRows(), int64(batchesSinceReset*len(rows)); got != want {
		b.Fatalf("buffer holds %d rows, want %d", got, want)
	}
}
