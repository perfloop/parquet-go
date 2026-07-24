package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

const (
	bufferWriteRowsLargePayloadSize = 64 * 1024
	bufferWriteRowsSmallPayloadSize = 1
)

func makeBufferWriteRowsLargeFirstFallbackRows() []parquet.Row {
	rows := make([]parquet.Row, bufferWriteRowsBatchSize)
	for i := range rows {
		size := bufferWriteRowsSmallPayloadSize
		if i == 0 {
			size = bufferWriteRowsLargePayloadSize
		}
		first := make([]byte, size)
		second := make([]byte, size)
		for j := range first {
			first[j] = byte(i + j)
			second[j] = byte(i*3 + j)
		}
		rows[i] = parquet.Row{
			parquet.ValueOf(first).Level(0, 0, 0),
			parquet.ValueOf(second).Level(0, 0, 1),
		}
	}
	second := rows[1]
	rows[1] = parquet.Row{second[1], second[0]}
	return rows
}

func BenchmarkBufferWriteRowsLargeFirstFallbackByteArrays(b *testing.B) {
	rows := makeBufferWriteRowsLargeFirstFallbackRows()

	b.ReportAllocs()
	for b.Loop() {
		buffer := parquet.NewBuffer(bufferWriteRowsByteArraySchema)
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}
		if got := buffer.NumRows(); got != int64(len(rows)) {
			b.Fatalf("buffer holds %d rows, want %d", got, len(rows))
		}
	}
}
