package parquet_test

import (
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// BenchmarkBufferWriteRowsFromRowBufferFinalNoncanonicalByteArrays exercises
// Buffer.WriteRows through RowBuffer's public raw-row transfer path. RowBuffer
// preserves the supplied Row value sequence, and CopyRows uses RowWriterTo to
// pass that batch to the destination. The setup verifies that the last row
// remains reordered before the timed transfers begin.
func BenchmarkBufferWriteRowsFromRowBufferFinalNoncanonicalByteArrays(b *testing.B) {
	const (
		rowsPerBatch = 1024
		payloadBytes = 64
		batches      = 8
	)

	schema := parquet.SchemaOf(bufferWriteRowsByteArrayRecord{})
	rows := make([]parquet.Row, rowsPerBatch)
	for i := range rows {
		rows[i] = schema.Deconstruct(nil, makeBufferWriteRowsByteArrayRecord(i, payloadBytes))
	}
	rows[len(rows)-1][0], rows[len(rows)-1][1] = rows[len(rows)-1][1], rows[len(rows)-1][0]

	source := parquet.NewRowBuffer[any](&parquet.RowGroupConfig{Schema: schema})
	if n, err := source.WriteRows(rows); err != nil || n != len(rows) {
		b.Fatalf("source.WriteRows() = %d, %v; want %d, nil", n, err, len(rows))
	}

	check := source.Rows()
	if err := check.SeekToRow(rowsPerBatch - 1); err != nil {
		b.Fatal(err)
	}
	last := make([]parquet.Row, 1)
	n, err := check.ReadRows(last)
	if err != nil && err != io.EOF {
		b.Fatal(err)
	}
	if n != 1 || last[0][0].Column() != 1 || last[0][1].Column() != 0 {
		b.Fatalf("RowBuffer changed final raw row order: %#v", last)
	}
	if err := check.Close(); err != nil {
		b.Fatal(err)
	}

	buffer := parquet.NewBuffer(schema)
	b.SetBytes(int64(rowsPerBatch * payloadBytes * 8))
	b.ResetTimer()

	batchesWritten := 0
	for b.Loop() {
		sourceRows := source.Rows()
		n, err := parquet.CopyRows(buffer, sourceRows)
		if closeErr := sourceRows.Close(); closeErr != nil {
			b.Fatal(closeErr)
		}
		if err != nil {
			b.Fatal(err)
		}
		if n != rowsPerBatch {
			b.Fatalf("CopyRows() = %d; want %d", n, rowsPerBatch)
		}

		batchesWritten++
		if batchesWritten == batches {
			if n := buffer.NumRows(); n != int64(rowsPerBatch*batches) {
				b.Fatalf("NumRows() = %d; want %d", n, rowsPerBatch*batches)
			}
			buffer.Reset()
			batchesWritten = 0
		}
	}
}
