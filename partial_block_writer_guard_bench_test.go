package parquet_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkWriterInt64PartialBlockStorage(b *testing.B) {
	rows := partialBlockWriterRows(1_000_000_000_000 + int64(b.N&1))
	schema := parquet.NewSchema("partial_block", parquet.Group{
		"value": parquet.Required(parquet.Leaf(parquet.Int64Type)),
	})
	var output bytes.Buffer
	var outputSize int

	for b.Loop() {
		output.Reset()
		writer := parquet.NewWriter(
			&output,
			schema,
			parquet.DefaultEncodingFor(parquet.Int64, &parquet.DeltaBinaryPacked),
			parquet.MaxRowsPerRowGroup(int64(len(rows))),
			parquet.WriteBufferSize(0),
		)
		n, err := writer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		outputSize = output.Len()
	}

	file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(outputSize))
	if err != nil {
		b.Fatal(err)
	}
	if got, want := file.NumRows(), int64(len(rows)); got != want {
		b.Fatalf("file row count mismatch: want=%d got=%d", want, got)
	}
	column, ok := file.RowGroups()[0].ColumnChunks()[0].(*parquet.FileColumnChunk)
	if !ok {
		b.Fatal("file column has an unexpected type")
	}
	if encoding := column.Node().Encoding(); encoding == nil || encoding.Encoding() != parquet.DeltaBinaryPacked.Encoding() {
		b.Fatalf("data column encoding = %v, want %v", encoding, parquet.DeltaBinaryPacked.Encoding())
	}

	readRows := make([]parquet.Row, len(rows))
	reader := file.RowGroups()[0].Rows()
	defer reader.Close()
	n, err := reader.ReadRows(readRows)
	if err != nil && err != io.EOF {
		b.Fatal(err)
	}
	if n != len(rows) {
		b.Fatalf("read %d rows, want %d", n, len(rows))
	}
	for i := range rows {
		if got, want := readRows[i][0].Int64(), rows[i][0].Int64(); got != want {
			b.Fatalf("row %d value = %d, want %d", i, got, want)
		}
	}
	b.ReportMetric(float64(outputSize), "serialized_bytes/op")
}

func partialBlockWriterRows(firstValue int64) []parquet.Row {
	rows := make([]parquet.Row, 6)
	for i := range rows {
		rows[i] = parquet.Row{parquet.Int64Value(firstValue+int64(i)).Level(0, 0, 0)}
	}
	return rows
}
