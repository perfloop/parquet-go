package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestBufferWriteRowsUsesCurrentColumnBuffers(t *testing.T) {
	schema := parquet.NewSchema("buffer", parquet.Group{
		"first":  parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
		"second": parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
	})
	buffer := parquet.NewBuffer(schema)
	columns := buffer.ColumnBuffers()
	replacement := columns[0].Clone()
	columns[0] = replacement

	row := parquet.Row{
		parquet.ValueOf([]byte("first")).Level(0, 0, 0),
		parquet.ValueOf([]byte("second")).Level(0, 0, 1),
	}
	if n, err := buffer.WriteRows([]parquet.Row{row}); err != nil {
		t.Fatal(err)
	} else if n != 1 {
		t.Fatalf("WriteRows wrote %d rows, want 1", n)
	}
	if got := replacement.Len(); got != 1 {
		t.Fatalf("replacement column has %d values, want 1", got)
	}
	if got := buffer.NumRows(); got != 1 {
		t.Fatalf("buffer has %d rows, want 1", got)
	}
}
