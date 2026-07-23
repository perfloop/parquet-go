package parquet

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/internal/memory"
)

const bitmapPoolBenchmarkRows = 4_096

type bitmapPoolBenchmarkRecord struct {
	Value int32 `parquet:",optional"`
}

type bitmapPoolReadRecord struct {
	Value *int32 `parquet:",optional"`
}

func bitmapPoolBenchmarkRecords() []bitmapPoolBenchmarkRecord {
	rows := make([]bitmapPoolBenchmarkRecord, bitmapPoolBenchmarkRows)
	for i := range rows {
		if i%3 != 0 {
			rows[i].Value = int32(i + 1)
		}
	}
	return rows
}

func TestGenericWriterOptionalBitmapNulls(t *testing.T) {
	rows := bitmapPoolBenchmarkRecords()
	var output bytes.Buffer
	writer := NewGenericWriter[bitmapPoolBenchmarkRecord](
		&output,
		MaxRowsPerRowGroup(int64(len(rows))),
	)

	if n, err := writer.Write(rows); err != nil {
		t.Fatalf("writing rows: %v", err)
	} else if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing writer: %v", err)
	}

	got, err := Read[bitmapPoolReadRecord](bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatalf("reading rows: %v", err)
	}
	if len(got) != len(rows) {
		t.Fatalf("read %d rows, want %d", len(got), len(rows))
	}
	for i := range rows {
		if rows[i].Value == 0 {
			if got[i].Value != nil {
				t.Fatalf("row %d: got %d, want null", i, *got[i].Value)
			}
		} else if got[i].Value == nil || *got[i].Value != rows[i].Value {
			t.Fatalf("row %d: got %v, want %d", i, got[i].Value, rows[i].Value)
		}
	}
}

func BenchmarkGenericWriterOptionalBitmapPoolMiss4096(b *testing.B) {
	rows := bitmapPoolBenchmarkRecords()
	writer := NewGenericWriter[bitmapPoolBenchmarkRecord](
		io.Discard,
		WriteBufferSize(0),
		MaxRowsPerRowGroup(int64(len(rows))),
	)

	// Allocate the writer's column buffers before timing so each iteration's
	// forced pool miss is the allocation shape under test.
	if n, err := writer.Write(rows); err != nil {
		b.Fatalf("warming writer: %v", err)
	} else if n != len(rows) {
		b.Fatalf("warming writer wrote %d rows, want %d", n, len(rows))
	}
	writer.Reset(io.Discard)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		// GenericWriter splits this batch into 64-row writes. Replacing the
		// pool forces the first optional-field bitmap acquisition to miss.
		bitmapPool = memory.Pool[bitmap]{}

		if n, err := writer.Write(rows); err != nil {
			b.Fatalf("writing rows: %v", err)
		} else if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		writer.Reset(io.Discard)
	}
}
