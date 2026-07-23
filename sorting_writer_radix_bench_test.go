package parquet_test

import (
	"bytes"
	"math/rand"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// sortingWriterFixedWidthRow is deliberately limited to the fixed-width,
// required-key shape considered by the radix-run-store experiment.
type sortingWriterFixedWidthRow struct {
	Key     int64 `parquet:"key"`
	Payload int64 `parquet:"payload"`
}

func makeSortingWriterFixedWidthRows(count, cardinality int) []sortingWriterFixedWidthRow {
	rows := make([]sortingWriterFixedWidthRow, count)
	for i := range rows {
		key := int64(i % cardinality)
		rows[i] = sortingWriterFixedWidthRow{
			Key:     key,
			Payload: ^key,
		}
	}

	// The deterministic runtime shuffle makes every bounded run overlap while
	// keeping baseline and candidate inputs identical.
	rand.New(rand.NewSource(1)).Shuffle(len(rows), func(i, j int) {
		rows[i], rows[j] = rows[j], rows[i]
	})
	return rows
}

func TestSortingWriterOverlappingFixedWidthRuns(t *testing.T) {
	const (
		rowCount           = 4096
		sortRowCount int64 = 127
		maxRows      int64 = 512
	)

	tests := []struct {
		name        string
		pool        func(testing.TB) parquet.BufferPool
		cardinality int
		deduplicate bool
	}{
		{
			name: "memory/unique",
			pool: func(testing.TB) parquet.BufferPool {
				return parquet.NewBufferPool()
			},
			cardinality: rowCount,
		},
		{
			name: "file/unique",
			pool: func(t testing.TB) parquet.BufferPool {
				return parquet.NewFileBufferPool(t.TempDir(), "sorting-run-*")
			},
			cardinality: rowCount,
		},
		{
			name: "memory/deduplicate-across-runs",
			pool: func(testing.TB) parquet.BufferPool {
				return parquet.NewBufferPool()
			},
			cardinality: rowCount / 2,
			deduplicate: true,
		},
		{
			name: "file/deduplicate-across-runs",
			pool: func(t testing.TB) parquet.BufferPool {
				return parquet.NewFileBufferPool(t.TempDir(), "sorting-run-*")
			},
			cardinality: rowCount / 2,
			deduplicate: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rows := makeSortingWriterFixedWidthRows(rowCount, test.cardinality)
			var output bytes.Buffer
			writer := parquet.NewSortingWriter[sortingWriterFixedWidthRow](
				&output,
				sortRowCount,
				parquet.MaxRowsPerRowGroup(maxRows),
				parquet.SortingWriterConfig(
					parquet.SortingBuffers(test.pool(t)),
					parquet.SortingColumns(parquet.Ascending("key")),
					parquet.DropDuplicatedRows(test.deduplicate),
				),
			)

			if n, err := writer.Write(rows); err != nil {
				t.Fatal(err)
			} else if n != len(rows) {
				t.Fatalf("wrote %d rows, want %d", n, len(rows))
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			for i, rowGroup := range file.RowGroups() {
				if rowGroup.NumRows() > maxRows {
					t.Fatalf("row group %d has %d rows, exceeds limit %d", i, rowGroup.NumRows(), maxRows)
				}
			}

			got, err := parquet.Read[sortingWriterFixedWidthRow](bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != test.cardinality {
				t.Fatalf("read %d rows, want %d", len(got), test.cardinality)
			}
			for i, row := range got {
				if row.Key != int64(i) {
					t.Fatalf("row %d has key %d, want %d", i, row.Key, i)
				}
				if row.Payload != ^row.Key {
					t.Fatalf("row %d has payload %d for key %d", i, row.Payload, row.Key)
				}
			}
		})
	}
}

func BenchmarkSortingWriterOverlappingInt64Runs(b *testing.B) {
	const (
		rowCount           = 65536
		sortRowCount int64 = 256
	)
	rows := makeSortingWriterFixedWidthRows(rowCount, rowCount)

	b.ReportAllocs()
	for b.Loop() {
		var output bytes.Buffer
		writer := parquet.NewSortingWriter[sortingWriterFixedWidthRow](
			&output,
			sortRowCount,
			parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending("key")),
			),
		)
		if n, err := writer.Write(rows); err != nil {
			b.Fatal(err)
		} else if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if output.Len() == 0 {
			b.Fatal("sorting writer produced no parquet output")
		}
	}
}
