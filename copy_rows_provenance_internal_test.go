package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// copyRowsProvenanceRow is deliberately wide enough to exercise the complete
// file-row-group copy path: fixed-width values, a dictionary-eligible string,
// a non-dictionary string, and several independent columns.
type copyRowsProvenanceRow struct {
	ID    int64   `parquet:"id"`
	Name  string  `parquet:"name,dict"`
	Email string  `parquet:"email"`
	Score float64 `parquet:"score"`
	Flag  bool    `parquet:"flag"`
	Count int32   `parquet:"count"`
}

func makeCopyRowsProvenanceRows(n int) []copyRowsProvenanceRow {
	rows := make([]copyRowsProvenanceRow, n)
	for i := range rows {
		rows[i] = copyRowsProvenanceRow{
			ID:    int64(i),
			Name:  fmt.Sprintf("category-%d", i%32),
			Email: fmt.Sprintf("user%d@example.com", i),
			Score: float64(i) * 0.125,
			Flag:  i%2 == 0,
			Count: int32(i % 1000),
		}
	}
	return rows
}

func writeCopyRowsProvenanceFile[T any](t testing.TB, rows []T, options ...WriterOption) *File {
	t.Helper()

	var output bytes.Buffer
	writer := NewGenericWriter[T](&output, options...)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}

	file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return file
}

func readCopyRowsProvenanceFile[T any](t testing.TB, input []byte, want int) []T {
	t.Helper()

	reader := NewGenericReader[T](bytes.NewReader(input))
	defer reader.Close()

	rows := make([]T, 0, want)
	buffer := make([]T, min(64, want))
	for len(rows) < want {
		n, err := reader.Read(buffer)
		rows = append(rows, buffer[:n]...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading copied rows: %v", err)
		}
		if n == 0 {
			t.Fatal("reader made no progress")
		}
	}
	return rows
}

func copyRowsProvenance(t testing.TB, dst RowWriter, rows Rows) int64 {
	t.Helper()

	n, err := CopyRows(dst, rows)
	closeErr := rows.Close()
	if err != nil {
		t.Fatalf("CopyRows: %v", err)
	}
	if closeErr != nil {
		t.Fatalf("closing source rows: %v", closeErr)
	}
	return n
}

// TestCopyRowsFileRowGroup checks that CopyRows retains its public behavior for
// a pristine file row-group reader, an already-consumed reader, schema
// conversion, and a destination row-group limit. The matching case also
// compares CopyRows with the Writer.WriteRowGroup handoff that the optimized
// implementation will use.
func TestCopyRowsFileRowGroup(t *testing.T) {
	t.Run("matching file row groups match WriteRowGroup", func(t *testing.T) {
		input := makeCopyRowsProvenanceRows(1_000)
		source := writeCopyRowsProvenanceFile(t, input,
			Compression(&Snappy),
			MaxRowsPerRowGroup(250),
		)

		write := func(copyRows bool) []byte {
			var output bytes.Buffer
			writer := NewGenericWriter[copyRowsProvenanceRow](&output,
				Compression(&Snappy),
				MaxRowsPerRowGroup(250),
			)

			var written int64
			for _, rowGroup := range source.RowGroups() {
				if copyRows {
					written += copyRowsProvenance(t, writer, rowGroup.Rows())
				} else {
					n, err := writer.WriteRowGroup(rowGroup)
					if err != nil {
						t.Fatalf("WriteRowGroup: %v", err)
					}
					written += n
				}
			}
			if written != int64(len(input)) {
				t.Fatalf("written rows = %d, want %d", written, len(input))
			}
			if err := writer.Close(); err != nil {
				t.Fatalf("closing destination writer: %v", err)
			}
			return bytes.Clone(output.Bytes())
		}

		before := copyPathCounter.Load()
		copied := write(true)
		copiedChunks := copyPathCounter.Load() - before
		direct := write(false)
		t.Logf("CopyRows file-row-group copy-path chunks=%d", copiedChunks)

		gotCopied := readCopyRowsProvenanceFile[copyRowsProvenanceRow](t, copied, len(input))
		gotDirect := readCopyRowsProvenanceFile[copyRowsProvenanceRow](t, direct, len(input))
		if !reflect.DeepEqual(gotCopied, input) {
			t.Fatal("CopyRows output differs from source rows")
		}
		if !reflect.DeepEqual(gotDirect, input) {
			t.Fatal("WriteRowGroup output differs from source rows")
		}
		if !reflect.DeepEqual(gotCopied, gotDirect) {
			t.Fatal("CopyRows and WriteRowGroup produced different rows")
		}
	})

	t.Run("partially consumed reader copies only remaining rows", func(t *testing.T) {
		input := makeCopyRowsProvenanceRows(100)
		source := writeCopyRowsProvenanceFile(t, input, Compression(&Snappy))
		rows := source.RowGroups()[0].Rows()

		consumed := make([]Row, 17)
		n, err := rows.ReadRows(consumed)
		if err != nil {
			rows.Close()
			t.Fatalf("reading prefix: %v", err)
		}
		if n != len(consumed) {
			rows.Close()
			t.Fatalf("read prefix = %d, want %d", n, len(consumed))
		}

		var output bytes.Buffer
		writer := NewGenericWriter[copyRowsProvenanceRow](&output, Compression(&Snappy))
		if written := copyRowsProvenance(t, writer, rows); written != int64(len(input)-n) {
			t.Fatalf("remaining rows written = %d, want %d", written, len(input)-n)
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing destination writer: %v", err)
		}

		got := readCopyRowsProvenanceFile[copyRowsProvenanceRow](t, output.Bytes(), len(input)-n)
		if want := input[n:]; !reflect.DeepEqual(got, want) {
			t.Fatal("partially consumed reader copied rows outside its remaining suffix")
		}
	})

	t.Run("schema conversion remains row oriented", func(t *testing.T) {
		type sourceRow struct {
			ID string `parquet:"id"`
		}
		type targetRow struct {
			ID int64 `parquet:"id"`
		}

		input := []sourceRow{{ID: "1"}, {ID: "2"}, {ID: "3"}}
		source := writeCopyRowsProvenanceFile(t, input, Compression(&Snappy))

		var output bytes.Buffer
		writer := NewGenericWriter[targetRow](&output, Compression(&Snappy))
		before := copyPathCounter.Load()
		if written := copyRowsProvenance(t, writer, source.RowGroups()[0].Rows()); written != int64(len(input)) {
			t.Fatalf("converted rows written = %d, want %d", written, len(input))
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing destination writer: %v", err)
		}
		if copied := copyPathCounter.Load() - before; copied != 0 {
			t.Fatalf("schema conversion unexpectedly copied %d chunks", copied)
		}

		got := readCopyRowsProvenanceFile[targetRow](t, output.Bytes(), len(input))
		want := []targetRow{{ID: 1}, {ID: 2}, {ID: 3}}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("converted rows = %#v, want %#v", got, want)
		}
	})

	t.Run("smaller destination row groups still split", func(t *testing.T) {
		input := makeCopyRowsProvenanceRows(100)
		source := writeCopyRowsProvenanceFile(t, input, Compression(&Snappy))

		var output bytes.Buffer
		writer := NewGenericWriter[copyRowsProvenanceRow](&output,
			Compression(&Snappy),
			MaxRowsPerRowGroup(25),
		)
		if written := copyRowsProvenance(t, writer, source.RowGroups()[0].Rows()); written != int64(len(input)) {
			t.Fatalf("written rows = %d, want %d", written, len(input))
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing destination writer: %v", err)
		}

		file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
		if err != nil {
			t.Fatalf("opening destination file: %v", err)
		}
		if got := len(file.RowGroups()); got != 4 {
			t.Fatalf("destination row groups = %d, want 4", got)
		}
		for _, rowGroup := range file.RowGroups() {
			if rowGroup.NumRows() > 25 {
				t.Fatalf("destination row group has %d rows, exceeds limit", rowGroup.NumRows())
			}
		}
		got := readCopyRowsProvenanceFile[copyRowsProvenanceRow](t, output.Bytes(), len(input))
		if !reflect.DeepEqual(got, input) {
			t.Fatal("smaller destination row groups changed copied rows")
		}
	})
}

// BenchmarkCopyRowsFileRowGroup measures the public CopyRows operation for a
// matched, multi-row-group parquet file. Each timed iteration consumes the
// returned row count and closes the destination writer; the copy-path counter
// reports whether the benchmark reached Writer.WriteRowGroup's L0 handoff.
func BenchmarkCopyRowsFileRowGroup(b *testing.B) {
	const (
		numRows          = 200_000
		rowsPerRowGroup  = 50_000
		columnsPerRow    = 6
		expectedChunkOps = columnsPerRow * (numRows / rowsPerRowGroup)
	)

	input := makeCopyRowsProvenanceRows(numRows)
	var sourceOutput bytes.Buffer
	sourceWriter := NewGenericWriter[copyRowsProvenanceRow](&sourceOutput,
		Compression(&Snappy),
		MaxRowsPerRowGroup(rowsPerRowGroup),
	)
	if _, err := sourceWriter.Write(input); err != nil {
		b.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		b.Fatal(err)
	}

	source, err := OpenFile(bytes.NewReader(sourceOutput.Bytes()), int64(sourceOutput.Len()))
	if err != nil {
		b.Fatal(err)
	}
	rowGroups := source.RowGroups()
	if len(rowGroups) != numRows/rowsPerRowGroup {
		b.Fatalf("source row groups = %d, want %d", len(rowGroups), numRows/rowsPerRowGroup)
	}

	before := copyPathCounter.Load()
	b.ReportAllocs()
	b.SetBytes(int64(sourceOutput.Len()))
	b.ResetTimer()

	for b.Loop() {
		writer := NewGenericWriter[copyRowsProvenanceRow](io.Discard,
			Compression(&Snappy),
			MaxRowsPerRowGroup(rowsPerRowGroup),
		)

		var written int64
		for _, rowGroup := range rowGroups {
			rows := rowGroup.Rows()
			n, err := CopyRows(writer, rows)
			closeErr := rows.Close()
			if err != nil {
				b.Fatal(err)
			}
			if closeErr != nil {
				b.Fatal(closeErr)
			}
			written += n
		}
		if written != numRows {
			b.Fatalf("written rows = %d, want %d", written, numRows)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	copiedChunks := copyPathCounter.Load() - before
	b.ReportMetric(float64(copiedChunks)/float64(b.N), "copied_chunks/op")
	b.ReportMetric(float64(expectedChunkOps), "expected_copied_chunks/op")
}
