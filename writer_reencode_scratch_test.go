package parquet

import (
	"bytes"
	"io"
	"reflect"
	"testing"
)

// TestWriteRowGroupReencodeScratchMatchesRowPath verifies that the L3 path
// reuses its per-write scratch only after each synchronous column copy returns.
// It exercises the Snappy-to-Zstd codec-mismatch path with repeated row groups
// at both benchmarked schema widths, then compares the decoded result to the
// row-oriented fallback.
func TestWriteRowGroupReencodeScratchMatchesRowPath(t *testing.T) {
	const (
		rowsPerGroup = reencodeValueBufferSize + 1
		numRowGroups = 3
	)

	t.Run("six-columns", func(t *testing.T) {
		testWriteRowGroupReencodeScratchMatchesRowPath(t, makeBenchRows(numRowGroups*rowsPerGroup), rowsPerGroup)
	})
	t.Run("twenty-four-columns", func(t *testing.T) {
		testWriteRowGroupReencodeScratchMatchesRowPath(t, makeWideRows(numRowGroups*rowsPerGroup), rowsPerGroup)
	})
}

func testWriteRowGroupReencodeScratchMatchesRowPath[T any](t *testing.T, rows []T, rowsPerGroup int64) {
	t.Helper()

	var src bytes.Buffer
	sourceWriter := NewGenericWriter[T](&src, Compression(&Snappy), MaxRowsPerRowGroup(rowsPerGroup))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}

	source, err := OpenFile(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	if got := len(source.RowGroups()); got < 2 {
		t.Fatalf("source row groups = %d, want repeated row groups", got)
	}

	rewrite := func(reencode bool) []T {
		previous := disableWriteReencode
		disableWriteReencode = !reencode
		defer func() { disableWriteReencode = previous }()

		var dst bytes.Buffer
		// The codec mismatch rules out L0, leaving L3 or the row fallback.
		writer := NewGenericWriter[T](&dst, Compression(&Zstd), MaxRowsPerRowGroup(rowsPerGroup))
		for _, rowGroup := range source.RowGroups() {
			if _, err := writer.WriteRowGroup(rowGroup); err != nil {
				t.Fatalf("WriteRowGroup(reencode=%v): %v", reencode, err)
			}
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing destination writer: %v", err)
		}
		return readReencodeScratchRows[T](t, dst.Bytes(), len(rows))
	}

	beforeL3 := reencodePathCounter.Load()
	beforeCopy := copyPathCounter.Load()
	gotL3 := rewrite(true)
	if got := reencodePathCounter.Load() - beforeL3; got != int64(len(source.RowGroups())) {
		t.Fatalf("L3 writes = %d, want %d", got, len(source.RowGroups()))
	}
	if got := copyPathCounter.Load() - beforeCopy; got != 0 {
		t.Fatalf("L0 copies = %d, want 0 for codec mismatch", got)
	}
	gotRowPath := rewrite(false)

	if !reflect.DeepEqual(gotL3, rows) {
		t.Fatal("L3 output differs from source rows")
	}
	if !reflect.DeepEqual(gotL3, gotRowPath) {
		t.Fatal("L3 output differs from row-path output")
	}
}

func readReencodeScratchRows[T any](t *testing.T, data []byte, count int) []T {
	t.Helper()

	reader := NewGenericReader[T](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]T, count)
	read := 0
	for read < len(rows) {
		n, err := reader.Read(rows[read:])
		read += n
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("reading rewritten rows: %v", err)
		}
		if n == 0 {
			t.Fatal("reading rewritten rows made no progress")
		}
	}
	return rows[:read]
}
