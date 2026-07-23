package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"
)

// codecRewritePageRow has an optional dictionary-encoded column so the V2
// coverage below exercises its uncompressed level prefix as well as its encoded
// value payload.
type codecRewritePageRow struct {
	ID      int64   `parquet:"id"`
	Name    *string `parquet:"name,optional,dict"`
	Payload string  `parquet:"payload"`
}

func makeCodecRewritePageRows(n int) []codecRewritePageRow {
	rows := make([]codecRewritePageRow, n)
	for i := range rows {
		row := codecRewritePageRow{
			ID:      int64(i),
			Payload: fmt.Sprintf("payload-%06d-%048d", i, i*17),
		}
		if i%7 != 0 {
			name := fmt.Sprintf("category-%d", i%23)
			row.Name = &name
		}
		rows[i] = row
	}
	return rows
}

func writeCodecRewritePageFile(t testing.TB, rows []codecRewritePageRow, options ...WriterOption) *File {
	t.Helper()

	var buf bytes.Buffer
	w := NewGenericWriter[codecRewritePageRow](&buf, options...)
	for i := range rows {
		if _, err := w.Write(rows[i : i+1]); err != nil {
			t.Fatalf("writing source row %d: %v", i, err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}

	f, err := OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return f
}

func rewriteCodecPages(t testing.TB, src *File, options ...WriterOption) []byte {
	t.Helper()

	var dst bytes.Buffer
	w := NewGenericWriter[codecRewritePageRow](&dst, options...)
	for _, rowGroup := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rowGroup); err != nil {
			t.Fatalf("rewriting row group: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}
	return dst.Bytes()
}

func readCodecRewritePageRows(t testing.TB, data []byte, n int) []codecRewritePageRow {
	t.Helper()

	r := NewGenericReader[codecRewritePageRow](bytes.NewReader(data))
	defer r.Close()

	rows := make([]codecRewritePageRow, n)
	read := 0
	for read < len(rows) {
		m, err := r.Read(rows[read:])
		read += m
		if err != nil {
			if err != io.EOF {
				t.Fatalf("reading rewritten rows: %v", err)
			}
			break
		}
	}
	return rows[:read]
}

func TestWriteRowGroupCodecRewritePageIndexes(t *testing.T) {
	rows := makeCodecRewritePageRows(2_000)

	for _, dataPageVersion := range []int{1, 2} {
		t.Run(fmt.Sprintf("data-page-v%d", dataPageVersion), func(t *testing.T) {
			common := []WriterOption{
				DataPageVersion(dataPageVersion),
				PageBufferSize(512),
				ColumnIndexSizeLimit(func([]string) int { return 1 << 20 }),
			}
			src := writeCodecRewritePageFile(t, rows, append(common, Compression(&Snappy))...)

			for column, chunk := range src.RowGroups()[0].ColumnChunks() {
				offsetIndex, err := chunk.OffsetIndex()
				if err != nil {
					t.Fatalf("source column %d has no offset index: %v", column, err)
				}
				if offsetIndex.NumPages() < 2 {
					t.Fatalf("source column %d has %d pages, want multiple pages", column, offsetIndex.NumPages())
				}
			}

			beforeCopy := copyPathCounter.Load()
			out := rewriteCodecPages(t, src, append(common, Compression(&Zstd))...)
			if copied := copyPathCounter.Load() - beforeCopy; copied != 0 {
				t.Fatalf("codec rewrite unexpectedly used the verbatim path for %d columns", copied)
			}

			got := readCodecRewritePageRows(t, out, len(rows))
			if !reflect.DeepEqual(got, rows) {
				t.Fatalf("rewritten rows differ: got %d rows, want %d", len(got), len(rows))
			}

			file, err := OpenFile(bytes.NewReader(out), int64(len(out)))
			if err != nil {
				t.Fatalf("opening rewritten file: %v", err)
			}
			assertCodecRewritePageIndexes(t, file)
		})
	}
}

func assertCodecRewritePageIndexes(t testing.TB, file *File) {
	t.Helper()

	for rowGroupNumber, rowGroup := range file.RowGroups() {
		for columnNumber, column := range rowGroup.ColumnChunks() {
			columnIndex, err := column.ColumnIndex()
			if err != nil {
				t.Fatalf("row group %d column %d has no column index: %v", rowGroupNumber, columnNumber, err)
			}
			offsetIndex, err := column.OffsetIndex()
			if err != nil {
				t.Fatalf("row group %d column %d has no offset index: %v", rowGroupNumber, columnNumber, err)
			}

			pages := column.Pages()
			pageNumber := 0
			firstRow := int64(0)
			for {
				page, err := pages.ReadPage()
				if err == io.EOF {
					break
				}
				if err != nil {
					pages.Close()
					t.Fatalf("reading row group %d column %d page %d: %v", rowGroupNumber, columnNumber, pageNumber, err)
				}
				if pageNumber >= columnIndex.NumPages() || pageNumber >= offsetIndex.NumPages() {
					pages.Close()
					t.Fatalf("row group %d column %d has an unindexed page %d", rowGroupNumber, columnNumber, pageNumber)
				}
				if got := offsetIndex.FirstRowIndex(pageNumber); got != firstRow {
					pages.Close()
					t.Fatalf("row group %d column %d page %d starts at row %d, index says %d", rowGroupNumber, columnNumber, pageNumber, firstRow, got)
				}
				if got := offsetIndex.CompressedPageSize(pageNumber); got <= 0 {
					pages.Close()
					t.Fatalf("row group %d column %d page %d has invalid compressed size %d", rowGroupNumber, columnNumber, pageNumber, got)
				}
				if got, want := columnIndex.NullCount(pageNumber), page.NumNulls(); got != want {
					pages.Close()
					t.Fatalf("row group %d column %d page %d null count is %d, want %d", rowGroupNumber, columnNumber, pageNumber, got, want)
				}
				if min, max, ok := page.Bounds(); ok {
					if got := columnIndex.MinValue(pageNumber); !Equal(got, min) {
						pages.Close()
						t.Fatalf("row group %d column %d page %d min index does not match decoded page", rowGroupNumber, columnNumber, pageNumber)
					}
					if got := columnIndex.MaxValue(pageNumber); !Equal(got, max) {
						pages.Close()
						t.Fatalf("row group %d column %d page %d max index does not match decoded page", rowGroupNumber, columnNumber, pageNumber)
					}
				}

				firstRow += page.NumRows()
				pageNumber++
			}
			if err := pages.Close(); err != nil {
				t.Fatalf("closing row group %d column %d pages: %v", rowGroupNumber, columnNumber, err)
			}
			if got, want := pageNumber, columnIndex.NumPages(); got != want {
				t.Fatalf("row group %d column %d has %d decoded pages, column index has %d", rowGroupNumber, columnNumber, got, want)
			}
			if got, want := pageNumber, offsetIndex.NumPages(); got != want {
				t.Fatalf("row group %d column %d has %d decoded pages, offset index has %d", rowGroupNumber, columnNumber, got, want)
			}
		}
	}
}
