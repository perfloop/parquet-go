package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type encryptedPageReaderRow struct {
	Value int64 `parquet:"value"`
}

const encryptedPageReaderRows = 4_096

func encryptedPageReaderFile(tb testing.TB) (*parquet.File, int) {
	tb.Helper()

	rows := make([]encryptedPageReaderRow, encryptedPageReaderRows)
	for i := range rows {
		rows[i].Value = int64(i)
	}

	footerKey := aes128Key(0xD2)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{0xD2, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedPageReaderRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(512),
		parquet.DefaultEncoding(&parquet.Plain),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatalf("write encrypted rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close encrypted writer: %v", err)
	}

	file, err := parquet.OpenFile(
		bytes.NewReader(data.Bytes()),
		int64(data.Len()),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
	)
	if err != nil {
		tb.Fatalf("open encrypted file: %v", err)
	}
	return file, len(rows)
}

func TestEncryptedPageReadKeepsReleasedPagesIndependent(t *testing.T) {
	file, wantRows := encryptedPageReaderFile(t)
	columns := file.RowGroups()[0].ColumnChunks()
	if len(columns) != 1 {
		t.Fatalf("encrypted file has %d columns, want 1", len(columns))
	}

	pages := columns[0].Pages()
	defer func() {
		if err := pages.Close(); err != nil {
			t.Errorf("close encrypted pages: %v", err)
		}
	}()

	first, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("read first encrypted page: %v", err)
	}
	defer parquet.Release(first)

	second, err := pages.ReadPage()
	if err != nil {
		t.Fatalf("read second encrypted page: %v", err)
	}
	defer parquet.Release(second)

	// Reading the second page evicts the first from FilePages' page cache. The
	// first page must still own its decoded values until its caller releases it.
	next := 0
	readEncryptedPageValues(t, first, &next)
	readEncryptedPageValues(t, second, &next)
	pageCount := 2

	for {
		page, err := pages.ReadPage()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("read encrypted page %d: %v", pageCount, err)
		}

		readEncryptedPageValues(t, page, &next)
		parquet.Release(page)
		pageCount++
	}

	if pageCount < 2 {
		t.Fatalf("read %d encrypted pages, want multiple pages", pageCount)
	}
	if next != wantRows {
		t.Fatalf("read %d encrypted values, want %d", next, wantRows)
	}
}

func readEncryptedPageValues(t *testing.T, page parquet.Page, next *int) {
	t.Helper()

	values := page.Values()
	buf := make([]parquet.Value, 128)
	for {
		n, err := values.ReadValues(buf)
		for _, value := range buf[:n] {
			if got, want := value.Int64(), int64(*next); got != want {
				t.Fatalf("encrypted page value %d: got %d, want %d", *next, got, want)
			}
			*next++
		}
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("read encrypted page values: %v", err)
		}
	}
}

func BenchmarkGenericReaderEncryptedPages(b *testing.B) {
	file, numRows := encryptedPageReaderFile(b)
	reader := parquet.NewGenericReader[encryptedPageReaderRow](file)
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Errorf("close encrypted reader: %v", err)
		}
	})

	rows := make([]encryptedPageReaderRow, numRows)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		n, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatal(err)
		}
		if n != numRows {
			b.Fatalf("read %d encrypted rows, want %d", n, numRows)
		}
		if got, want := rows[0].Value, int64(0); got != want {
			b.Fatalf("first encrypted row: got %d, want %d", got, want)
		}
		if got, want := rows[len(rows)-1].Value, int64(numRows-1); got != want {
			b.Fatalf("last encrypted row: got %d, want %d", got, want)
		}
		reader.Reset()
	}
}
