package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

// This fixture makes each encrypted page body substantially larger than the
// 512-byte bodies in BenchmarkGenericReaderEncryptedPages. Final-release
// scrubbing is proportional to buffer capacity, so it guards the large-page
// side of that cost separately from the existing small-page allocation win.
type encryptedLargePlainByteArrayRow struct {
	Value []byte `parquet:"value"`
}

const (
	encryptedLargePlainByteArrayRows      = 256
	encryptedLargePlainByteArrayValueSize = 256
)

func encryptedLargePlainByteArrayFile(tb testing.TB) (*parquet.File, int) {
	tb.Helper()

	rows := make([]encryptedLargePlainByteArrayRow, encryptedLargePlainByteArrayRows)
	for i := range rows {
		value := make([]byte, encryptedLargePlainByteArrayValueSize)
		for j := range value {
			value[j] = byte(i + j)
		}
		rows[i].Value = value
	}

	footerKey := aes128Key(0xE1)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{0xE1, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedLargePlainByteArrayRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(16*1024),
		parquet.DefaultEncoding(&parquet.Plain),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatalf("write encrypted large BYTE_ARRAY rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close encrypted large BYTE_ARRAY writer: %v", err)
	}

	file, err := parquet.OpenFile(
		bytes.NewReader(data.Bytes()),
		int64(data.Len()),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
	)
	if err != nil {
		tb.Fatalf("open encrypted large BYTE_ARRAY file: %v", err)
	}
	return file, len(rows)
}

func BenchmarkGenericReaderEncryptedLargePlainByteArrayPages(b *testing.B) {
	file, numRows := encryptedLargePlainByteArrayFile(b)
	reader := parquet.NewGenericReader[encryptedLargePlainByteArrayRow](file)
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Errorf("close encrypted large BYTE_ARRAY reader: %v", err)
		}
	})

	rows := make([]encryptedLargePlainByteArrayRow, numRows)
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
		if got := len(rows[0].Value); got != encryptedLargePlainByteArrayValueSize {
			b.Fatalf("first encrypted value length: got %d, want %d", got, encryptedLargePlainByteArrayValueSize)
		}
		if got, want := rows[len(rows)-1].Value[len(rows[len(rows)-1].Value)-1], byte((encryptedLargePlainByteArrayRows-1+encryptedLargePlainByteArrayValueSize-1)%256); got != want {
			b.Fatalf("last encrypted value byte: got %d, want %d", got, want)
		}
		reader.Reset()
	}
}
