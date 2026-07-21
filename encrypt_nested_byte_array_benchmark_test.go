package parquet_test

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

type encryptedNestedByteArrayReaderRow struct {
	Values []string `parquet:"values,list"`
}

const encryptedNestedByteArrayReaderRows = 4_096

func encryptedNestedByteArrayReaderFile(tb testing.TB) (*parquet.File, int, []string, []string) {
	tb.Helper()

	rows := make([]encryptedNestedByteArrayReaderRow, encryptedNestedByteArrayReaderRows)
	for i := range rows {
		rows[i].Values = []string{
			string(bytes.Repeat([]byte{byte('a' + i%26)}, 17+i%31)),
			string(bytes.Repeat([]byte{byte('A' + i%26)}, 43+i%29)),
			string(bytes.Repeat([]byte{byte('0' + i%10)}, 71+i%23)),
		}
	}

	footerKey := aes128Key(0xDA)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{0xDA, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedNestedByteArrayReaderRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(512),
		parquet.DefaultEncoding(&parquet.Plain),
		parquet.Compression(&parquet.Uncompressed),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatalf("write encrypted nested byte-array rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close encrypted nested byte-array writer: %v", err)
	}

	file, err := parquet.OpenFile(
		bytes.NewReader(data.Bytes()),
		int64(data.Len()),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
	)
	if err != nil {
		tb.Fatalf("open encrypted nested byte-array file: %v", err)
	}

	metadata := file.Metadata().RowGroups[0].Columns[0].MetaData
	if metadata.Codec != format.Uncompressed || !slices.Contains(metadata.Encoding, format.Plain) || slices.Contains(metadata.Encoding, format.RLEDictionary) {
		tb.Fatalf("encrypted nested byte-array column has codec %v and encodings %v", metadata.Codec, metadata.Encoding)
	}
	return file, len(rows), rows[0].Values, rows[len(rows)-1].Values
}

func BenchmarkGenericReaderEncryptedNestedPlainByteArrayPages(b *testing.B) {
	file, numRows, first, last := encryptedNestedByteArrayReaderFile(b)
	reader := parquet.NewGenericReader[encryptedNestedByteArrayReaderRow](file)
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Errorf("close encrypted nested byte-array reader: %v", err)
		}
	})

	rows := make([]encryptedNestedByteArrayReaderRow, numRows)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		n, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatal(err)
		}
		if n != numRows {
			b.Fatalf("read %d encrypted nested byte-array rows, want %d", n, numRows)
		}
		if !slices.Equal(rows[0].Values, first) {
			b.Fatal("first encrypted nested byte-array row does not match")
		}
		if !slices.Equal(rows[len(rows)-1].Values, last) {
			b.Fatal("last encrypted nested byte-array row does not match")
		}
		reader.Reset()
	}
}
