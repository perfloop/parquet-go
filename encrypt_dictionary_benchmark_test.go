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

type encryptedDictionaryReaderRow struct {
	Value []byte `parquet:"value"`
}

const (
	encryptedDictionaryReaderRows   = 4_096
	encryptedDictionaryReaderValues = 128
)

func encryptedDictionaryReaderFile(tb testing.TB) (*parquet.File, int, []byte, []byte) {
	tb.Helper()

	dictionary := make([][]byte, encryptedDictionaryReaderValues)
	for i := range dictionary {
		dictionary[i] = bytes.Repeat([]byte{byte(i)}, 96)
	}
	rows := make([]encryptedDictionaryReaderRow, encryptedDictionaryReaderRows)
	for i := range rows {
		rows[i].Value = dictionary[i%len(dictionary)]
	}

	footerKey := aes128Key(0xD7)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{0xD7, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedDictionaryReaderRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(512),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.Compression(&parquet.Gzip),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatalf("write encrypted dictionary rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close encrypted dictionary writer: %v", err)
	}

	file, err := parquet.OpenFile(
		bytes.NewReader(data.Bytes()),
		int64(data.Len()),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
	)
	if err != nil {
		tb.Fatalf("open encrypted dictionary file: %v", err)
	}

	metadata := file.Metadata().RowGroups[0].Columns[0].MetaData
	if metadata.Codec != format.Gzip || !slices.Contains(metadata.Encoding, format.RLEDictionary) {
		tb.Fatalf("encrypted dictionary column has codec %v and encodings %v", metadata.Codec, metadata.Encoding)
	}
	return file, len(rows), dictionary[0], dictionary[(len(rows)-1)%len(dictionary)]
}

func BenchmarkGenericReaderEncryptedCompressedDictionaryPages(b *testing.B) {
	file, numRows, first, last := encryptedDictionaryReaderFile(b)
	reader := parquet.NewGenericReader[encryptedDictionaryReaderRow](file)
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Errorf("close encrypted dictionary reader: %v", err)
		}
	})

	rows := make([]encryptedDictionaryReaderRow, numRows)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		n, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatal(err)
		}
		if n != numRows {
			b.Fatalf("read %d encrypted dictionary rows, want %d", n, numRows)
		}
		if !bytes.Equal(rows[0].Value, first) {
			b.Fatal("first encrypted dictionary row does not match")
		}
		if !bytes.Equal(rows[len(rows)-1].Value, last) {
			b.Fatal("last encrypted dictionary row does not match")
		}
		reader.Reset()
	}
}
