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

// This fixture makes the compressed encrypted dictionary substantially larger
// than the existing 512-byte-page companion. The release path clears both the
// decrypted envelope body and its decompressed replacement, so its cost scales
// differently from the large uncompressed BYTE_ARRAY guard.
type encryptedLargeDictionaryReaderRow struct {
	Value []byte `parquet:"value"`
}

const (
	encryptedLargeDictionaryReaderRows   = 1_024
	encryptedLargeDictionaryReaderValues = 64
	encryptedLargeDictionaryValueSize    = 512
)

func encryptedLargeDictionaryReaderFile(tb testing.TB) (*parquet.File, int, []byte, []byte) {
	tb.Helper()

	dictionary := make([][]byte, encryptedLargeDictionaryReaderValues)
	for i := range dictionary {
		dictionary[i] = bytes.Repeat([]byte{byte(i)}, encryptedLargeDictionaryValueSize)
	}
	rows := make([]encryptedLargeDictionaryReaderRow, encryptedLargeDictionaryReaderRows)
	for i := range rows {
		rows[i].Value = dictionary[i%len(dictionary)]
	}

	footerKey := aes128Key(0xE2)
	cfg := &parquet.EncryptionConfig{
		FooterKey:       footerKey,
		EncryptedFooter: true,
		FileIdentifier:  []byte{0xE2, 1, 2, 3, 4, 5, 6, 7},
	}

	var data bytes.Buffer
	writer := parquet.NewGenericWriter[encryptedLargeDictionaryReaderRow](&data,
		parquet.WithEncryption(cfg),
		parquet.PageBufferSize(32*1024),
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary),
		parquet.Compression(&parquet.Gzip),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatalf("write encrypted large dictionary rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close encrypted large dictionary writer: %v", err)
	}

	file, err := parquet.OpenFile(
		bytes.NewReader(data.Bytes()),
		int64(data.Len()),
		parquet.WithDecryption(&staticKeyRetriever{footerKey: footerKey}),
	)
	if err != nil {
		tb.Fatalf("open encrypted large dictionary file: %v", err)
	}
	metadata := file.Metadata().RowGroups[0].Columns[0].MetaData
	if metadata.Codec != format.Gzip || !slices.Contains(metadata.Encoding, format.RLEDictionary) {
		tb.Fatalf("encrypted large dictionary column has codec %v and encodings %v", metadata.Codec, metadata.Encoding)
	}
	return file, len(rows), dictionary[0], dictionary[(len(rows)-1)%len(dictionary)]
}

func BenchmarkGenericReaderEncryptedLargeCompressedDictionaryPages(b *testing.B) {
	file, numRows, first, last := encryptedLargeDictionaryReaderFile(b)
	reader := parquet.NewGenericReader[encryptedLargeDictionaryReaderRow](file)
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Errorf("close encrypted large dictionary reader: %v", err)
		}
	})

	rows := make([]encryptedLargeDictionaryReaderRow, numRows)
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		n, err := reader.Read(rows)
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatal(err)
		}
		if n != numRows {
			b.Fatalf("read %d encrypted large dictionary rows, want %d", n, numRows)
		}
		if !bytes.Equal(rows[0].Value, first) {
			b.Fatal("first encrypted large dictionary row does not match")
		}
		if !bytes.Equal(rows[len(rows)-1].Value, last) {
			b.Fatal("last encrypted large dictionary row does not match")
		}
		reader.Reset()
	}
}
