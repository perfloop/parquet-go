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

const genericReaderDeltaInt64ValueCount = 8193

type genericReaderDeltaInt64Row struct {
	Value int64 `parquet:"value,delta,uncompressed"`
}

func genericReaderDeltaInt64Rows() []genericReaderDeltaInt64Row {
	rows := make([]genericReaderDeltaInt64Row, genericReaderDeltaInt64ValueCount)
	for i := range rows {
		rows[i].Value = int64(i) * int64(i)
	}
	return rows
}

func newGenericReaderDeltaInt64(t testing.TB) (*parquet.GenericReader[genericReaderDeltaInt64Row], []genericReaderDeltaInt64Row, []genericReaderDeltaInt64Row) {
	t.Helper()

	want := genericReaderDeltaInt64Rows()
	data := new(bytes.Buffer)
	writer := parquet.NewGenericWriter[genericReaderDeltaInt64Row](data,
		parquet.PageBufferSize(128*1024),
		parquet.WriteBufferSize(0),
	)
	if _, err := writer.Write(want); err != nil {
		t.Fatalf("write delta rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close delta writer: %v", err)
	}
	assertGenericReaderDeltaInt64Encoding(t, data.Bytes())

	reader := parquet.NewGenericReader[genericReaderDeltaInt64Row](bytes.NewReader(data.Bytes()))
	return reader, want, make([]genericReaderDeltaInt64Row, len(want))
}

func assertGenericReaderDeltaInt64Encoding(t testing.TB, data []byte) {
	t.Helper()

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("open delta file: %v", err)
	}
	metadata := file.Metadata()
	if len(metadata.RowGroups) != 1 || len(metadata.RowGroups[0].Columns) != 1 {
		t.Fatalf("unexpected delta file layout: %d row groups", len(metadata.RowGroups))
	}
	for _, encoding := range metadata.RowGroups[0].Columns[0].MetaData.Encoding {
		if encoding == format.DeltaBinaryPacked {
			return
		}
	}
	t.Fatal("delta benchmark file does not advertise DELTA_BINARY_PACKED")
}

func readGenericReaderDeltaInt64(t testing.TB, reader *parquet.GenericReader[genericReaderDeltaInt64Row], got, want []genericReaderDeltaInt64Row) {
	t.Helper()

	n, err := reader.Read(got)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("read delta rows: %v", err)
	}
	if n != len(want) {
		t.Fatalf("read %d delta rows, want %d", n, len(want))
	}
	if !slices.Equal(got, want) {
		t.Fatal("decoded delta rows differ")
	}
}

func TestGenericReaderDeltaInt64(t *testing.T) {
	reader, want, got := newGenericReaderDeltaInt64(t)
	defer reader.Close()
	readGenericReaderDeltaInt64(t, reader, got, want)
}

func BenchmarkGenericReaderDeltaInt64(b *testing.B) {
	reader, want, got := newGenericReaderDeltaInt64(b)
	defer reader.Close()
	readGenericReaderDeltaInt64(b, reader, got, want)
	reader.Reset()

	b.ReportAllocs()
	b.SetBytes(int64(len(want)) * 8)
	b.ResetTimer()
	for b.Loop() {
		reader.Reset()
		n, err := reader.Read(got)
		if err != nil && !errors.Is(err, io.EOF) {
			b.Fatalf("read delta rows: %v", err)
		}
		if n != len(want) {
			b.Fatalf("read %d delta rows, want %d", n, len(want))
		}
	}
	b.StopTimer()

	if !slices.Equal(got, want) {
		b.Fatal("decoded delta rows differ")
	}
}
