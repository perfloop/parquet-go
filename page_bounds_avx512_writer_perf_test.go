//go:build amd64 && !purego

package parquet

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go/compress/snappy"
	"github.com/parquet-go/parquet-go/internal/memory"
)

// ColumnWriter checks its page buffer after batches of 64 rows. This is the
// first flat INT64 page size that crosses its 98%-of-default-buffer target.
const defaultInt64WriterPageValues = ((DefaultPageBufferSize*98/100 + 8*64 - 1) / (8 * 64)) * 64

var (
	defaultInt64WriterPageSchema = NewSchema("int64", Group{
		"value": Required(Leaf(Int64Type)),
	})
	defaultInt64WriterPageUncompressedOptions = []WriterOption{
		defaultInt64WriterPageSchema,
		DefaultEncoding(&Plain),
		Compression(&Uncompressed),
	}
	defaultInt64WriterPageSnappyOptions = []WriterOption{
		defaultInt64WriterPageSchema,
		DefaultEncoding(&Plain),
		Compression(&snappy.Codec{}),
	}
)

func BenchmarkInt64PageBoundsBatchedFlush(b *testing.B) {
	benchmarkInt64PageBoundsWindow(b, defaultInt64WriterPageValues)
}

func BenchmarkInt64PageBoundsDefaultWindowInterior(b *testing.B) {
	benchmarkInt64PageBoundsWindow(b, (defaultInt64PageValues+DefaultPageBufferSize/8)/2)
}

func benchmarkInt64PageBoundsWindow(b *testing.B, n int) {
	if !hasAVX512VL {
		b.Fatal("default-window bounds benchmarks require AVX-512VL")
	}

	values, wantMin, wantMax := int64PageBoundsValues(n)
	page := int64Page{values: memory.SliceBufferFrom(values)}
	wantChecksum := wantMin ^ wantMax
	var checksum int64

	b.SetBytes(int64(len(values)) * 8)
	b.ReportAllocs()

	for b.Loop() {
		min, max, ok := page.Bounds()
		if !ok {
			b.Fatal("Bounds returned no values")
		}
		checksum ^= min.Int64() ^ max.Int64()
	}

	if b.N%2 == 0 {
		wantChecksum = 0
	}
	if checksum != wantChecksum {
		b.Fatalf("bounds checksum = %d, want %d", checksum, wantChecksum)
	}
}

func BenchmarkInt64WriterDefaultPage(b *testing.B) {
	if !hasAVX512VL {
		b.Fatal("BenchmarkInt64WriterDefaultPage requires AVX-512VL")
	}

	rows := int64WriterDefaultPageRows()
	b.Run("plain-uncompressed", func(b *testing.B) {
		benchmarkInt64WriterDefaultPage(b, rows, defaultInt64WriterPageUncompressedOptions)
	})
	b.Run("plain-snappy", func(b *testing.B) {
		benchmarkInt64WriterDefaultPage(b, rows, defaultInt64WriterPageSnappyOptions)
	})
}

func TestInt64WriterDefaultPageCardinality(t *testing.T) {
	rows := int64WriterDefaultPageRows()
	var output bytes.Buffer
	if err := writeInt64WriterDefaultPage(&output, rows, defaultInt64WriterPageUncompressedOptions); err != nil {
		t.Fatal(err)
	}

	file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	pages := file.RowGroups()[0].ColumnChunks()[0].Pages()
	defer pages.Close()

	page, err := pages.ReadPage()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := page.NumValues(), int64(defaultInt64WriterPageValues); got != want {
		t.Fatalf("first page NumValues() = %d, want %d", got, want)
	}
	if _, err := pages.ReadPage(); err != io.EOF {
		t.Fatalf("second page error = %v, want EOF", err)
	}
}

func int64WriterDefaultPageRows() []Row {
	rows := make([]Row, defaultInt64WriterPageValues)
	state := uint64(1)
	for i := range rows {
		state = state*6364136223846793005 + 1442695040888963407
		rows[i] = Row{Int64Value(int64(state)).Level(0, 0, 0)}
	}
	return rows
}

type int64WriterBenchmarkSink struct{ size int64 }

func (sink *int64WriterBenchmarkSink) Write(data []byte) (int, error) {
	sink.size += int64(len(data))
	return len(data), nil
}

func benchmarkInt64WriterDefaultPage(b *testing.B, rows []Row, options []WriterOption) {
	b.SetBytes(int64(len(rows)) * 8)
	b.ReportAllocs()

	for b.Loop() {
		var output int64WriterBenchmarkSink
		if err := writeInt64WriterDefaultPage(&output, rows, options); err != nil {
			b.Fatal(err)
		}
		if output.size == 0 {
			b.Fatal("writer produced no output")
		}
	}
}

func writeInt64WriterDefaultPage(output io.Writer, rows []Row, options []WriterOption) error {
	writer := NewWriter(output, options...)
	if n, err := writer.WriteRows(rows); err != nil {
		return err
	} else if n != len(rows) {
		return fmt.Errorf("WriteRows wrote %d rows, want %d", n, len(rows))
	}
	return writer.Close()
}
