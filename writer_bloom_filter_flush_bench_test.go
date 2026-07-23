package parquet_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"
)

const streamingBloomFilterRows = 16 * 1024

var streamingBloomFilterSchema = parquet.NewSchema("row", parquet.Group{
	"value": parquet.String(),
})

func makeStreamingBloomFilterRows() []parquet.Row {
	rows := make([]parquet.Row, streamingBloomFilterRows)
	for i := range rows {
		x := uint64(i + 1)
		value := fmt.Sprintf("%016x-%016x-%016x-%016x", x*0x9E3779B185EBCA87, x*0xC2B2AE3D27D4EB4F, x*0x165667B19E3779F9, x*0x85EBCA77C2B2AE63)
		rows[i] = parquet.Row{parquet.ValueOf(value).Level(0, 0, 0)}
	}
	return rows
}

func newStreamingBloomFilterWriter(output *bytes.Buffer, options ...parquet.WriterOption) *parquet.Writer {
	writerOptions := []parquet.WriterOption{
		streamingBloomFilterSchema,
		parquet.BloomFilters(parquet.SplitBlockFilter(10, "value")),
		parquet.DefaultEncoding(&parquet.Plain),
		parquet.Compression(&parquet.Zstd),
		parquet.DataPageVersion(2),
		parquet.PageBufferSize(4 * 1024),
		parquet.WriteBufferSize(0),
	}
	writerOptions = append(writerOptions, options...)
	return parquet.NewWriter(output, writerOptions...)
}

func writeStreamingBloomFilterRows(writer *parquet.Writer, rows []parquet.Row) error {
	n, err := writer.WriteRows(rows)
	if err != nil {
		return err
	}
	if n != len(rows) {
		return fmt.Errorf("wrote %d rows, want %d", n, len(rows))
	}
	return nil
}

func TestWriterFlushStreamingBloomFilterMembership(t *testing.T) {
	rows := makeStreamingBloomFilterRows()

	for _, useFileBuffer := range []bool{false, true} {
		name := "memory-buffer"
		if useFileBuffer {
			name = "file-buffer"
		}

		t.Run(name, func(t *testing.T) {
			var bufferDir string
			var options []parquet.WriterOption
			if useFileBuffer {
				bufferDir = t.TempDir()
				options = append(options, parquet.ColumnPageBuffers(parquet.NewFileBufferPool(bufferDir, "pages-*")))
			}

			output := new(bytes.Buffer)
			writer := newStreamingBloomFilterWriter(output, options...)
			if err := writeStreamingBloomFilterRows(writer, rows); err != nil {
				t.Fatal(err)
			}
			if err := writer.Flush(); err != nil {
				t.Fatal(err)
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			file, err := parquet.OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if len(file.RowGroups()) != 1 {
				t.Fatalf("got %d row groups, want 1", len(file.RowGroups()))
			}

			meta := file.Metadata().RowGroups[0].Columns[0].MetaData
			dataPages := 0
			for _, stat := range meta.EncodingStats {
				if stat.PageType == format.DataPageV2 {
					dataPages += int(stat.Count)
				}
			}
			if dataPages < 2 {
				t.Fatalf("got %d data pages, want at least 2", dataPages)
			}

			filter := file.RowGroups()[0].ColumnChunks()[0].BloomFilter()
			if filter == nil {
				t.Fatal("missing bloom filter")
			}
			for i, row := range rows {
				ok, err := filter.Check(row[0])
				if err != nil {
					t.Fatalf("checking row %d: %v", i, err)
				}
				if !ok {
					t.Fatalf("bloom filter does not contain row %d", i)
				}
			}

			if bufferDir != "" {
				entries, err := os.ReadDir(bufferDir)
				if err != nil {
					t.Fatal(err)
				}
				if len(entries) != 0 {
					t.Fatalf("file buffer pool retained %d temporary files", len(entries))
				}
			}
		})
	}
}

func BenchmarkWriterFlushStreamingBloomFilter(b *testing.B) {
	rows := makeStreamingBloomFilterRows()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		output := new(bytes.Buffer)
		writer := newStreamingBloomFilterWriter(output)
		if err := writeStreamingBloomFilterRows(writer, rows); err != nil {
			b.Fatal(err)
		}
		beforeFlush := output.Len()

		b.StartTimer()
		err := writer.Flush()
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if output.Len() <= beforeFlush {
			b.Fatal("flush did not write the row group")
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWriterWriteRowsStreamingBloomFilter(b *testing.B) {
	rows := makeStreamingBloomFilterRows()
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		b.StopTimer()
		output := new(bytes.Buffer)
		writer := newStreamingBloomFilterWriter(output)

		b.StartTimer()
		err := writeStreamingBloomFilterRows(writer, rows)
		b.StopTimer()
		if err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if output.Len() == 0 {
			b.Fatal("close did not write the parquet file")
		}
	}
}
