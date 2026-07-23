package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const (
	writerLevelTransitionPrefixRows = 256
	writerLevelTransitionSuffixRows = 256
)

// makeWriterLevelTransitionRecords reproduces the input shape used by the
// accepted raw level-staging fixture, so its typed suffix has the same optional
// and repeated-level distribution as the raw prefix.
func makeWriterLevelTransitionRecords(numRows int) []writerLevelStagingRecord {
	records := make([]writerLevelStagingRecord, numRows)
	for i := range records {
		value := int32(i*17 + 3)
		record := writerLevelStagingRecord{Required: value}
		switch i % 8 {
		case 0:
		case 1:
			record.Optional = &value
			record.Values = []int32{}
		case 2:
			record.Values = []int32{value}
		case 3:
			record.Optional = &value
			record.Values = []int32{value, value + 1}
		case 4:
			record.Optional = &value
			record.Values = []int32{value, value + 1, value + 2, value + 3}
		case 5:
			record.Values = []int32{value, value + 1, value + 2, value + 3, value + 4}
		case 6:
			record.Optional = &value
			record.Values = []int32{value, value + 1, value + 2, value + 3, value + 4, value + 5, value + 6, value + 7}
		case 7:
			record.Optional = &value
			record.Values = []int32{value, value + 1, value + 2}
		}
		records[i] = record
	}
	return records
}

func writeWriterLevelTransitionRawPrefix(writer *parquet.GenericWriter[writerLevelStagingRecord], rows []parquet.Row) error {
	columns := make([][]parquet.Value, len(writer.ColumnWriters()))
	for _, row := range rows {
		for column, values := range row.Range {
			columns[column] = append(columns[column], values...)
		}
	}
	for column, values := range columns {
		if n, err := writer.ColumnWriters()[column].WriteRowValues(values); err != nil {
			return err
		} else if n != len(rows) {
			return fmt.Errorf("column %d raw WriteRowValues wrote %d rows, want %d", column, n, len(rows))
		}
		// This is the repository-tested ColumnWriter.WriteRowValues followed by
		// Close control. It makes the public raw prefix durable before the first
		// GenericWriter.Write, allowing both revisions to continue with the same
		// rows while the candidate's retained level adapter serves sparse writes.
		if err := writer.ColumnWriters()[column].Close(); err != nil {
			return err
		}
	}
	return nil
}

func equalWriterLevelTransitionRecord(got, want writerLevelStagingRecord) bool {
	if got.Required != want.Required || !slices.Equal(got.Values, want.Values) {
		return false
	}
	if got.Optional == nil || want.Optional == nil {
		return got.Optional == nil && want.Optional == nil
	}
	return *got.Optional == *want.Optional
}

func writerLevelTransitionOutputMetrics(data []byte, want []writerLevelStagingRecord) (int, error) {
	reader := parquet.NewGenericReader[writerLevelStagingRecord](bytes.NewReader(data))
	got := make([]writerLevelStagingRecord, len(want))
	n, err := reader.Read(got)
	closeErr := reader.Close()
	if err != nil && err != io.EOF {
		return 0, err
	}
	if closeErr != nil {
		return 0, closeErr
	}
	if n != len(got) {
		return 0, fmt.Errorf("reader returned %d rows, want %d", n, len(got))
	}
	for i := range got {
		if !equalWriterLevelTransitionRecord(got[i], want[i]) {
			return 0, fmt.Errorf("decoded row %d differs from its input", i)
		}
	}

	file, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0, err
	}
	count := 0
	for _, column := range file.RowGroups()[0].ColumnChunks() {
		pages := column.Pages()
		for {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				pages.Close()
				return 0, err
			}
			count++
			parquet.Release(page)
		}
		if err := pages.Close(); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func BenchmarkWriterRawThenGenericTransition(b *testing.B) {
	_, rows := makeWriterLevelStagingInput(2 * (writerLevelTransitionPrefixRows + writerLevelTransitionSuffixRows))
	records := makeWriterLevelTransitionRecords(len(rows))
	offset := 0
	lastOffset := 0
	var output []byte

	b.ReportAllocs()
	for b.Loop() {
		var buffer bytes.Buffer
		writer := parquet.NewGenericWriter[writerLevelStagingRecord](
			&buffer,
			parquet.PageBufferSize(1<<20),
			parquet.WriteBufferSize(0),
		)

		if err := writeWriterLevelTransitionRawPrefix(writer, rows[offset:offset+writerLevelTransitionPrefixRows]); err != nil {
			b.Fatal(err)
		}
		if n, err := writer.Write(records[offset+writerLevelTransitionPrefixRows : offset+writerLevelTransitionPrefixRows+writerLevelTransitionSuffixRows]); err != nil {
			b.Fatal(err)
		} else if n != writerLevelTransitionSuffixRows {
			b.Fatalf("generic Write wrote %d rows, want %d", n, writerLevelTransitionSuffixRows)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		output = buffer.Bytes()
		lastOffset = offset
		offset = (offset + 113) % (len(records) - writerLevelTransitionPrefixRows - writerLevelTransitionSuffixRows + 1)
	}

	pages, err := writerLevelTransitionOutputMetrics(output, records[lastOffset:lastOffset+writerLevelTransitionPrefixRows+writerLevelTransitionSuffixRows])
	if err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(pages), "pages/op")
}
