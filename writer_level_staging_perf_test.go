package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const writerLevelStagingRowCount = 4096

type writerLevelStagingRecord struct {
	Required int32   `parquet:"required"`
	Optional *int32  `parquet:"optional,optional"`
	Values   []int32 `parquet:"values,list"`
}

type writerLevelStagingHistogram struct {
	definitions []int64
	repetitions []int64
}

func makeWriterLevelStagingInput(numRows int) (*parquet.Schema, []parquet.Row) {
	schema := parquet.SchemaOf(writerLevelStagingRecord{})
	records := make([]writerLevelStagingRecord, numRows)
	rows := make([]parquet.Row, numRows)

	for i := range records {
		value := int32(i*17 + 3)
		record := writerLevelStagingRecord{Required: value}

		switch i % 8 {
		case 0:
			// A nil optional value and nil list exercise the low definition levels.
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
		rows[i] = schema.Deconstruct(nil, &records[i])
	}

	return schema, rows
}

func writerLevelStagingHistograms(rows []parquet.Row) []writerLevelStagingHistogram {
	histograms := make([]writerLevelStagingHistogram, 0)
	for _, row := range rows {
		for _, value := range row {
			column := value.Column()
			for len(histograms) <= column {
				histograms = append(histograms, writerLevelStagingHistogram{})
			}
			histogram := &histograms[column]
			histogram.definitions = incrementWriterLevelStagingHistogram(histogram.definitions, int(value.DefinitionLevel()))
			histogram.repetitions = incrementWriterLevelStagingHistogram(histogram.repetitions, int(value.RepetitionLevel()))
		}
	}
	return histograms
}

func incrementWriterLevelStagingHistogram(histogram []int64, level int) []int64 {
	if level >= len(histogram) {
		histogram = append(histogram, make([]int64, level+1-len(histogram))...)
	}
	histogram[level]++
	return histogram
}

func TestWriterWriteRowsLevelStagingRoundTrip(t *testing.T) {
	schema, rows := makeWriterLevelStagingInput(writerLevelStagingRowCount)
	histograms := writerLevelStagingHistograms(rows)

	for _, version := range []int{1, 2} {
		t.Run(fmt.Sprintf("data-page-v%d", version), func(t *testing.T) {
			var buffer bytes.Buffer
			writer := parquet.NewWriter(
				&buffer,
				schema,
				parquet.DataPageVersion(version),
				parquet.PageBufferSize(4<<10),
				parquet.WriteBufferSize(0),
			)

			if n, err := writer.WriteRows(rows); err != nil {
				t.Fatal(err)
			} else if n != len(rows) {
				t.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}

			file, err := parquet.OpenFile(bytes.NewReader(buffer.Bytes()), int64(buffer.Len()))
			if err != nil {
				t.Fatal(err)
			}
			if got := file.NumRows(); got != int64(len(rows)) {
				t.Fatalf("file has %d rows, want %d", got, len(rows))
			}

			reader := parquet.NewReader(file)
			readBuffer := make([]parquet.Row, 127)
			rowIndex := 0
			for {
				n, err := reader.ReadRows(readBuffer)
				for i := range n {
					if !reflect.DeepEqual(readBuffer[i], rows[rowIndex+i]) {
						t.Fatalf("decoded raw row %d differs from the row supplied to WriteRows", rowIndex+i)
					}
				}
				rowIndex += n
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			if rowIndex != len(rows) {
				t.Fatalf("reader returned %d rows, want %d", rowIndex, len(rows))
			}

			assertWriterLevelStagingMetadata(t, file, histograms)
			assertWriterLevelStagingPages(t, file, histograms, len(rows))
		})
	}
}

func assertWriterLevelStagingMetadata(t *testing.T, file *parquet.File, want []writerLevelStagingHistogram) {
	t.Helper()

	metadata := file.Metadata()
	if len(metadata.RowGroups) != 1 {
		t.Fatalf("file has %d row groups, want 1", len(metadata.RowGroups))
	}
	columns := metadata.RowGroups[0].Columns
	if len(columns) != len(want) {
		t.Fatalf("row group has %d columns, want %d", len(columns), len(want))
	}

	indexes := file.ColumnIndexes()
	for column, histogram := range want {
		if len(histogram.definitions) > 1 {
			if got := columns[column].MetaData.SizeStatistics.DefinitionLevelHistogram; !equalWriterLevelStagingHistogram(got, histogram.definitions) {
				t.Errorf("column %d definition histogram = %v, want %v", column, got, histogram.definitions)
			}
			if got := sumWriterLevelStagingPageHistograms(indexes[column].DefinitionLevelHistogram, len(histogram.definitions)); !equalWriterLevelStagingHistogram(got, histogram.definitions) {
				t.Errorf("column %d aggregate column-index definition histogram = %v, want %v", column, got, histogram.definitions)
			}
		}
		if len(histogram.repetitions) > 1 {
			if got := columns[column].MetaData.SizeStatistics.RepetitionLevelHistogram; !equalWriterLevelStagingHistogram(got, histogram.repetitions) {
				t.Errorf("column %d repetition histogram = %v, want %v", column, got, histogram.repetitions)
			}
			if got := sumWriterLevelStagingPageHistograms(indexes[column].RepetitionLevelHistogram, len(histogram.repetitions)); !equalWriterLevelStagingHistogram(got, histogram.repetitions) {
				t.Errorf("column %d aggregate column-index repetition histogram = %v, want %v", column, got, histogram.repetitions)
			}
		}
	}
}

func equalWriterLevelStagingHistogram(got, want []int64) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func sumWriterLevelStagingPageHistograms(pageHistograms []int64, numLevels int) []int64 {
	if numLevels == 0 || len(pageHistograms)%numLevels != 0 {
		return nil
	}
	histogram := make([]int64, numLevels)
	for i, count := range pageHistograms {
		histogram[i%numLevels] += count
	}
	return histogram
}

func assertWriterLevelStagingPages(t *testing.T, file *parquet.File, histograms []writerLevelStagingHistogram, numRows int) {
	t.Helper()

	for _, column := range file.RowGroups()[0].ColumnChunks() {
		pages := column.Pages()
		var pageCount int
		var pageRows int64
		for {
			page, err := pages.ReadPage()
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
			pageCount++
			pageRows += page.NumRows()
			parquet.Release(page)
		}
		if err := pages.Close(); err != nil {
			t.Fatal(err)
		}
		if pageRows != int64(numRows) {
			t.Errorf("column %d pages contain %d rows, want %d", column.Column(), pageRows, numRows)
		}
		if len(histograms[column.Column()].definitions) > 1 && pageCount < 2 {
			t.Errorf("level-bearing column %d has %d pages, want multiple pages", column.Column(), pageCount)
		}
	}
}

func BenchmarkWriterWriteRowsLevelStaging(b *testing.B) {
	schema, rows := makeWriterLevelStagingInput(writerLevelStagingRowCount)
	offset := 0

	b.ReportAllocs()
	for b.Loop() {
		var buffer bytes.Buffer
		writer := parquet.NewWriter(
			&buffer,
			schema,
			parquet.PageBufferSize(4<<10),
			parquet.WriteBufferSize(0),
		)

		n, err := writer.WriteRows(rows[offset:])
		if err != nil {
			b.Fatal(err)
		}
		if offset > 0 {
			more, err := writer.WriteRows(rows[:offset])
			if err != nil {
				b.Fatal(err)
			}
			n += more
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if buffer.Len() == 0 {
			b.Fatal("writer produced an empty file")
		}

		offset = (offset + 113) % len(rows)
	}
}
