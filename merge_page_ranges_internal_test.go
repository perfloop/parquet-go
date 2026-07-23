package parquet

import (
	"bytes"
	"fmt"
	"io"
	"testing"
)

const (
	pageRangeMergeRows    = 16_384
	pageRangeMergeOverlap = pageRangeMergeRows / 16
)

type pageRangeMergeRow struct {
	Key    int64 `parquet:"key"`
	Source int32 `parquet:"source"`
	Value0 int64 `parquet:"value0"`
	Value1 int64 `parquet:"value1"`
	Value2 int64 `parquet:"value2"`
	Value3 int64 `parquet:"value3"`
	Value4 int64 `parquet:"value4"`
	Value5 int64 `parquet:"value5"`
	Value6 int64 `parquet:"value6"`
	Value7 int64 `parquet:"value7"`
}

func makePageRangeMergeRow(key int64, source int32) pageRangeMergeRow {
	return pageRangeMergeRow{
		Key:    key,
		Source: source,
		Value0: key*3 + int64(source),
		Value1: key*5 + int64(source),
		Value2: key*7 + int64(source),
		Value3: key*11 + int64(source),
		Value4: key*13 + int64(source),
		Value5: key*17 + int64(source),
		Value6: key*19 + int64(source),
		Value7: key*23 + int64(source),
	}
}

type pageRangeMergeFixture struct {
	rowGroups []RowGroup
	options   []RowGroupOption
}

// noPageIndexRowGroup creates the absent-metadata control for the range-plan
// fixture while preserving its source rows and sorting metadata.
type noPageIndexRowGroup struct{ RowGroup }

func (g noPageIndexRowGroup) ColumnChunks() []ColumnChunk {
	chunks := append([]ColumnChunk(nil), g.RowGroup.ColumnChunks()...)
	chunks[0] = noPageIndexColumnChunk{ColumnChunk: chunks[0]}
	return chunks
}

type noPageIndexColumnChunk struct{ ColumnChunk }

func (noPageIndexColumnChunk) ColumnIndex() (ColumnIndex, error) {
	return nil, ErrMissingColumnIndex
}

func newPageRangeMergeFixture(tb testing.TB) pageRangeMergeFixture {
	tb.Helper()

	rowGroups := []RowGroup{
		newPageRangeMergeRowGroup(tb, 0, 0),
		newPageRangeMergeRowGroup(tb, pageRangeMergeRows-pageRangeMergeOverlap, 1),
	}
	for _, rowGroup := range rowGroups {
		checkPageRangeMergeIndexes(tb, rowGroup)
	}

	return pageRangeMergeFixture{
		rowGroups: rowGroups,
		options: []RowGroupOption{
			SortingRowGroupConfig(
				SortingColumns(Ascending("key")),
			),
		},
	}
}

func newPageRangeMergeRowGroup(tb testing.TB, start int, source int32) RowGroup {
	tb.Helper()

	var data bytes.Buffer
	writer := NewGenericWriter[pageRangeMergeRow](
		&data,
		PageBufferSize(8*1024),
		ColumnIndexSizeLimit(func([]string) int { return 1 << 20 }),
		SortingWriterConfig(SortingColumns(Ascending("key"))),
	)
	for i := range pageRangeMergeRows {
		row := makePageRangeMergeRow(int64(start+i), source)
		if _, err := writer.Write([]pageRangeMergeRow{row}); err != nil {
			tb.Fatalf("write row %d: %v", i, err)
		}
	}
	if err := writer.Close(); err != nil {
		tb.Fatalf("close writer: %v", err)
	}

	reader := bytes.NewReader(data.Bytes())
	file, err := OpenFile(reader, reader.Size())
	if err != nil {
		tb.Fatalf("open file: %v", err)
	}
	if len(file.RowGroups()) != 1 {
		tb.Fatalf("row group count: got %d, want 1", len(file.RowGroups()))
	}
	return file.RowGroups()[0]
}

func checkPageRangeMergeIndexes(tb testing.TB, rowGroup RowGroup) {
	tb.Helper()

	chunk := rowGroup.ColumnChunks()[0]
	columnIndex, err := chunk.ColumnIndex()
	if err != nil {
		tb.Fatalf("column index: %v", err)
	}
	offsetIndex, err := chunk.OffsetIndex()
	if err != nil {
		tb.Fatalf("offset index: %v", err)
	}
	if columnIndex.NumPages() < 8 {
		tb.Fatalf("column index has %d pages, want at least 8", columnIndex.NumPages())
	}
	if columnIndex.NumPages() != offsetIndex.NumPages() {
		tb.Fatalf("page index count mismatch: column=%d offset=%d", columnIndex.NumPages(), offsetIndex.NumPages())
	}
	if !columnIndex.IsAscending() {
		tb.Fatal("column index is not ascending")
	}

	previousFirstRow := int64(-1)
	for pageIndex := range columnIndex.NumPages() {
		if columnIndex.NullPage(pageIndex) || columnIndex.NullCount(pageIndex) != 0 {
			tb.Fatalf("key page %d contains nulls", pageIndex)
		}
		firstRow := offsetIndex.FirstRowIndex(pageIndex)
		if firstRow <= previousFirstRow {
			tb.Fatalf("page %d starts at row %d after %d", pageIndex, firstRow, previousFirstRow)
		}
		previousFirstRow = firstRow
	}
}

func (f pageRangeMergeFixture) expectedRow(rowIndex int) pageRangeMergeRow {
	prefix := pageRangeMergeRows - pageRangeMergeOverlap

	switch {
	case rowIndex < prefix:
		return makePageRangeMergeRow(int64(rowIndex), 0)
	case rowIndex < prefix+2*pageRangeMergeOverlap:
		overlapIndex := rowIndex - prefix
		return makePageRangeMergeRow(int64(prefix+overlapIndex/2), int32(overlapIndex%2))
	default:
		return makePageRangeMergeRow(int64(pageRangeMergeRows+rowIndex-(prefix+2*pageRangeMergeOverlap)), 1)
	}
}

func (f pageRangeMergeFixture) merge(tb testing.TB, options []RowGroupOption) RowGroup {
	tb.Helper()

	merged, err := MergeRowGroups(f.rowGroups, options...)
	if err != nil {
		tb.Fatalf("merge row groups: %v", err)
	}
	return merged
}

func (f pageRangeMergeFixture) checkRows(tb testing.TB, merged RowGroup) {
	tb.Helper()

	rows := merged.Rows()
	defer func() {
		if err := rows.Close(); err != nil {
			tb.Errorf("close rows: %v", err)
		}
	}()

	buffer := make([]Row, 257)
	rowIndex := 0
	for {
		n, err := rows.ReadRows(buffer)
		for _, row := range buffer[:n] {
			if len(row) != 10 {
				tb.Fatalf("row %d has %d values, want 10", rowIndex, len(row))
			}
			want := f.expectedRow(rowIndex)
			got := pageRangeMergeRow{
				Key:    row[0].Int64(),
				Source: row[1].Int32(),
				Value0: row[2].Int64(),
				Value1: row[3].Int64(),
				Value2: row[4].Int64(),
				Value3: row[5].Int64(),
				Value4: row[6].Int64(),
				Value5: row[7].Int64(),
				Value6: row[8].Int64(),
				Value7: row[9].Int64(),
			}
			if got != want {
				tb.Fatalf("row %d: got %+v, want %+v", rowIndex, got, want)
			}
			rowIndex++
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatalf("read rows: %v", err)
		}
	}

	if want := 2 * pageRangeMergeRows; rowIndex != want {
		tb.Fatalf("row count: got %d, want %d", rowIndex, want)
	}
}

func TestMergeRowGroupsPageIndexedBoundaryOverlap(t *testing.T) {
	fixture := newPageRangeMergeFixture(t)

	t.Run("preserves_order_and_tie_order", func(t *testing.T) {
		merged := fixture.merge(t, fixture.options)
		fixture.checkRows(t, merged)
	})

	t.Run("falls_back_without_leading_page_index", func(t *testing.T) {
		rowGroups := append([]RowGroup(nil), fixture.rowGroups...)
		rowGroups[0] = noPageIndexRowGroup{RowGroup: rowGroups[0]}
		merged, err := MergeRowGroups(rowGroups, fixture.options...)
		if err != nil {
			t.Fatal(err)
		}
		fixture.checkRows(t, merged)
	})

	t.Run("deduplicates_across_range_boundaries", func(t *testing.T) {
		options := []RowGroupOption{
			SortingRowGroupConfig(
				SortingColumns(Ascending("key")),
				DropDuplicatedRows(true),
			),
		}
		merged := fixture.merge(t, options)
		rows := merged.Rows()
		defer rows.Close()

		buffer := make([]Row, 257)
		rowIndex := 0
		for {
			n, err := rows.ReadRows(buffer)
			for _, row := range buffer[:n] {
				if got, want := row[0].Int64(), int64(rowIndex); got != want {
					t.Fatalf("deduplicated row %d key: got %d, want %d", rowIndex, got, want)
				}
				rowIndex++
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("read deduplicated rows: %v", err)
			}
		}
		if want := 2*pageRangeMergeRows - pageRangeMergeOverlap; rowIndex != want {
			t.Fatalf("deduplicated row count: got %d, want %d", rowIndex, want)
		}
	})
}

func BenchmarkMergeRowGroupsPageIndexedBoundaryOverlap(b *testing.B) {
	fixture := newPageRangeMergeFixture(b)
	fixture.checkRows(b, fixture.merge(b, fixture.options))

	buffer := make([]Row, 256)
	var totalRows int64
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		merged := fixture.merge(b, fixture.options)
		rows := merged.Rows()
		rowCount, lastKey, err := drainPageRangeMergeRows(rows, buffer)
		if err != nil {
			b.Fatal(err)
		}
		if want := int64(2 * pageRangeMergeRows); rowCount != want {
			b.Fatalf("row count: got %d, want %d", rowCount, want)
		}
		if want := int64(2*pageRangeMergeRows - pageRangeMergeOverlap - 1); lastKey != want {
			b.Fatalf("last key: got %d, want %d", lastKey, want)
		}
		totalRows += rowCount
	}
	b.StopTimer()
	b.ReportMetric(float64(totalRows)/b.Elapsed().Seconds(), "row/s")
}

func drainPageRangeMergeRows(rows Rows, buffer []Row) (rowCount, lastKey int64, err error) {
	defer func() {
		closeErr := rows.Close()
		if err == nil {
			err = closeErr
		}
	}()

	for {
		n, readErr := rows.ReadRows(buffer)
		rowCount += int64(n)
		if n > 0 {
			lastKey = buffer[n-1][0].Int64()
		}
		switch readErr {
		case nil:
		case io.EOF:
			return rowCount, lastKey, nil
		default:
			return 0, 0, fmt.Errorf("read rows: %w", readErr)
		}
	}
}
