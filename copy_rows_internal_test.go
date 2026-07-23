package parquet

import (
	"bytes"
	"errors"
	"hash/crc32"
	"io"
	"reflect"
	"slices"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// TestCopyRowsFileRowGroupPreservesAppendBehavior compares the provenance
// handoff with CopyRows' row-oriented path. WriteRowGroup flushes a pending
// group, while CopyRows must instead append source rows into it.
func TestCopyRowsFileRowGroupPreservesAppendBehavior(t *testing.T) {
	sourceRows := makeCopyTestRows(10)
	for i := range sourceRows {
		sourceRows[i].ID += 20
	}
	source := writeCopyTestFile(t, sourceRows,
		SortingWriterConfig(SortingColumns(Ascending("id"))),
	)

	for _, test := range []struct {
		name       string
		prefixRows int
		wantGroups []int64
		wantMin    []int64
		wantMax    []int64
	}{
		{
			name:       "below destination limit",
			prefixRows: 10,
			wantGroups: []int64{20},
			wantMin:    []int64{0},
			wantMax:    []int64{29},
		},
		{
			name:       "crosses destination limit",
			prefixRows: 20,
			wantGroups: []int64{25, 5},
			wantMin:    []int64{0, 25},
			wantMax:    []int64{24, 29},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			prefix := makeCopyTestRows(test.prefixRows)
			copyTo := func(rowOriented bool) []byte {
				var output bytes.Buffer
				writer := NewGenericWriter[copyTestRow](&output,
					MaxRowsPerRowGroup(25),
					SortingWriterConfig(SortingColumns(Ascending("id"))),
				)
				if _, err := writer.Write(prefix); err != nil {
					t.Fatalf("writing prefix: %v", err)
				}

				rows := source.RowGroups()[0].Rows()
				reader := RowReader(rows)
				if rowOriented {
					reader = rowReaderNoWriterTo{RowReader: rows, schema: rows.Schema()}
				}
				written, err := CopyRows(writer, reader)
				if err != nil {
					rows.Close()
					t.Fatalf("CopyRows: %v", err)
				}
				if written != int64(len(sourceRows)) {
					rows.Close()
					t.Fatalf("written rows = %d, want %d", written, len(sourceRows))
				}

				if !rowOriented {
					buffer := make([]Row, 1)
					if n, err := rows.ReadRows(buffer); n != 0 || !errors.Is(err, io.EOF) {
						rows.Close()
						t.Fatalf("source reader after CopyRows = (%d, %v), want (0, EOF)", n, err)
					}
				}
				if err := rows.Close(); err != nil {
					t.Fatalf("closing source rows: %v", err)
				}
				if err := writer.Close(); err != nil {
					t.Fatalf("closing destination writer: %v", err)
				}
				return output.Bytes()
			}

			baseline := copyTo(true)
			got := copyTo(false)
			checkCopyRowsRowGroupMetadata(t, baseline, test.wantGroups, test.wantMin, test.wantMax)
			checkCopyRowsRowGroupMetadata(t, got, test.wantGroups, test.wantMin, test.wantMax)

			want := slices.Concat(prefix, sourceRows)
			if copied := readCopyTestRows(t, got, len(want)); !reflect.DeepEqual(copied, want) {
				t.Fatal("CopyRows output differs from the appended source rows")
			}
		})
	}
}

func TestCopyRowsFileRowGroupConsumesReaderAfterHandoff(t *testing.T) {
	sourceRows := makeCopyTestRows(10)
	source := writeCopyTestFile(t, sourceRows, MaxRowsPerRowGroup(10))
	rows := source.RowGroups()[0].Rows()
	defer rows.Close()

	var output bytes.Buffer
	writer := NewGenericWriter[copyTestRow](&output, MaxRowsPerRowGroup(10))
	before := copyPathCounter.Load()
	written, err := CopyRows(writer, rows)
	if err != nil {
		t.Fatalf("CopyRows: %v", err)
	}
	if written != int64(len(sourceRows)) {
		t.Fatalf("written rows = %d, want %d", written, len(sourceRows))
	}
	if copied := copyPathCounter.Load() - before; copied == 0 {
		t.Fatal("CopyRows did not use the file row-group handoff")
	}
	if n, err := rows.ReadRows(make([]Row, 1)); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("source reader after handoff = (%d, %v), want (0, EOF)", n, err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}
}

func TestCopyRowsFileRowGroupPreservesDestinationLifecycle(t *testing.T) {
	t.Run("does not inherit source sorting", func(t *testing.T) {
		sourceRows := makeCopyTestRows(10)
		source := writeCopyTestFile(t, sourceRows,
			MaxRowsPerRowGroup(10),
			SortingWriterConfig(SortingColumns(Ascending("id"))),
		)
		for _, test := range []struct {
			name    string
			options []WriterOption
			want    []SortingColumn
		}{
			{
				name:    "unconfigured destination",
				options: []WriterOption{MaxRowsPerRowGroup(10)},
			},
			{
				name: "different configured destination",
				options: []WriterOption{
					MaxRowsPerRowGroup(10),
					SortingWriterConfig(SortingColumns(Descending("id"))),
				},
				want: []SortingColumn{Descending("id")},
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				baseline := copyFileRowGroupForTest(t, source.RowGroups()[0], true, nil, test.options...)
				got := copyFileRowGroupForTest(t, source.RowGroups()[0], false, nil, test.options...)
				if !bytes.Equal(got, baseline) {
					t.Fatal("provenance handoff changed the destination file")
				}

				file, err := OpenFile(bytes.NewReader(got), int64(len(got)))
				if err != nil {
					t.Fatalf("opening destination file: %v", err)
				}
				if sorting := file.RowGroups()[0].SortingColumns(); len(sorting) != len(test.want) ||
					!sortingColumnsHavePrefix(sorting, test.want) {
					t.Fatalf("destination sorting columns = %#v, want %#v", sorting, test.want)
				}
			})
		}
	})

	t.Run("allows later append", func(t *testing.T) {
		sourceRows := makeCopyTestRows(10)
		source := writeCopyTestFile(t, sourceRows, MaxRowsPerRowGroup(10))
		laterRows := makeCopyTestRows(5)
		baseline := copyFileRowGroupThenWriteRowsForTest(t, source.RowGroups()[0], true, laterRows, MaxRowsPerRowGroup(25))
		got := copyFileRowGroupThenWriteRowsForTest(t, source.RowGroups()[0], false, laterRows, MaxRowsPerRowGroup(25))
		if !bytes.Equal(got, baseline) {
			t.Fatal("provenance handoff changed the later-append destination file")
		}

		file, err := OpenFile(bytes.NewReader(got), int64(len(got)))
		if err != nil {
			t.Fatalf("opening destination file: %v", err)
		}
		if len(file.RowGroups()) != 1 || file.RowGroups()[0].NumRows() != 15 {
			var numRows int64
			if len(file.RowGroups()) > 0 {
				numRows = file.RowGroups()[0].NumRows()
			}
			t.Fatalf("destination row groups = %d with %d rows, want one 15-row group", len(file.RowGroups()), numRows)
		}
	})
}

func copyFileRowGroupForTest(t *testing.T, source RowGroup, rowOriented bool, laterRows []copyTestRow, options ...WriterOption) []byte {
	t.Helper()

	var output bytes.Buffer
	writer := NewGenericWriter[copyTestRow](&output, options...)
	rows := source.Rows()
	reader := RowReader(rows)
	if rowOriented {
		reader = rowReaderNoWriterTo{RowReader: rows, schema: rows.Schema()}
	}
	written, err := CopyRows(writer, reader)
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("CopyRows: %v", err)
	}
	if written != source.NumRows() {
		t.Fatalf("written rows = %d, want %d", written, source.NumRows())
	}
	if _, err := writer.Write(laterRows); err != nil {
		t.Fatalf("writing later rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}
	return output.Bytes()
}

func copyFileRowGroupThenWriteRowsForTest(t *testing.T, source RowGroup, rowOriented bool, laterRows []copyTestRow, options ...WriterOption) []byte {
	t.Helper()

	schema := SchemaOf(copyTestRow{})
	var output bytes.Buffer
	writer := NewWriter(&output, append([]WriterOption{schema}, options...)...)
	rows := source.Rows()
	reader := RowReader(rows)
	if rowOriented {
		reader = rowReaderNoWriterTo{RowReader: rows, schema: rows.Schema()}
	}
	written, err := CopyRows(writer, reader)
	if closeErr := rows.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatalf("CopyRows: %v", err)
	}
	if written != source.NumRows() {
		t.Fatalf("written rows = %d, want %d", written, source.NumRows())
	}

	rowBuffer := make([]Row, len(laterRows))
	for i := range laterRows {
		rowBuffer[i] = schema.Deconstruct(rowBuffer[i], laterRows[i])
	}
	defer clearRows(rowBuffer)
	if n, err := writer.WriteRows(rowBuffer); err != nil || n != len(rowBuffer) {
		t.Fatalf("writing later rows = (%d, %v), want (%d, nil)", n, err, len(rowBuffer))
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}
	return output.Bytes()
}

func checkCopyRowsRowGroupMetadata(t *testing.T, data []byte, wantRows, wantMin, wantMax []int64) {
	t.Helper()

	file, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening copied file: %v", err)
	}
	if len(file.RowGroups()) != len(wantRows) {
		t.Fatalf("output row groups = %d, want %d", len(file.RowGroups()), len(wantRows))
	}
	for i, rowGroup := range file.RowGroups() {
		if got := rowGroup.NumRows(); got != wantRows[i] {
			t.Fatalf("row group %d rows = %d, want %d", i, got, wantRows[i])
		}
		sorting := rowGroup.SortingColumns()
		if len(sorting) != 1 || !reflect.DeepEqual(sorting[0].Path(), []string{"id"}) ||
			sorting[0].Descending() || sorting[0].NullsFirst() {
			t.Fatalf("row group %d sorting columns = %#v, want ascending(id)", i, sorting)
		}
		column, ok := rowGroup.ColumnChunks()[0].(*FileColumnChunk)
		if !ok {
			t.Fatalf("row group %d first column type = %T, want *FileColumnChunk", i, rowGroup.ColumnChunks()[0])
		}
		min, max, ok := column.Bounds()
		if !ok || min.Int64() != wantMin[i] || max.Int64() != wantMax[i] {
			t.Fatalf("row group %d id bounds = (%v, %v, %t), want (%d, %d, true)",
				i, min, max, ok, wantMin[i], wantMax[i])
		}
	}
}

type shortRowGroupWriter struct {
	rowGroups int
	rows      int
}

func (w *shortRowGroupWriter) WriteRows(rows []Row) (int, error) {
	w.rows += len(rows)
	return len(rows), nil
}

func (w *shortRowGroupWriter) WriteRowGroup(RowGroup) (int64, error) {
	w.rowGroups++
	return 1, nil
}

func TestCopyRowsFileRowGroupFallsBackForGenericRowGroupWriter(t *testing.T) {
	sourceRows := makeCopyTestRows(10)
	source := writeCopyTestFile(t, sourceRows)
	rows := source.RowGroups()[0].Rows()
	defer rows.Close()

	destination := new(shortRowGroupWriter)
	written, err := CopyRows(destination, rows)
	if err != nil {
		t.Fatalf("CopyRows: %v", err)
	}
	if written != int64(len(sourceRows)) {
		t.Fatalf("written rows = %d, want %d", written, len(sourceRows))
	}
	if destination.rowGroups != 0 {
		t.Fatalf("generic RowGroupWriter was called %d times", destination.rowGroups)
	}
	if destination.rows != len(sourceRows) {
		t.Fatalf("row-oriented destination received %d rows, want %d", destination.rows, len(sourceRows))
	}
}

func TestCopyRowsFileRowGroupPreservesCorruptionFailure(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	options := []WriterOption{
		Compression(&Snappy),
		MaxRowsPerRowGroup(int64(len(rows))),
	}
	data, source := writeCopyRowsCorruptionSource(t, rows, options...)

	t.Run("clean input reaches handoff", func(t *testing.T) {
		rows := source.RowGroups()[0].Rows()
		defer rows.Close()

		var output bytes.Buffer
		writer := NewGenericWriter[copyTestRow](&output, options...)
		before := copyPathCounter.Load()
		want := source.RowGroups()[0].NumRows()
		if n, err := CopyRows(writer, rows); err != nil || n != want {
			t.Fatalf("CopyRows = (%d, %v), want (%d, nil)", n, err, want)
		}
		if copied := copyPathCounter.Load() - before; copied == 0 {
			t.Fatal("clean source did not reach the L0 handoff")
		}
		if err := writer.Close(); err != nil {
			t.Fatalf("closing destination writer: %v", err)
		}
	})

	copyErr := func(corrupted []byte, rowOriented bool) error {
		file, err := OpenFile(bytes.NewReader(corrupted), int64(len(corrupted)))
		if err != nil {
			t.Fatalf("opening corrupt source: %v", err)
		}

		rows := file.RowGroups()[0].Rows()
		defer rows.Close()
		reader := RowReader(rows)
		if rowOriented {
			reader = rowReaderNoWriterTo{RowReader: rows, schema: rows.Schema()}
		}

		var output bytes.Buffer
		writer := NewGenericWriter[copyTestRow](&output, options...)
		_, err = CopyRows(writer, reader)
		return err
	}

	t.Run("page checksum", func(t *testing.T) {
		corrupted := corruptFirstCopyRowsDataPage(t, data, source)
		baselineErr := copyErr(corrupted, true)
		if !errors.Is(baselineErr, ErrCorrupted) {
			t.Fatalf("row-oriented CopyRows error = %v, want ErrCorrupted", baselineErr)
		}
		if err := copyErr(corrupted, false); !errors.Is(err, ErrCorrupted) {
			t.Fatalf("provenance CopyRows error = %v, want ErrCorrupted", err)
		}
	})

	t.Run("compressed body with recomputed checksum", func(t *testing.T) {
		corrupted := corruptFirstCopyRowsDataPageWithValidChecksum(t, data, source)
		validateFirstCopyRowsPageChecksum(t, corrupted, source)
		if baselineErr := copyErr(corrupted, true); baselineErr == nil {
			t.Fatal("row-oriented CopyRows succeeded with corrupt compressed data")
		}
		if err := copyErr(corrupted, false); err == nil {
			t.Fatal("provenance CopyRows succeeded with corrupt compressed data")
		}
	})
}

func writeCopyRowsCorruptionSource(t *testing.T, rows []copyTestRow, options ...WriterOption) ([]byte, *File) {
	t.Helper()

	var output bytes.Buffer
	writer := NewGenericWriter[copyTestRow](&output, options...)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}

	data := bytes.Clone(output.Bytes())
	file, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return data, file
}

func validateFirstCopyRowsPageChecksum(t *testing.T, data []byte, file *File) {
	t.Helper()

	rowGroup, ok := file.RowGroups()[0].(*FileRowGroup)
	if !ok {
		t.Fatalf("source row group type = %T, want *FileRowGroup", file.RowGroups()[0])
	}
	chunk, ok := rowGroup.columns[0].(*FileColumnChunk)
	if !ok {
		t.Fatalf("source column chunk type = %T, want *FileColumnChunk", rowGroup.columns[0])
	}
	pages := chunk.PagesFrom(bytes.NewReader(data))
	defer pages.Close()
	header := new(format.PageHeader)
	if err := pages.decoder.Decode(header); err != nil {
		t.Fatalf("decoding first page header: %v", err)
	}
	page, err := pages.readPage(header, pages.rbuf)
	if err != nil {
		t.Fatalf("validating first page checksum: %v", err)
	}
	page.unref()
}

func corruptFirstCopyRowsDataPageWithValidChecksum(t *testing.T, data []byte, file *File) []byte {
	t.Helper()

	rowGroup, ok := file.RowGroups()[0].(*FileRowGroup)
	if !ok {
		t.Fatalf("source row group type = %T, want *FileRowGroup", file.RowGroups()[0])
	}
	chunk, ok := rowGroup.columns[0].(*FileColumnChunk)
	if !ok {
		t.Fatalf("source column chunk type = %T, want *FileColumnChunk", rowGroup.columns[0])
	}
	pages := chunk.PagesFrom(bytes.NewReader(data))
	defer pages.Close()
	var header format.PageHeader
	if err := pages.decoder.Decode(&header); err != nil {
		t.Fatalf("decoding first page header: %v", err)
	}
	if header.CRC == 0 {
		t.Fatal("expected source data page checksum")
	}

	sectionOffset, err := pages.section.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("locating first page body: %v", err)
	}
	bodyOffset := chunk.chunk.MetaData.DataPageOffset + sectionOffset - int64(pages.rbuf.Buffered())
	bodyEnd := bodyOffset + int64(header.CompressedPageSize)
	if bodyOffset < 0 || bodyEnd > int64(len(data)) {
		t.Fatalf("first page body [%d,%d) is outside source data of %d bytes", bodyOffset, bodyEnd, len(data))
	}

	var encoded bytes.Buffer
	if err := thrift.NewEncoder(pages.protocol.NewWriter(&encoded)).Encode(&header); err != nil {
		t.Fatalf("encoding first page header: %v", err)
	}
	headerOffset := bodyOffset - int64(encoded.Len())
	if headerOffset < 0 || !bytes.Equal(data[headerOffset:bodyOffset], encoded.Bytes()) {
		t.Fatal("encoded first page header does not match source bytes")
	}

	corrupted := bytes.Clone(data)
	corrupted[bodyOffset+int64(header.CompressedPageSize)/2] ^= 0xFF
	header.CRC = int32(crc32.ChecksumIEEE(corrupted[bodyOffset:bodyEnd]))
	encoded.Reset()
	if err := thrift.NewEncoder(pages.protocol.NewWriter(&encoded)).Encode(&header); err != nil {
		t.Fatalf("encoding modified page header: %v", err)
	}
	if encoded.Len() != int(bodyOffset-headerOffset) {
		t.Fatalf("modified page header length = %d, want %d", encoded.Len(), bodyOffset-headerOffset)
	}
	copy(corrupted[headerOffset:bodyOffset], encoded.Bytes())
	return corrupted
}

func corruptFirstCopyRowsDataPage(t *testing.T, data []byte, file *File) []byte {
	t.Helper()

	rowGroup, ok := file.RowGroups()[0].(*FileRowGroup)
	if !ok {
		t.Fatalf("source row group type = %T, want *FileRowGroup", file.RowGroups()[0])
	}
	chunk, ok := rowGroup.columns[0].(*FileColumnChunk)
	if !ok {
		t.Fatalf("source column chunk type = %T, want *FileColumnChunk", rowGroup.columns[0])
	}
	if chunk.chunk.MetaData.DictionaryPageOffset != 0 {
		t.Fatal("expected first source column to have no dictionary page")
	}

	pages := chunk.PagesFrom(bytes.NewReader(data))
	defer pages.Close()
	header := new(format.PageHeader)
	if err := pages.decoder.Decode(header); err != nil {
		t.Fatalf("decoding first page header: %v", err)
	}
	if header.Type != format.DataPage && header.Type != format.DataPageV2 {
		t.Fatalf("first page type = %s, want data page", header.Type)
	}
	if header.CompressedPageSize < 2 {
		t.Fatalf("first page compressed size = %d, want at least 2", header.CompressedPageSize)
	}

	sectionOffset, err := pages.section.Seek(0, io.SeekCurrent)
	if err != nil {
		t.Fatalf("locating first page body: %v", err)
	}
	bodyOffset := chunk.chunk.MetaData.DataPageOffset + sectionOffset - int64(pages.rbuf.Buffered())
	bodyEnd := bodyOffset + int64(header.CompressedPageSize)
	if bodyOffset < 0 || bodyEnd > int64(len(data)) {
		t.Fatalf("first page body [%d,%d) is outside source data of %d bytes", bodyOffset, bodyEnd, len(data))
	}

	corrupted := bytes.Clone(data)
	corrupted[bodyOffset+int64(header.CompressedPageSize)/2] ^= 0xFF
	return corrupted
}
