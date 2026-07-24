package parquet

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go/format"
)

// codecRewriteProofRow deliberately combines the one conservative L2 shape with
// two demotion shapes. Payload is a plain optional BYTE_ARRAY data-page-v2
// column; Category has a dictionary page and Flag is BOOLEAN, so neither is part
// of the raw-page transcode contract.
type codecRewriteProofRow struct {
	Payload  *string `parquet:"payload,optional"`
	Category string  `parquet:"category,dict"`
	Flag     bool    `parquet:"flag"`
}

const codecRewriteProofPageBufferSize = 8 << 10

func makeCodecRewriteProofRows(n, payloadSize int) []codecRewriteProofRow {
	rows := make([]codecRewriteProofRow, n)
	for i := range rows {
		if i%5 != 0 {
			payload := codecRewriteProofPayload(i, payloadSize)
			rows[i].Payload = &payload
		}
		rows[i].Category = fmt.Sprintf("category-%02d", i%17)
		rows[i].Flag = i%2 == 0
	}
	return rows
}

func codecRewriteProofPayload(row, size int) string {
	payload := make([]byte, size)
	for i := range payload {
		// The row-dependent bytes keep the encoded input runtime-built and prevent
		// the fixture from collapsing into one constant byte sequence.
		payload[i] = byte('a' + (row*13+i*7)%26)
	}
	return string(payload)
}

func writeCodecRewriteProofSource(t testing.TB, rows []codecRewriteProofRow) *File {
	t.Helper()

	var source bytes.Buffer
	w := NewGenericWriter[codecRewriteProofRow](
		&source,
		Compression(&Snappy),
		DataPageVersion(2),
		PageBufferSize(codecRewriteProofPageBufferSize),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}

	file, err := OpenFile(bytes.NewReader(source.Bytes()), int64(source.Len()))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return file
}

// codecRewriteL2FixtureEligible expresses the deliberately narrow L2 workload
// used by this proof. It is not the writer dispatch predicate: it locks the
// fixture to the safe plain BYTE_ARRAY/V2/codec-mismatch shape while the test
// separately proves that dictionary and BOOLEAN columns remain demotions.
func codecRewriteL2FixtureEligible(chunk *FileColumnChunk) bool {
	meta := &chunk.chunk.MetaData
	if chunk.Type().Kind() != ByteArray || meta.Codec != format.Snappy || meta.DictionaryPageOffset != 0 {
		return false
	}
	if chunk.chunk.ColumnIndexOffset == 0 || chunk.chunk.OffsetIndexOffset == 0 {
		return false
	}
	if len(meta.EncodingStats) == 0 {
		return false
	}
	for _, stat := range meta.EncodingStats {
		if stat.PageType != format.DataPageV2 || stat.Count <= 0 {
			return false
		}
	}
	return true
}

func assertCodecRewriteFixtureEligibility(t testing.TB, source *File) {
	t.Helper()

	rowGroups := source.RowGroups()
	if len(rowGroups) != 1 {
		t.Fatalf("source row groups = %d, want 1", len(rowGroups))
	}
	chunks := rowGroups[0].ColumnChunks()
	if len(chunks) != 3 {
		t.Fatalf("source columns = %d, want 3", len(chunks))
	}

	eligible := make([]int, 0, 1)
	hasDictionary := false
	hasBoolean := false
	for i, chunk := range chunks {
		fileChunk, ok := chunk.(*FileColumnChunk)
		if !ok {
			t.Fatalf("source column %d type = %T, want *FileColumnChunk", i, chunk)
		}
		if codecRewriteL2FixtureEligible(fileChunk) {
			eligible = append(eligible, i)
		}
		if fileChunk.chunk.MetaData.DictionaryPageOffset != 0 {
			hasDictionary = true
			if codecRewriteL2FixtureEligible(fileChunk) {
				t.Fatalf("dictionary column %d unexpectedly matches the L2 fixture", i)
			}
		}
		if fileChunk.Type().Kind() == Boolean {
			hasBoolean = true
			if codecRewriteL2FixtureEligible(fileChunk) {
				t.Fatalf("BOOLEAN column %d unexpectedly matches the L2 fixture", i)
			}
		}
	}

	if !reflect.DeepEqual(eligible, []int{0}) {
		t.Fatalf("L2 fixture eligible columns = %v, want [0] (plain BYTE_ARRAY only)", eligible)
	}
	if !hasDictionary {
		t.Fatal("fixture has no dictionary-boundary column to demote")
	}
	if !hasBoolean {
		t.Fatal("fixture has no BOOLEAN column to demote")
	}
}

func rewriteCodecProofRowGroup(t testing.TB, source *File) []byte {
	t.Helper()

	var destination bytes.Buffer
	w := NewGenericWriter[codecRewriteProofRow](
		&destination,
		Compression(&Zstd),
		DataPageVersion(2),
		PageBufferSize(codecRewriteProofPageBufferSize),
	)
	for _, rowGroup := range source.RowGroups() {
		if _, err := w.WriteRowGroup(rowGroup); err != nil {
			t.Fatalf("WriteRowGroup: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}
	return destination.Bytes()
}

func readCodecRewriteProofRows(t testing.TB, data []byte, want int) []codecRewriteProofRow {
	t.Helper()

	reader := NewGenericReader[codecRewriteProofRow](bytes.NewReader(data))
	defer reader.Close()

	rows := make([]codecRewriteProofRow, want)
	read := 0
	for read < len(rows) {
		n, err := reader.Read(rows[read:])
		read += n
		if err != nil {
			if err != io.EOF {
				t.Fatalf("reading rewritten rows: %v", err)
			}
			break
		}
	}
	return rows[:read]
}

func assertCodecRewriteIndexes(t testing.TB, output *File, wantRows int64, wantPayloadNulls int64) {
	t.Helper()

	rowGroups := output.RowGroups()
	if len(rowGroups) != 1 {
		t.Fatalf("output row groups = %d, want 1", len(rowGroups))
	}
	if got := rowGroups[0].NumRows(); got != wantRows {
		t.Fatalf("output row count = %d, want %d", got, wantRows)
	}

	for column, chunk := range rowGroups[0].ColumnChunks() {
		columnIndex, err := chunk.ColumnIndex()
		if err != nil {
			t.Fatalf("column %d index: %v", column, err)
		}
		offsetIndex, err := chunk.OffsetIndex()
		if err != nil {
			t.Fatalf("column %d offset index: %v", column, err)
		}
		if columnIndex.NumPages() == 0 {
			t.Fatalf("column %d has no indexed pages", column)
		}
		if columnIndex.NumPages() != offsetIndex.NumPages() {
			t.Fatalf("column %d index page counts differ: column=%d offset=%d", column, columnIndex.NumPages(), offsetIndex.NumPages())
		}

		var nulls int64
		var previousOffset int64 = -1
		var previousFirstRow int64 = -1
		for page := range offsetIndex.NumPages() {
			offset := offsetIndex.Offset(page)
			firstRow := offsetIndex.FirstRowIndex(page)
			if offset <= previousOffset {
				t.Fatalf("column %d page %d offset = %d, previous = %d", column, page, offset, previousOffset)
			}
			if firstRow < 0 || firstRow < previousFirstRow {
				t.Fatalf("column %d page %d first row = %d, previous = %d", column, page, firstRow, previousFirstRow)
			}
			if size := offsetIndex.CompressedPageSize(page); size <= 0 {
				t.Fatalf("column %d page %d compressed size = %d", column, page, size)
			}
			previousOffset = offset
			previousFirstRow = firstRow
			nulls += columnIndex.NullCount(page)
		}
		if column == 0 && nulls != wantPayloadNulls {
			t.Fatalf("payload null count = %d, want %d", nulls, wantPayloadNulls)
		}

		pages := chunk.Pages()
		for _, row := range []int64{0, wantRows / 3, wantRows / 2, wantRows - 1} {
			if err := pages.SeekToRow(row); err != nil {
				pages.Close()
				t.Fatalf("column %d SeekToRow(%d): %v", column, row, err)
			}
			page, err := pages.ReadPage()
			if err != nil {
				pages.Close()
				t.Fatalf("column %d ReadPage after SeekToRow(%d): %v", column, row, err)
			}
			Release(page)
		}
		if err := pages.Close(); err != nil {
			t.Fatalf("closing column %d pages: %v", column, err)
		}
	}
}

// TestWriteRowGroupCodecRewritePreservesRowsAndIndexes is the controlled safety
// experiment for the codec-mismatch workload. It locks the fixture to one safe
// plain BYTE_ARRAY candidate, explicitly includes dictionary and BOOLEAN
// demotions, and verifies decoded rows plus usable column/offset indexes after
// the Snappy-to-Zstd rewrite.
func TestWriteRowGroupCodecRewritePreservesRowsAndIndexes(t *testing.T) {
	rows := makeCodecRewriteProofRows(2_000, 96)
	source := writeCodecRewriteProofSource(t, rows)
	assertCodecRewriteFixtureEligibility(t, source)

	data := rewriteCodecProofRowGroup(t, source)
	output, err := OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("opening rewritten file: %v", err)
	}
	if got := output.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk).chunk.MetaData.Codec; got != format.Zstd {
		t.Fatalf("output codec = %s, want ZSTD", got)
	}

	got := readCodecRewriteProofRows(t, data, len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatalf("rewritten rows differ: got %d rows, want %d", len(got), len(rows))
	}
	assertCodecRewriteIndexes(t, output, int64(len(rows)), int64(len(rows)/5))
}

// codecTranscodeBenchRow is intentionally limited to the conservative L2
// workload: a non-dictionary optional BYTE_ARRAY column using DataPageV2.
type codecTranscodeBenchRow struct {
	Payload *string `parquet:"payload,optional"`
}

var codecTranscodeBenchmarkSink int64

func BenchmarkWriteRowGroupCodecTranscodeByteArray(b *testing.B) {
	rows := make([]codecTranscodeBenchRow, 24_000)
	for i := range rows {
		if i%7 != 0 {
			payload := codecRewriteProofPayload(i, 320)
			rows[i].Payload = &payload
		}
	}

	var source bytes.Buffer
	sourceWriter := NewGenericWriter[codecTranscodeBenchRow](
		&source,
		Compression(&Snappy),
		DataPageVersion(2),
		PageBufferSize(codecRewriteProofPageBufferSize),
	)
	if _, err := sourceWriter.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		b.Fatal(err)
	}
	file, err := OpenFile(bytes.NewReader(source.Bytes()), int64(source.Len()))
	if err != nil {
		b.Fatal(err)
	}
	rowGroups := file.RowGroups()
	if len(rowGroups) != 1 || len(rowGroups[0].ColumnChunks()) != 1 {
		b.Fatalf("benchmark source shape: %d row groups, %d columns", len(rowGroups), len(rowGroups[0].ColumnChunks()))
	}
	chunk, ok := rowGroups[0].ColumnChunks()[0].(*FileColumnChunk)
	if !ok || !codecRewriteL2FixtureEligible(chunk) {
		b.Fatal("benchmark source is not the conservative plain BYTE_ARRAY/V2 Snappy fixture")
	}

	var destination bytes.Buffer
	destination.Grow(source.Len())
	b.ReportAllocs()
	b.SetBytes(int64(source.Len()))
	b.ResetTimer()

	for b.Loop() {
		destination.Reset()
		w := NewGenericWriter[codecTranscodeBenchRow](
			&destination,
			Compression(&Zstd),
			DataPageVersion(2),
			PageBufferSize(codecRewriteProofPageBufferSize),
		)
		var written int64
		for _, rowGroup := range rowGroups {
			n, err := w.WriteRowGroup(rowGroup)
			if err != nil {
				b.Fatal(err)
			}
			written += n
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
		if written != int64(len(rows)) || destination.Len() == 0 {
			b.Fatalf("rewrite result: rows=%d bytes=%d", written, destination.Len())
		}
		// Keep the real output observable so the benchmark cannot discard the
		// rewrite just because no later benchmark iteration reads its bytes.
		codecTranscodeBenchmarkSink = written + int64(destination.Len())
	}
}
