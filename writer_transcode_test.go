package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	geom "github.com/twpayne/go-geom"
	"slices"
)

const copyTestColumnCount = 3

func writeCopyTestData(t testing.TB, rows []copyTestRow, options ...WriterOption) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewGenericWriter[copyTestRow](&buf, options...)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}
	return buf.Bytes()
}

func openCopyTestData(t testing.TB, data []byte, options ...FileOption) *File {
	t.Helper()

	file, err := OpenFile(bytes.NewReader(data), int64(len(data)), options...)
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	return file
}

func rewriteCopyTestRowGroup(t testing.TB, src *File, options ...WriterOption) ([]byte, error) {
	t.Helper()

	var dst bytes.Buffer
	w := NewGenericWriter[copyTestRow](&dst, options...)
	for _, rowGroup := range src.RowGroups() {
		if _, err := w.WriteRowGroup(rowGroup); err != nil {
			return nil, err
		}
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return dst.Bytes(), nil
}

// TestWriteRowGroupTranscodeRebuildsMetadata changes deserialized footer index
// and statistics values without touching raw pages. L2 must derive the output
// index and chunk statistics from page headers, not copy those stale values.
func testWriteRowGroupTranscodeRebuildsMetadata(t *testing.T) {
	rows := makeCopyTestRows(5_000)
	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), PageBufferSize(512)))

	source := src.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk)
	wantStats := cloneStatistics(source.chunk.MetaData.Statistics)
	if len(src.offsetIndexes) == 0 || len(src.offsetIndexes[0].PageLocations) == 0 {
		t.Fatal("source has no offset index page locations")
	}
	if len(src.columnIndexes) == 0 || len(src.columnIndexes[0].MinValues) == 0 {
		t.Fatal("source has no column index bounds")
	}
	src.offsetIndexes[0].PageLocations[0].FirstRowIndex = 17
	src.columnIndexes[0].MinValues[0] = bytes.Repeat([]byte{0xFF}, len(src.columnIndexes[0].MinValues[0]))
	source.chunk.MetaData.Statistics.MinValue = bytes.Repeat([]byte{0xFF}, len(source.chunk.MetaData.Statistics.MinValue))

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd), PageBufferSize(512))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(copyTestColumnCount); got != want {
		t.Fatalf("transcoded %d columns, want %d", got, want)
	}

	file := openCopyTestData(t, out)
	output := file.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk)
	if !reflect.DeepEqual(output.chunk.MetaData.Statistics, wantStats) {
		t.Fatalf("output chunk statistics were not rebuilt from pages:\n got: %#v\nwant: %#v", output.chunk.MetaData.Statistics, wantStats)
	}
	for column, input := range src.RowGroups()[0].ColumnChunks() {
		inputMeta := input.(*FileColumnChunk).chunk.MetaData
		outputMeta := file.RowGroups()[0].ColumnChunks()[column].(*FileColumnChunk).chunk.MetaData
		if !reflect.DeepEqual(outputMeta.SizeStatistics, inputMeta.SizeStatistics) {
			t.Fatalf("column %d size statistics changed:\n got: %#v\nwant: %#v", column, outputMeta.SizeStatistics, inputMeta.SizeStatistics)
		}
		if !reflect.DeepEqual(outputMeta.EncodingStats, inputMeta.EncodingStats) {
			t.Fatalf("column %d encoding statistics changed:\n got: %#v\nwant: %#v", column, outputMeta.EncodingStats, inputMeta.EncodingStats)
		}
	}
	assertCodecRewritePageIndexes(t, file)
}

// testWriteRowGroupTranscodeUsesImmutableSourceCodec covers File.Metadata's
// public mutability. FileColumnChunk retains the physical codec selected when
// the file was opened, so L2 must ignore a later footer mutation.
func testWriteRowGroupTranscodeUsesImmutableSourceCodec(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy)))
	for i := range src.Metadata().RowGroups[0].Columns {
		src.Metadata().RowGroups[0].Columns[i].MetaData.Codec = format.Uncompressed
	}
	for _, chunk := range src.RowGroups()[0].ColumnChunks() {
		if got := chunk.(*FileColumnChunk).column.Compression().CompressionCodec(); got != format.Snappy {
			t.Fatalf("cached source codec = %s, want SNAPPY", got)
		}
	}

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(copyTestColumnCount); got != want {
		t.Fatalf("transcoded %d columns with mutated source codec metadata, want %d", got, want)
	}
	if got := readCopyTestRows(t, out, len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("mutated source codec metadata changed rows")
	}
}

// testWriteRowGroupTranscodeUsesImmutableSourceLayout verifies that raw L2
// reads the layout captured when OpenFile constructed the column chunk, not
// mutable public offsets in File.Metadata.
func testWriteRowGroupTranscodeUsesImmutableSourceLayout(t *testing.T) {
	type twoInt64Row struct {
		Left  int64 `parquet:"left"`
		Right int64 `parquet:"right"`
	}
	rows := make([]twoInt64Row, 1_000)
	for i := range rows {
		rows[i] = twoInt64Row{Left: int64(i), Right: int64(1_000_000 + i)}
	}
	var source bytes.Buffer
	sourceWriter := NewGenericWriter[twoInt64Row](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatal(err)
	}
	src := openCopyTestData(t, source.Bytes())
	metadata := src.Metadata()
	left := metadata.RowGroups[0].Columns[0].MetaData
	right := &metadata.RowGroups[0].Columns[1].MetaData
	right.DataPageOffset = left.DataPageOffset
	right.DictionaryPageOffset = left.DictionaryPageOffset
	right.TotalCompressedSize = left.TotalCompressedSize

	before := transcodePathCounter.Load()
	var output bytes.Buffer
	writer := NewGenericWriter[twoInt64Row](&output, Compression(&Zstd))
	if _, err := writer.WriteRowGroup(src.RowGroups()[0]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(2); got != want {
		t.Fatalf("transcoded %d columns after source layout mutation, want %d", got, want)
	}
	reader := NewGenericReader[twoInt64Row](bytes.NewReader(output.Bytes()))
	defer reader.Close()
	decoded := make([]twoInt64Row, len(rows))
	if n, err := reader.Read(decoded); (err != nil && !errors.Is(err, io.EOF)) || n != len(rows) {
		t.Fatalf("reading rewritten rows = (%d, %v), want %d rows", n, err, len(rows))
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatal("mutable source layout substituted a column")
	}
}

// testWriteRowGroupTranscodeUsesPageRowCount ensures raw-page output takes its
// row count from DataPageV2 headers rather than mutable File metadata.
func testWriteRowGroupTranscodeUsesPageRowCount(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy)))
	src.Metadata().RowGroups[0].NumRows = int64(len(rows) - 1)

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(copyTestColumnCount); got != want {
		t.Fatalf("transcoded %d columns, want %d", got, want)
	}
	if got := readCopyTestRows(t, out, len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("mutable source row count changed rows")
	}
}

// testWriteRowGroupTranscodeDemotesByteArraySizeStats keeps the one
// size-statistic field that requires decoded values on the L3 path. Mutating the
// public source footer must not become destination metadata.
func testWriteRowGroupTranscodeDemotesByteArraySizeStats(t *testing.T) {
	type byteArrayRow struct {
		Value string `parquet:"value"`
	}
	rows := []byteArrayRow{{"one"}, {"two"}, {"three"}}
	var source bytes.Buffer
	sourceWriter := NewGenericWriter[byteArrayRow](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatal(err)
	}
	src := openCopyTestData(t, source.Bytes())
	want := cloneSizeStatistics(src.Metadata().RowGroups[0].Columns[0].MetaData.SizeStatistics)
	if want.UnencodedByteArrayDataBytes == 0 {
		t.Fatal("source byte-array column has no size statistics")
	}
	src.Metadata().RowGroups[0].Columns[0].MetaData.SizeStatistics.UnencodedByteArrayDataBytes = 0

	before := transcodePathCounter.Load()
	var output bytes.Buffer
	writer := NewGenericWriter[byteArrayRow](&output, Compression(&Zstd))
	if _, err := writer.WriteRowGroup(src.RowGroups()[0]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := transcodePathCounter.Load() - before; got != 0 {
		t.Fatalf("transcoded %d byte-array columns", got)
	}
	file := openCopyTestData(t, output.Bytes())
	got := file.Metadata().RowGroups[0].Columns[0].MetaData.SizeStatistics
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("output byte-array size statistics = %#v, want %#v", got, want)
	}
	reader := NewGenericReader[byteArrayRow](bytes.NewReader(output.Bytes()))
	defer reader.Close()
	decoded := make([]byteArrayRow, len(rows))
	if n, err := reader.Read(decoded); (err != nil && !errors.Is(err, io.EOF)) || n != len(rows) {
		t.Fatalf("reading rewritten rows = (%d, %v), want %d rows", n, err, len(rows))
	}
	if !reflect.DeepEqual(decoded, rows) {
		t.Fatal("mutated source size statistics changed rows")
	}
}

// testWriteRowGroupTranscodeDictionaryByteArraySizeStats proves the sole
// BYTE_ARRAY shape L2 keeps has the same size-statistics result as L3 without
// trusting a mutable source footer.
func testWriteRowGroupTranscodeDictionaryByteArraySizeStats(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	const nameColumn = 1

	l3src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), DataPageVersion(1), PageBufferSize(512)))
	l3out, err := rewriteCopyTestRowGroup(t, l3src, Compression(&Zstd), DataPageVersion(2), PageBufferSize(512))
	if err != nil {
		t.Fatal(err)
	}
	want := openCopyTestData(t, l3out).Metadata().RowGroups[0].Columns[nameColumn].MetaData.SizeStatistics

	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), PageBufferSize(512)))
	src.Metadata().RowGroups[0].Columns[nameColumn].MetaData.SizeStatistics.UnencodedByteArrayDataBytes = 123
	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd), PageBufferSize(512))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(copyTestColumnCount); got != want {
		t.Fatalf("transcoded %d columns, want %d", got, want)
	}
	file := openCopyTestData(t, out)
	got := file.Metadata().RowGroups[0].Columns[nameColumn].MetaData.SizeStatistics
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("dictionary byte-array size statistics = %#v, want L3 %#v", got, want)
	}
	if got := readCopyTestRows(t, out, len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("dictionary byte-array transcode changed rows")
	}
}

// testWriteRowGroupTranscodeRespectsDictionaryLimit ensures a destination
// dictionary policy remains owned by L3 rather than preserving source pages.
func testWriteRowGroupTranscodeRespectsDictionaryLimit(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), PageBufferSize(512)))

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd), PageBufferSize(512), DictionaryMaxBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(copyTestColumnCount-1); got != want {
		t.Fatalf("transcoded %d non-dictionary columns, want %d", got, want)
	}
	file := openCopyTestData(t, out)
	stats := file.Metadata().RowGroups[0].Columns[1].MetaData.EncodingStats
	plainDataPage := false
	for _, stat := range stats {
		plainDataPage = plainDataPage || (stat.PageType == format.DataPageV2 && stat.Encoding == format.Plain)
	}
	if !plainDataPage {
		t.Fatal("destination dictionary limit did not switch a data page to PLAIN")
	}
	if got := readCopyTestRows(t, out, len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("dictionary-limit rewrite changed rows")
	}
}

func corruptFirstCopyTestPage(t testing.TB, data []byte) []byte {
	t.Helper()

	corrupted := slices.Clone(data)
	file := openCopyTestData(t, corrupted)
	chunk := file.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk)
	offset := chunk.chunk.MetaData.DataPageOffset
	reader := bytes.NewReader(corrupted[offset:])
	protocol := thrift.CompactProtocol{}
	header := new(format.PageHeader)
	if err := thrift.NewDecoder(protocol.NewReader(reader)).Decode(header); err != nil {
		t.Fatalf("decoding source page header: %v", err)
	}
	if header.CRC == 0 {
		t.Fatal("source page has no CRC")
	}
	bodyOffset := int(offset) + len(corrupted[offset:]) - reader.Len()
	body := corrupted[bodyOffset : bodyOffset+int(header.CompressedPageSize)]
	codec := LookupCompressionCodec(chunk.chunk.MetaData.Codec)
	for i, value := range body {
		body[i] = value ^ 1
		if _, err := codec.Decode(nil, body); err == nil {
			return corrupted
		}
		body[i] = value
	}
	t.Fatal("could not alter a compressed page body without breaking decompression")
	return nil
}

// TestWriteRowGroupTranscodeRejectsBadSourceCRC verifies that a raw-page rewrite
// preserves the existing FilePages integrity boundary rather than emitting a
// fresh checksum for a corrupt but decompressible source body. The V1 subtest
// takes L3, so both paths must reject the same input class.
func testWriteRowGroupTranscodeRejectsBadSourceCRC(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	for _, pageVersion := range []int{1, 2} {
		t.Run(fmt.Sprintf("data-page-v%d", pageVersion), func(t *testing.T) {
			data := writeCopyTestData(t, rows, Compression(&Snappy), DataPageVersion(pageVersion))
			src := openCopyTestData(t, corruptFirstCopyTestPage(t, data))

			before := transcodePathCounter.Load()
			_, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd), DataPageVersion(2))
			if !errors.Is(err, ErrCorrupted) {
				t.Fatalf("WriteRowGroup error = %v, want ErrCorrupted", err)
			}
			if got := transcodePathCounter.Load() - before; got != 0 {
				t.Fatalf("transcoded %d corrupt columns", got)
			}
		})
	}
}

func firstCopyTestDataPageHeader(t testing.TB, data []byte, column int) *format.PageHeader {
	t.Helper()

	file := openCopyTestData(t, data)
	chunk := file.RowGroups()[0].ColumnChunks()[column].(*FileColumnChunk)
	offset := chunk.chunk.MetaData.DataPageOffset
	reader := bytes.NewReader(data[offset:])
	protocol := thrift.CompactProtocol{}
	header := new(format.PageHeader)
	if err := thrift.NewDecoder(protocol.NewReader(reader)).Decode(header); err != nil {
		t.Fatalf("decoding data page header: %v", err)
	}
	if header.Type != format.DataPageV2 {
		t.Fatalf("page type = %s, want DATA_PAGE_V2", header.Type)
	}
	return header
}

// TestWriteRowGroupTranscodeRespectsMetadataPolicy exercises options whose
// output metadata must be recomputed by L3 rather than inherited from a source
// page. It also covers a source lacking page statistics when the destination
// requests them.
func testWriteRowGroupTranscodeRespectsMetadataPolicy(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	allSkipStats := []WriterOption{SkipPageStatistics("id"), SkipPageStatistics("name"), SkipPageStatistics("val")}
	allSkipBounds := []WriterOption{SkipPageBounds("id"), SkipPageBounds("name"), SkipPageBounds("val")}

	for _, test := range []struct {
		name    string
		options []WriterOption
		check   func(*testing.T, *File, []byte)
	}{
		{
			name:    "page-statistics-disabled",
			options: []WriterOption{DataPageStatistics(false)},
			check: func(t *testing.T, _ *File, data []byte) {
				for column := range copyTestColumnCount {
					header := firstCopyTestDataPageHeader(t, data, column)
					if !reflect.DeepEqual(header.DataPageHeaderV2.V.Statistics, format.Statistics{}) {
						t.Fatalf("column %d retained page statistics despite DataPageStatistics(false)", column)
					}
				}
			},
		},
		{
			name:    "page-statistics-skipped",
			options: allSkipStats,
			check: func(t *testing.T, _ *File, data []byte) {
				for column := range copyTestColumnCount {
					header := firstCopyTestDataPageHeader(t, data, column)
					if !reflect.DeepEqual(header.DataPageHeaderV2.V.Statistics, format.Statistics{}) {
						t.Fatalf("column %d retained skipped page statistics", column)
					}
				}
			},
		},
		{
			name:    "page-bounds-skipped",
			options: allSkipBounds,
			check: func(t *testing.T, file *File, _ []byte) {
				for column, chunk := range file.RowGroups()[0].ColumnChunks() {
					meta := chunk.(*FileColumnChunk).chunk.MetaData
					if meta.Statistics.MinValue != nil || meta.Statistics.MaxValue != nil {
						t.Fatalf("column %d retained skipped chunk bounds", column)
					}
				}
			},
		},
		{
			name:    "deprecated-statistics-enabled",
			options: []WriterOption{DeprecatedDataPageStatistics(true)},
			check: func(t *testing.T, file *File, _ []byte) {
				for column, chunk := range file.RowGroups()[0].ColumnChunks() {
					meta := chunk.(*FileColumnChunk).chunk.MetaData
					if meta.Statistics.Min == nil || meta.Statistics.Max == nil {
						t.Fatalf("column %d omitted requested deprecated statistics", column)
					}
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy)))
			before := transcodePathCounter.Load()
			options := append([]WriterOption{Compression(&Zstd)}, test.options...)
			out, err := rewriteCopyTestRowGroup(t, src, options...)
			if err != nil {
				t.Fatal(err)
			}
			if got := transcodePathCounter.Load() - before; got != 0 {
				t.Fatalf("transcoded %d columns despite destination metadata policy", got)
			}
			file := openCopyTestData(t, out)
			test.check(t, file, out)
		})
	}

	t.Run("source-page-statistics-absent", func(t *testing.T) {
		src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), DataPageStatistics(false)))
		before := transcodePathCounter.Load()
		out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd))
		if err != nil {
			t.Fatal(err)
		}
		if got := transcodePathCounter.Load() - before; got != 0 {
			t.Fatalf("transcoded %d columns with missing source page statistics", got)
		}
		for column := range copyTestColumnCount {
			header := firstCopyTestDataPageHeader(t, out, column)
			if reflect.DeepEqual(header.DataPageHeaderV2.V.Statistics, format.Statistics{}) {
				t.Fatalf("column %d did not restore destination page statistics", column)
			}
		}
	})
}

func testWriteRowGroupTranscodeDemotesForBloomFilter(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	filter := SplitBlockFilter(10, "id")
	src := openCopyTestData(t, writeCopyTestData(t, rows, Compression(&Snappy), BloomFilters(filter)))

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd), BloomFilters(filter))
	if err != nil {
		t.Fatal(err)
	}
	if got, want := transcodePathCounter.Load()-before, int64(2); got != want {
		t.Fatalf("transcoded %d columns, want %d eligible non-filter columns", got, want)
	}
	file := openCopyTestData(t, out)
	bloom := file.RowGroups()[0].ColumnChunks()[0].BloomFilter()
	if bloom == nil {
		t.Fatal("rewritten column has no bloom filter")
	}
	if ok, err := bloom.Check(ValueOf(int64(500))); err != nil || !ok {
		t.Fatalf("rewritten bloom filter misses a present value: ok=%v err=%v", ok, err)
	}
}

type transcodeTestKeyRetriever struct {
	footer  []byte
	columns map[string][]byte
	missing bool
}

func (k transcodeTestKeyRetriever) FooterKey([]byte) ([]byte, error) { return k.footer, nil }

func (k transcodeTestKeyRetriever) ColumnKey(path []string, _ []byte) ([]byte, error) {
	if k.missing {
		return nil, fmt.Errorf("missing key for %q: %w", strings.Join(path, "."), ErrKeyNotFound)
	}
	if key, ok := k.columns[strings.Join(path, ".")]; ok {
		return key, nil
	}
	return k.footer, nil
}

func writeEncryptedCopyTestData(t testing.TB, rows []copyTestRow, config *EncryptionConfig) []byte {
	t.Helper()

	var buf bytes.Buffer
	w := NewGenericWriter[copyTestRow](&buf, Compression(&Snappy), WithEncryption(config))
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("writing encrypted rows: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("closing encrypted writer: %v", err)
	}
	return buf.Bytes()
}

func testWriteRowGroupTranscodeDemotesEncryptedSource(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	footerKey := bytes.Repeat([]byte{0xA5}, 16)
	data := writeEncryptedCopyTestData(t, rows, &EncryptionConfig{FooterKey: footerKey, EncryptedFooter: true})
	src := openCopyTestData(t, data, WithDecryption(transcodeTestKeyRetriever{footer: footerKey}))

	before := transcodePathCounter.Load()
	out, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd))
	if err != nil {
		t.Fatal(err)
	}
	if got := transcodePathCounter.Load() - before; got != 0 {
		t.Fatalf("transcoded %d encrypted columns", got)
	}
	if got := readCopyTestRows(t, out, len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("encrypted source rewrite changed rows")
	}
}

func testWriteRowGroupTranscodeDemotesGeospatialColumn(t *testing.T) {
	type geometryRow struct {
		Geom geom.T `parquet:"geom,geometry(OGC:CRS84)"`
	}
	rows := []geometryRow{
		{Geom: geom.NewPointFlat(geom.XY, []float64{10, 20})},
		{Geom: geom.NewPointFlat(geom.XY, []float64{30, 40})},
	}

	var source bytes.Buffer
	sourceWriter := NewGenericWriter[geometryRow](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatal(err)
	}
	src := openCopyTestData(t, source.Bytes())
	want := src.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk).chunk.MetaData.GeospatialStatistics

	before := transcodePathCounter.Load()
	var output bytes.Buffer
	writer := NewGenericWriter[geometryRow](&output, Compression(&Zstd))
	if _, err := writer.WriteRowGroup(src.RowGroups()[0]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if got := transcodePathCounter.Load() - before; got != 0 {
		t.Fatalf("transcoded %d geospatial columns", got)
	}
	out := openCopyTestData(t, output.Bytes())
	got := out.RowGroups()[0].ColumnChunks()[0].(*FileColumnChunk).chunk.MetaData.GeospatialStatistics
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("geospatial statistics changed:\n got: %#v\nwant: %#v", got, want)
	}
}

func testWriteRowGroupTranscodeRejectsMissingColumnKey(t *testing.T) {
	rows := makeCopyTestRows(1_000)
	footerKey := bytes.Repeat([]byte{0xA5}, 16)
	columnKey := bytes.Repeat([]byte{0x5A}, 16)
	data := writeEncryptedCopyTestData(t, rows, &EncryptionConfig{
		FooterKey:       footerKey,
		ColumnKeys:      map[string][]byte{"id": columnKey},
		EncryptedFooter: true,
	})
	src := openCopyTestData(t, data, WithDecryption(transcodeTestKeyRetriever{footer: footerKey, missing: true}))

	before := transcodePathCounter.Load()
	_, err := rewriteCopyTestRowGroup(t, src, Compression(&Zstd))
	if err == nil {
		t.Fatal("WriteRowGroup succeeded without the source column key")
	}
	if got := transcodePathCounter.Load() - before; got != 0 {
		t.Fatalf("transcoded %d inaccessible columns", got)
	}
}

func TestWriteRowGroupTranscodeRebuildsMetadata(t *testing.T) {
	testWriteRowGroupTranscodeRebuildsMetadata(t)
}

func TestWriteRowGroupTranscodeUsesImmutableSourceCodec(t *testing.T) {
	testWriteRowGroupTranscodeUsesImmutableSourceCodec(t)
}

func TestWriteRowGroupTranscodeUsesImmutableSourceLayout(t *testing.T) {
	testWriteRowGroupTranscodeUsesImmutableSourceLayout(t)
}

func TestWriteRowGroupTranscodeUsesPageRowCount(t *testing.T) {
	testWriteRowGroupTranscodeUsesPageRowCount(t)
}

func TestWriteRowGroupTranscodeDemotesByteArraySizeStats(t *testing.T) {
	testWriteRowGroupTranscodeDemotesByteArraySizeStats(t)
}

func TestWriteRowGroupTranscodeDictionaryByteArraySizeStats(t *testing.T) {
	testWriteRowGroupTranscodeDictionaryByteArraySizeStats(t)
}

func TestWriteRowGroupTranscodeRespectsDictionaryLimit(t *testing.T) {
	testWriteRowGroupTranscodeRespectsDictionaryLimit(t)
}

func TestWriteRowGroupTranscodeRejectsBadSourceCRC(t *testing.T) {
	testWriteRowGroupTranscodeRejectsBadSourceCRC(t)
}

func TestWriteRowGroupTranscodeRespectsMetadataPolicy(t *testing.T) {
	testWriteRowGroupTranscodeRespectsMetadataPolicy(t)
}

func TestWriteRowGroupTranscodeDemotesForBloomFilter(t *testing.T) {
	testWriteRowGroupTranscodeDemotesForBloomFilter(t)
}

func TestWriteRowGroupTranscodeDemotesEncryptedSource(t *testing.T) {
	testWriteRowGroupTranscodeDemotesEncryptedSource(t)
}

func TestWriteRowGroupTranscodeDemotesGeospatialColumn(t *testing.T) {
	testWriteRowGroupTranscodeDemotesGeospatialColumn(t)
}

func TestWriteRowGroupTranscodeRejectsMissingColumnKey(t *testing.T) {
	testWriteRowGroupTranscodeRejectsMissingColumnKey(t)
}
