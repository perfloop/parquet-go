package parquet

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"sync/atomic"

	"github.com/parquet-go/parquet-go/compress"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
	"github.com/parquet-go/parquet-go/internal/memory"
)

// transcodePathCounter counts column chunks rewritten by the L2 raw-page path.
// Test/benchmark instrumentation only.
var transcodePathCounter atomic.Int64

var (
	pageTranscoders      memory.Pool[pageTranscoder]
	errTranscodeFallback = errors.New("raw page transcoding is ineligible")
)

// transcodedChunk holds the metadata and dictionary bytes for a chunk whose
// data pages have been recompressed into its ColumnWriter page buffer.
type transcodedChunk struct {
	dictionary []byte
}

// columnChunkIsTranscodable reports whether src can be rewritten without
// decoding values. L2 deliberately supports only plaintext, flat DataPageV2
// columns with the writer's default statistics policy. This keeps the raw-page
// path independent of persisted page indexes and
// leaves configuration-sensitive layouts on the L3 value path.
func columnChunkIsTranscodable(dst *ColumnWriter, src *FileColumnChunk) bool {
	// Any encryption metadata, including a column key unavailable to the caller,
	// must stay on the authenticated page-read path.
	if src.file.decryption != nil || src.source.encrypted || src.decryptionKey != nil || dst.encKey != nil {
		return false
	}
	// L2 rebuilds indexes from DataPageV2 headers. Nested level histograms and
	// geospatial statistics require decoded values, so retain L3 for them.
	if dst.maxRepetitionLevel != 0 || dst.maxDefinitionLevel != 0 || dst.geospatialAccumulator != nil {
		return false
	}
	// The raw path preserves normalized source page statistics. Destination
	// policies that suppress or add metadata must be handled by L3.
	if dst.header.page.Type != format.DataPageV2 || !dst.writePageStats || !dst.writePageBounds || dst.writeDeprecatedStatistics {
		return false
	}

	// File.Metadata is publicly mutable. FileColumnChunk snapshots the physical
	// layout at OpenFile time; use that and Column's cached type/codec rather than
	// a caller-mutated footer to select or read raw bytes.
	sourceCodec := src.column.Compression()
	if sourceCodec == nil || sourceCodec.CompressionCodec() != src.source.codec {
		return false
	}
	if src.column.Type().Kind() != dst.columnType.Kind() {
		return false
	}
	if src.source.codec == dst.compression.CompressionCodec() {
		return false
	}
	// A destination dictionary limit can switch later pages to PLAIN, which raw
	// transcode cannot emulate without constructing destination dictionary values.
	if dst.dictionary != nil && dst.dictionaryMaxBytes != 0 {
		return false
	}
	if dst.columnFilter != nil {
		return false
	}
	return encodingStatsMatch(src.source.encodingStats, dst)
}

// loadTranscodedChunk reads the source's raw pages, changes only their
// compression, and stages the rewritten data pages in c. It rebuilds output
// indexes and chunk statistics from DataPageV2 headers instead of trusting the
// source's persisted index locations or footer statistics.
func (c *ColumnWriter) loadTranscodedChunk(src *FileColumnChunk) error {
	transcoded := new(transcodedChunk)

	baseOffset := src.source.dataPageOffset
	if src.source.dictionaryPageOffset != 0 {
		baseOffset = src.source.dictionaryPageOffset
	}
	section := io.NewSectionReader(src.file.reader, baseOffset, src.source.totalCompressedSize)
	rbuf, rbufpool := getBufioReader(section, src.file.config.ReadBufferSize)
	defer putBufioReader(rbuf, rbufpool)

	protocol := thrift.CompactProtocol{}
	decoder := thrift.NewDecoder(protocol.NewReader(rbuf))
	sourceCodec := src.column.Compression()
	transcoder := pageTranscoders.Get(
		func() *pageTranscoder {
			t := new(pageTranscoder)
			t.reset(sourceCodec, c.compression)
			return t
		},
		func(t *pageTranscoder) { t.reset(sourceCodec, c.compression) },
	)
	defer pageTranscoders.Put(transcoder)

	var (
		dataOffset        int64
		totalCompressed   int64
		totalUncompressed int64
		dataPageIndex     int
		dictionarySeen    bool
		firstRowIndex     int64
	)

	for pageIndex := 0; ; pageIndex++ {
		header := new(format.PageHeader)
		if err := decoder.Decode(header); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("reading source page header %d: %w", pageIndex, err)
		}
		if err := validatePageHeader(header); err != nil {
			return err
		}

		n := int(header.CompressedPageSize)
		if cap(transcoder.source) < n {
			transcoder.source = make([]byte, n)
		} else {
			transcoder.source = transcoder.source[:n]
		}
		if _, err := io.ReadFull(rbuf, transcoder.source); err != nil {
			return fmt.Errorf("reading source page body %d: %w", pageIndex, err)
		}
		if err := validateTranscodedPageChecksum(header, transcoder.source, src); err != nil {
			return err
		}
		if err := validateTranscodedPage(header, c); err != nil {
			return err
		}
		// L3 derives BYTE_ARRAY size statistics from decoded values. Its
		// dictionary page accounting is empty, but a plain (or delta) data page
		// carries values whose unencoded byte count cannot be recovered here.
		if c.columnType.Kind() == ByteArray && header.Type == format.DataPageV2 &&
			!isDictionaryFormat(header.DataPageHeaderV2.V.Encoding) {
			return fmt.Errorf("byte-array page encoding requires value decoding: %w", errTranscodeFallback)
		}

		var (
			min, max Value
			numRows  int64
		)
		if header.Type == format.DataPageV2 {
			var err error
			min, max, err = normalizeTranscodedPageStatistics(header, c.columnType.Kind())
			if err != nil {
				return err
			}
			numRows = int64(header.DataPageHeaderV2.V.NumRows)
		}

		body, err := transcoder.transcode(header, transcoder.source)
		if err != nil {
			return fmt.Errorf("transcoding source page %d: %w", pageIndex, err)
		}
		header.CompressedPageSize = int32(len(body))
		header.CRC = int32(crc32.ChecksumIEEE(body))

		headerBytes, err := encodeRawPageHeader(header)
		if err != nil {
			return fmt.Errorf("encoding transcoded page header %d: %w", pageIndex, err)
		}

		pageSize := int64(len(headerBytes) + len(body))
		totalCompressed += pageSize
		totalUncompressed += int64(len(headerBytes)) + int64(header.UncompressedPageSize)

		switch header.Type {
		case format.DictionaryPage:
			if dictionarySeen || dataPageIndex != 0 {
				return fmt.Errorf("unexpected dictionary page %d: %w", pageIndex, ErrCorrupted)
			}
			transcoded.dictionary = append(transcoded.dictionary[:0], headerBytes...)
			transcoded.dictionary = append(transcoded.dictionary, body...)
			dictionarySeen = true
			c.columnChunk.MetaData.EncodingStats = addPageEncodingStats(c.columnChunk.MetaData.EncodingStats, format.PageEncodingStats{
				PageType: header.Type,
				Encoding: header.DictionaryPageHeader.V.Encoding,
				Count:    1,
			})

		case format.DataPageV2:
			if err := c.writePageTo(pageSize, func(output io.Writer) (int64, error) {
				written, err := output.Write(headerBytes)
				if err != nil {
					return int64(written), err
				}
				n, err := output.Write(body)
				return int64(written + n), err
			}); err != nil {
				return fmt.Errorf("staging transcoded page %d: %w", pageIndex, err)
			}
			v2 := &header.DataPageHeaderV2.V
			c.recordTranscodedPageStatistics(int64(v2.NumValues), int64(v2.NumNulls), min, max)
			c.offsetIndex.PageLocations = append(c.offsetIndex.PageLocations, format.PageLocation{
				Offset:             dataOffset,
				CompressedPageSize: int32(pageSize),
				FirstRowIndex:      firstRowIndex,
			})
			dataOffset += pageSize
			firstRowIndex += numRows
			dataPageIndex++
			c.columnChunk.MetaData.EncodingStats = addPageEncodingStats(c.columnChunk.MetaData.EncodingStats, format.PageEncodingStats{
				PageType: header.Type,
				Encoding: v2.Encoding,
				Count:    1,
			})

		default:
			return fmt.Errorf("unsupported source page type %s: %w", header.Type, errTranscodeFallback)
		}
	}

	if dataPageIndex == 0 || dictionarySeen != (c.dictionary != nil) {
		return fmt.Errorf("source page layout does not match destination encoding: %w", errTranscodeFallback)
	}
	c.transcoded = transcoded
	c.numRows = firstRowIndex
	c.columnChunk.MetaData.TotalUncompressedSize = totalUncompressed
	c.columnChunk.MetaData.TotalCompressedSize = totalCompressed
	transcodePathCounter.Add(1)
	return nil
}

func validateTranscodedPageChecksum(header *format.PageHeader, body []byte, src *FileColumnChunk) error {
	if header.CRC == 0 {
		return nil
	}
	want := uint32(header.CRC)
	got := crc32.ChecksumIEEE(body)
	if want != got {
		return fmt.Errorf("crc32 checksum mismatch in page of column %q: want=0x%08X got=0x%08X: %w",
			columnPathString(src.column.Path()), want, got, ErrCorrupted)
	}
	return nil
}

func validateTranscodedPage(header *format.PageHeader, dst *ColumnWriter) error {
	switch header.Type {
	case format.DictionaryPage:
		if dst.dictionary == nil || !header.DictionaryPageHeader.Valid || header.DictionaryPageHeader.V.Encoding != format.Plain {
			return fmt.Errorf("dictionary page does not match destination configuration: %w", errTranscodeFallback)
		}
		header.DictionaryPageHeader.V.IsSorted = false
	case format.DataPageV2:
		if dst.header.page.Type != format.DataPageV2 || !header.DataPageHeaderV2.Valid || header.DataPageHeaderV2.V.Encoding != dst.encoding.Encoding() {
			return fmt.Errorf("data page v2 does not match destination configuration: %w", errTranscodeFallback)
		}
		v2 := &header.DataPageHeaderV2.V
		if v2.NumNulls != 0 || v2.NumRows != v2.NumValues || v2.RepetitionLevelsByteLength != 0 || v2.DefinitionLevelsByteLength != 0 {
			return fmt.Errorf("data page v2 has levels or nulls: %w", errTranscodeFallback)
		}
	default:
		return fmt.Errorf("unsupported page type %s: %w", header.Type, errTranscodeFallback)
	}
	return nil
}

func normalizeTranscodedPageStatistics(header *format.PageHeader, kind Kind) (Value, Value, error) {
	v2 := &header.DataPageHeaderV2.V
	stats := v2.Statistics
	if stats.MinValue == nil || stats.MaxValue == nil {
		return Value{}, Value{}, fmt.Errorf("data page v2 statistics are missing bounds: %w", errTranscodeFallback)
	}
	min := kind.Value(stats.MinValue)
	max := kind.Value(stats.MaxValue)
	v2.Statistics = format.Statistics{
		Min:       stats.MinValue,
		Max:       stats.MaxValue,
		NullCount: int64(v2.NumNulls),
		MinValue:  stats.MinValue,
		MaxValue:  stats.MaxValue,
	}
	return min, max, nil
}

func (c *ColumnWriter) recordTranscodedPageStatistics(numValues, numNulls int64, min, max Value) {
	c.columnIndex.IndexPage(numValues, numNulls, min, max)
	c.columnChunk.MetaData.NumValues += numValues
	c.columnChunk.MetaData.Statistics.NullCount += numNulls

	if c.columnChunk.MetaData.Statistics.MaxValue == nil || c.columnType.Compare(max, c.columnType.Kind().Value(c.columnChunk.MetaData.Statistics.MaxValue)) > 0 {
		c.columnChunk.MetaData.Statistics.MaxValue = appendTranscodedStatistic(max, c.columnChunk.MetaData.Statistics.MaxValue[:0])
	}
	if c.columnChunk.MetaData.Statistics.MinValue == nil || c.columnType.Compare(min, c.columnType.Kind().Value(c.columnChunk.MetaData.Statistics.MinValue)) < 0 {
		c.columnChunk.MetaData.Statistics.MinValue = appendTranscodedStatistic(min, c.columnChunk.MetaData.Statistics.MinValue[:0])
	}
}

func appendTranscodedStatistic(value Value, dst []byte) []byte {
	if dst == nil && value.Kind() == ByteArray && len(value.ByteArray()) == 0 {
		dst = make([]byte, 0)
	}
	return value.AppendBytes(dst)
}

type pageTranscoder struct {
	sourceCodec      compress.Codec
	destinationCodec compress.Codec
	source           []byte
	decoded          []byte
	encoded          []byte
}

func (t *pageTranscoder) reset(source, destination compress.Codec) {
	t.sourceCodec = source
	t.destinationCodec = destination
	t.source = t.source[:0]
	t.decoded = t.decoded[:0]
	t.encoded = t.encoded[:0]
}

func (t *pageTranscoder) transcode(header *format.PageHeader, source []byte) ([]byte, error) {
	raw := source
	sourceCompressed := true
	if header.Type == format.DataPageV2 {
		sourceCompressed = DataPageHeaderV2{&header.DataPageHeaderV2.V}.IsCompressed()
	}
	if sourceCompressed && isCompressed(t.sourceCodec) {
		var err error
		t.decoded, err = t.sourceCodec.Decode(t.decoded[:0], source)
		if err != nil {
			return nil, err
		}
		raw = t.decoded
	}

	if !isCompressed(t.destinationCodec) {
		if header.Type == format.DataPageV2 {
			header.DataPageHeaderV2.V.IsCompressed = thrift.New(false)
		}
		return raw, nil
	}

	var err error
	t.encoded, err = t.destinationCodec.Encode(t.encoded[:0], raw)
	if err != nil {
		return nil, err
	}
	if header.Type == format.DataPageV2 {
		header.DataPageHeaderV2.V.IsCompressed = thrift.New(true)
	}
	return t.encoded, nil
}

func encodeRawPageHeader(header *format.PageHeader) ([]byte, error) {
	var buf bytes.Buffer
	protocol := thrift.CompactProtocol{}
	if err := thrift.NewEncoder(protocol.NewWriter(&buf)).Encode(header); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
