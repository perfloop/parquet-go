package parquet

import (
	"bytes"
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

// disableWriteTranscode forces codec-mismatched chunks onto the L3 value path.
// It exists only so tests can compare the two implementations.
var disableWriteTranscode bool

var pageTranscoders memory.Pool[pageTranscoder]

// transcodedChunk holds the metadata and dictionary bytes for a chunk whose
// data pages have been recompressed into its ColumnWriter page buffer.
type transcodedChunk struct {
	dictionary  []byte
	columnIndex format.ColumnIndex
	sizeStats   format.SizeStatistics
}

// columnChunkIsTranscodable reports whether src can be rewritten without
// decoding values. L2 preserves the page encoding, page version, statistics,
// and page index, but changes the compression codec and rebuilds page offsets.
func columnChunkIsTranscodable(dst *ColumnWriter, src *FileColumnChunk) bool {
	if src.decryptionKey != nil || dst.encKey != nil {
		return false
	}

	meta := &src.chunk.MetaData
	if meta.Type != format.Type(dst.columnType.Kind()) {
		return false
	}
	if meta.Codec == dst.compression.CompressionCodec() {
		return false
	}
	if dst.columnFilter != nil {
		return false
	}
	if src.chunk.ColumnIndexOffset == 0 || src.chunk.OffsetIndexOffset == 0 {
		return false
	}
	if dst.dictionary != nil && meta.DictionaryPageOffset == 0 {
		return false
	}
	return encodingStatsMatch(meta.EncodingStats, dst)
}

// loadTranscodedChunk reads the source's raw pages, changes only their
// compression, and stages the rewritten data pages in c. It deliberately does
// not construct Page values: source page statistics and indexes remain valid
// because the encoded content is unchanged.
func (c *ColumnWriter) loadTranscodedChunk(src *FileColumnChunk) error {
	meta := &src.chunk.MetaData

	columnIndex, err := src.ColumnIndex()
	if err != nil {
		return fmt.Errorf("reading source column index: %w", err)
	}
	sourceColumnIndex, ok := columnIndex.(*FileColumnIndex)
	if !ok {
		return fmt.Errorf("source column index has unexpected type %T", columnIndex)
	}

	offsetIndex, err := src.OffsetIndex()
	if err != nil {
		return fmt.Errorf("reading source offset index: %w", err)
	}
	sourceOffsetIndex, ok := offsetIndex.(*FileOffsetIndex)
	if !ok {
		return fmt.Errorf("source offset index has unexpected type %T", offsetIndex)
	}
	if sourceColumnIndex.NumPages() != sourceOffsetIndex.NumPages() {
		return fmt.Errorf("source column and offset indexes disagree on page count")
	}

	transcoded := &transcodedChunk{
		columnIndex: cloneColumnIndex(sourceColumnIndex.index),
		sizeStats:   cloneSizeStatistics(meta.SizeStatistics),
	}

	baseOffset := meta.DataPageOffset
	if meta.DictionaryPageOffset != 0 {
		baseOffset = meta.DictionaryPageOffset
	}
	section := io.NewSectionReader(src.file.reader, baseOffset, meta.TotalCompressedSize)
	rbuf, rbufpool := getBufioReader(section, src.file.config.ReadBufferSize)
	defer putBufioReader(rbuf, rbufpool)

	protocol := thrift.CompactProtocol{}
	decoder := thrift.NewDecoder(protocol.NewReader(rbuf))
	sourceCodec := LookupCompressionCodec(meta.Codec)
	transcoder := pageTranscoders.Get(
		func() *pageTranscoder {
			t := new(pageTranscoder)
			t.reset(sourceCodec, c.compression)
			return t
		},
		func(t *pageTranscoder) { t.reset(sourceCodec, c.compression) },
	)
	defer pageTranscoders.Put(transcoder)

	expectedDataPages := len(sourceOffsetIndex.index.PageLocations)
	expectedPages := expectedDataPages
	if meta.DictionaryPageOffset != 0 {
		expectedPages++
	}

	var (
		dataOffset        int64
		totalCompressed   int64
		totalUncompressed int64
		dataPageIndex     int
		dictionarySeen    bool
	)

	for pageIndex := range expectedPages {
		header := new(format.PageHeader)
		if err := decoder.Decode(header); err != nil {
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

		if err := validateTranscodedPage(header, c); err != nil {
			return fmt.Errorf("validating source page %d: %w", pageIndex, err)
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
			if dictionarySeen || dataPageIndex != 0 || meta.DictionaryPageOffset == 0 {
				return fmt.Errorf("unexpected dictionary page %d", pageIndex)
			}
			transcoded.dictionary = append(transcoded.dictionary[:0], headerBytes...)
			transcoded.dictionary = append(transcoded.dictionary, body...)
			dictionarySeen = true

		case format.DataPage, format.DataPageV2:
			if dataPageIndex >= expectedDataPages {
				return fmt.Errorf("unexpected data page %d", pageIndex)
			}
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
			location := sourceOffsetIndex.index.PageLocations[dataPageIndex]
			c.offsetIndex.PageLocations = append(c.offsetIndex.PageLocations, format.PageLocation{
				Offset:             dataOffset,
				CompressedPageSize: int32(pageSize),
				FirstRowIndex:      location.FirstRowIndex,
			})
			dataOffset += pageSize
			dataPageIndex++

		default:
			return fmt.Errorf("unsupported source page type %s", header.Type)
		}
	}

	if dataPageIndex != expectedDataPages {
		return fmt.Errorf("source has %d data pages, expected %d", dataPageIndex, expectedDataPages)
	}
	if _, err := rbuf.ReadByte(); err != io.EOF {
		if err != nil {
			return fmt.Errorf("checking source page data boundary: %w", err)
		}
		return fmt.Errorf("source page data contains bytes after the final indexed page")
	}
	if dictionarySeen != (meta.DictionaryPageOffset != 0) {
		return fmt.Errorf("source dictionary page presence does not match metadata")
	}

	c.transcoded = transcoded
	c.numRows = src.rowGroup.NumRows
	c.columnChunk.MetaData.NumValues = meta.NumValues
	c.columnChunk.MetaData.TotalUncompressedSize = totalUncompressed
	c.columnChunk.MetaData.TotalCompressedSize = totalCompressed
	c.columnChunk.MetaData.Statistics = cloneStatistics(meta.Statistics)
	c.columnChunk.MetaData.EncodingStats = append(c.columnChunk.MetaData.EncodingStats[:0], meta.EncodingStats...)
	transcodePathCounter.Add(1)
	return nil
}

func validateTranscodedPage(header *format.PageHeader, dst *ColumnWriter) error {
	switch header.Type {
	case format.DictionaryPage:
		if dst.dictionary == nil || !header.DictionaryPageHeader.Valid || header.DictionaryPageHeader.V.Encoding != format.Plain {
			return fmt.Errorf("dictionary page does not match destination configuration")
		}
	case format.DataPage:
		if dst.header.page.Type != format.DataPage || !header.DataPageHeader.Valid || header.DataPageHeader.V.Encoding != dst.encoding.Encoding() {
			return fmt.Errorf("data page v1 does not match destination configuration")
		}
	case format.DataPageV2:
		if dst.header.page.Type != format.DataPageV2 || !header.DataPageHeaderV2.Valid || header.DataPageHeaderV2.V.Encoding != dst.encoding.Encoding() {
			return fmt.Errorf("data page v2 does not match destination configuration")
		}
	default:
		return fmt.Errorf("unsupported page type %s", header.Type)
	}
	return nil
}

type pageTranscoder struct {
	sourceCodec      compress.Codec
	destinationCodec compress.Codec
	source           []byte
	decoded          []byte
	encoded          []byte
	output           []byte
}

func (t *pageTranscoder) reset(source, destination compress.Codec) {
	t.sourceCodec = source
	t.destinationCodec = destination
	t.source = t.source[:0]
	t.decoded = t.decoded[:0]
	t.encoded = t.encoded[:0]
	t.output = t.output[:0]
}

func (t *pageTranscoder) transcode(header *format.PageHeader, source []byte) ([]byte, error) {
	payload := source
	levels := []byte(nil)
	compressed := true

	switch header.Type {
	case format.DictionaryPage:
		if !header.DictionaryPageHeader.Valid {
			return nil, ErrMissingPageHeader
		}
	case format.DataPage:
		if !header.DataPageHeader.Valid {
			return nil, ErrMissingPageHeader
		}
	case format.DataPageV2:
		if !header.DataPageHeaderV2.Valid {
			return nil, ErrMissingPageHeader
		}
		v2 := DataPageHeaderV2{&header.DataPageHeaderV2.V}
		levelsLen := v2.RepetitionLevelsByteLength() + v2.DefinitionLevelsByteLength()
		if levelsLen > int64(len(source)) {
			return nil, io.ErrUnexpectedEOF
		}
		levels = source[:levelsLen]
		payload = source[levelsLen:]
		compressed = v2.IsCompressed()
	default:
		return nil, fmt.Errorf("unsupported page type %s", header.Type)
	}

	raw := payload
	if compressed && isCompressed(t.sourceCodec) {
		var err error
		t.decoded, err = t.sourceCodec.Decode(t.decoded[:0], payload)
		if err != nil {
			return nil, err
		}
		raw = t.decoded
	}

	if isCompressed(t.destinationCodec) {
		var err error
		t.encoded, err = t.destinationCodec.Encode(t.encoded[:0], raw)
		if err != nil {
			return nil, err
		}
		payload = t.encoded
	} else {
		payload = raw
	}

	if header.Type == format.DataPageV2 {
		header.DataPageHeaderV2.V.IsCompressed = thrift.New(isCompressed(t.destinationCodec))
		t.output = append(t.output[:0], levels...)
		t.output = append(t.output, payload...)
		return t.output, nil
	}

	t.output = append(t.output[:0], payload...)
	return t.output, nil
}

func encodeRawPageHeader(header *format.PageHeader) ([]byte, error) {
	var buf bytes.Buffer
	protocol := thrift.CompactProtocol{}
	if err := thrift.NewEncoder(protocol.NewWriter(&buf)).Encode(header); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
