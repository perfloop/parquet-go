package parquet

import (
	"io"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/internal/memory"
)

const writerLevelBufferSize = 4096

// writerLevelsColumnBuffer is a write-only adapter for optional and repeated
// columns. It retains the payload in the regular physical column buffer, but
// encodes completed bounded level blocks as they arrive instead of retaining one
// byte per logical level until the page is flushed. The short tail remains raw
// so its hybrid encoding can span arbitrary WriteRowValues calls.
//
// ColumnWriter uses this adapter only for raw row-writing paths. Callers that
// start using the generic sparse writer are switched back to the regular
// optional or repeated buffers before they write more values.
type writerLevelsColumnBuffer struct {
	ColumnBuffer

	maxRepetitionLevel byte
	maxDefinitionLevel byte

	repetitions []byte
	definitions []byte

	repetitionTail memory.SliceBuffer[byte]
	definitionTail memory.SliceBuffer[byte]

	repetitionHistogram []int64
	definitionHistogram []int64

	page writerLevelsPage

	numRows   int64
	numValues int64
	numNulls  int64
}

func newWriterLevelsColumnBuffer(base ColumnBuffer, maxRepetitionLevel, maxDefinitionLevel byte) *writerLevelsColumnBuffer {
	column := &writerLevelsColumnBuffer{
		ColumnBuffer:       base,
		maxRepetitionLevel: maxRepetitionLevel,
		maxDefinitionLevel: maxDefinitionLevel,
	}
	if maxRepetitionLevel > 0 {
		column.repetitionHistogram = make([]int64, int(maxRepetitionLevel)+1)
	}
	if maxDefinitionLevel > 0 {
		column.definitionHistogram = make([]int64, int(maxDefinitionLevel)+1)
	}
	return column
}

func (column *writerLevelsColumnBuffer) Pages() Pages { return onePage(column.Page()) }

func (column *writerLevelsColumnBuffer) Page() Page {
	column.page = writerLevelsPage{
		page:                column.ColumnBuffer.Page(),
		column:              column,
		repetitions:         column.repetitions,
		definitions:         column.definitions,
		repetitionTail:      column.repetitionTail.Slice(),
		definitionTail:      column.definitionTail.Slice(),
		repetitionHistogram: column.repetitionHistogram,
		definitionHistogram: column.definitionHistogram,
		numRows:             column.numRows,
		numValues:           column.numValues,
		numNulls:            column.numNulls,
	}
	return &column.page
}

func (column *writerLevelsColumnBuffer) Reset() {
	column.ColumnBuffer.Reset()
	column.page = writerLevelsPage{}
	column.repetitions = column.repetitions[:0]
	column.definitions = column.definitions[:0]
	column.repetitionTail.Reset()
	column.definitionTail.Reset()
	clear(column.repetitionHistogram)
	clear(column.definitionHistogram)
	column.numRows = 0
	column.numValues = 0
	column.numNulls = 0
}

func (column *writerLevelsColumnBuffer) Size() int64 {
	size := column.ColumnBuffer.Size() + column.numValues
	if column.maxRepetitionLevel > 0 {
		return size + column.numValues + 8*column.numRows
	}
	return size + 4*column.numRows
}

func (column *writerLevelsColumnBuffer) Len() int { return int(column.numRows) }

func (column *writerLevelsColumnBuffer) WriteValues(values []Value) (int, error) {
	for i := 0; i < len(values); {
		for i < len(values) && values[i].definitionLevel != column.maxDefinitionLevel {
			i++
		}
		start := i
		for i < len(values) && values[i].definitionLevel == column.maxDefinitionLevel {
			i++
		}
		if start == i {
			continue
		}
		n, err := column.ColumnBuffer.WriteValues(values[start:i])
		if err != nil {
			return start + n, err
		}
		if n != i-start {
			return start + n, io.ErrShortWrite
		}
	}
	if err := column.appendLevels(values); err != nil {
		return 0, err
	}
	return len(values), nil
}

func (column *writerLevelsColumnBuffer) appendLevels(values []Value) error {
	for _, value := range values {
		if column.maxDefinitionLevel > 0 {
			column.definitionHistogram[value.definitionLevel]++
			column.definitionTail.Append(value.definitionLevel)
		}
		if value.definitionLevel != column.maxDefinitionLevel {
			column.numNulls++
		}
		if column.maxRepetitionLevel > 0 {
			column.repetitionHistogram[value.repetitionLevel]++
			column.repetitionTail.Append(value.repetitionLevel)
			if value.repetitionLevel == 0 {
				column.numRows++
			}
		} else {
			column.numRows++
		}
		column.numValues++

		if column.repetitionTail.Len() == writerLevelBufferSize {
			if err := column.flushRepetitionTail(); err != nil {
				return err
			}
		}
		if column.definitionTail.Len() == writerLevelBufferSize {
			if err := column.flushDefinitionTail(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (column *writerLevelsColumnBuffer) flushRepetitionTail() error {
	var err error
	column.repetitions, err = appendEncodedLevels(
		column.repetitions,
		column.repetitionTail.Slice(),
		column.maxRepetitionLevel,
	)
	if err == nil {
		column.repetitionTail.Resize(0)
	}
	return err
}

func (column *writerLevelsColumnBuffer) flushDefinitionTail() error {
	var err error
	column.definitions, err = appendEncodedLevels(
		column.definitions,
		column.definitionTail.Slice(),
		column.maxDefinitionLevel,
	)
	if err == nil {
		column.definitionTail.Resize(0)
	}
	return err
}

type writerLevelsPage struct {
	page   Page
	column *writerLevelsColumnBuffer

	repetitions []byte
	definitions []byte

	repetitionTail []byte
	definitionTail []byte

	repetitionHistogram []int64
	definitionHistogram []int64

	numRows   int64
	numValues int64
	numNulls  int64
}

func (page *writerLevelsPage) Type() Type             { return page.page.Type() }
func (page *writerLevelsPage) Column() int            { return page.page.Column() }
func (page *writerLevelsPage) Dictionary() Dictionary { return page.page.Dictionary() }
func (page *writerLevelsPage) NumRows() int64         { return page.numRows }
func (page *writerLevelsPage) NumValues() int64       { return page.numValues }
func (page *writerLevelsPage) NumNulls() int64        { return page.numNulls }
func (page *writerLevelsPage) Bounds() (Value, Value, bool) {
	return page.page.Bounds()
}
func (page *writerLevelsPage) Size() int64 {
	size := page.page.Size() + page.numValues
	if page.column.maxRepetitionLevel > 0 {
		size += page.numValues
	}
	return size
}
func (page *writerLevelsPage) RepetitionLevels() []byte { return nil }
func (page *writerLevelsPage) DefinitionLevels() []byte { return nil }
func (page *writerLevelsPage) Data() encoding.Values    { return page.page.Data() }
func (page *writerLevelsPage) Values() ValueReader      { return page.page.Values() }

func (page *writerLevelsPage) Slice(i, j int64) Page {
	if i != 0 || j != page.numRows {
		panic(errPageBoundsOutOfRange(i, j, page.numRows))
	}
	return page
}

func (page *writerLevelsPage) copyEncodedLevels(buffers *writerBuffers) error {
	buffers.repetitions = append(buffers.repetitions[:0], page.repetitions...)
	if len(page.repetitionTail) > 0 {
		var err error
		buffers.repetitions, err = appendEncodedLevels(
			buffers.repetitions,
			page.repetitionTail,
			page.column.maxRepetitionLevel,
		)
		if err != nil {
			return err
		}
	}

	buffers.definitions = append(buffers.definitions[:0], page.definitions...)
	if len(page.definitionTail) > 0 {
		var err error
		buffers.definitions, err = appendEncodedLevels(
			buffers.definitions,
			page.definitionTail,
			page.column.maxDefinitionLevel,
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func appendWriterLevelHistogram(columnHistogram, pageHistograms, counts []int64) []int64 {
	start := len(pageHistograms)
	pageHistograms = append(pageHistograms, make([]int64, len(counts))...)
	for level, count := range counts {
		columnHistogram[level] += count
		pageHistograms[start+level] = count
	}
	return pageHistograms
}

var _ ColumnBuffer = (*writerLevelsColumnBuffer)(nil)
var _ Page = (*writerLevelsPage)(nil)
