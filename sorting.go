package parquet

import (
	"errors"
	"io"
	"slices"
	"sort"
)

// SortingWriter is a type similar to GenericWriter but it ensures that rows
// are sorted according to the sorting columns configured on the writer.
//
// The writer accumulates rows in an in-memory buffer which is sorted when it
// reaches the target number of rows, then written to a temporary row group.
// When the writer is flushed or closed, the temporary row groups are merged
// into a row group in the output file, ensuring that rows remain sorted in the
// final row group. Required INT64 layouts using the built-in in-memory pool and
// no compression retain compact run values instead of materializing temporary
// row groups.
//
// Encoded and compressed temporary row groups hold a lot less memory than if
// all rows were retained in memory. Sorting then merging rows chunks also tends
// to be a lot more efficient than sorting all rows in memory as it results in
// better CPU cache utilization since sorting multi-megabyte arrays causes a lot
// of cache misses since the data set cannot be held in CPU caches.
type SortingWriter[T any] struct {
	rowbuf            *RowBuffer[T]
	writer            *GenericWriter[T]
	output            *GenericWriter[T]
	buffer            io.ReadWriteSeeker
	inMemoryInt64Runs *inMemoryInt64Runs
	maxRows           int64
	numRows           int64
	sorting           SortingConfig
	dedupe            dedupe
}

// NewSortingWriter constructs a new sorting writer which writes a parquet file
// where rows of each row group are ordered according to the sorting columns
// configured on the writer.
//
// The sortRowCount argument defines the target number of rows that will be
// sorted in memory before being written to temporary row groups. The greater
// this value the more memory is needed to buffer rows in memory. Choosing a
// value that is too small limits the maximum number of rows that can exist in
// the output file since the writer cannot create more than 32K temporary row
// groups to hold the sorted row chunks.
func NewSortingWriter[T any](output io.Writer, sortRowCount int64, options ...WriterOption) *SortingWriter[T] {
	config, err := NewWriterConfig(options...)
	if err != nil {
		panic(err)
	}
	w := &SortingWriter[T]{
		rowbuf: NewRowBuffer[T](&RowGroupConfig{
			Schema:  config.Schema,
			Sorting: config.Sorting,
		}),
		writer: NewGenericWriter[T](io.Discard, &WriterConfig{
			CreatedBy:            config.CreatedBy,
			ColumnPageBuffers:    config.ColumnPageBuffers,
			ColumnIndexSizeLimit: config.ColumnIndexSizeLimit,
			PageBufferSize:       config.PageBufferSize,
			WriteBufferSize:      config.WriteBufferSize,
			DataPageVersion:      config.DataPageVersion,
			Schema:               config.Schema,
			Compression:          config.Compression,
			Sorting:              config.Sorting,
			Encodings:            config.Encodings,
		}),
		output:  NewGenericWriter[T](output, config),
		maxRows: sortRowCount,
		sorting: config.Sorting,
	}
	if config.Compression == nil {
		w.inMemoryInt64Runs = newInMemoryInt64Runs(
			w.rowbuf.Schema(),
			config.Sorting,
			config.Sorting.SortingBuffers,
		)
	}
	return w
}

func (w *SortingWriter[T]) Close() error {
	if err := w.Flush(); err != nil {
		return err
	}

	defer w.resetSortingBuffer()

	if w.numRows == 0 {
		return w.output.Close()
	}

	if w.inMemoryInt64Runs != nil {
		if err := w.inMemoryInt64Runs.writeTo(w.output, w.rowbuf.compare, w.sorting.DropDuplicatedRows); err != nil {
			return err
		}
	} else {
		if err := w.writer.Close(); err != nil {
			return err
		}

		size, err := w.buffer.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		f, err := OpenFile(newReaderAt(w.buffer), size,
			&FileConfig{
				SkipPageIndex:    true,
				SkipBloomFilters: true,
				ReadBufferSize:   defaultReadBufferSize,
			},
		)
		if err != nil {
			return err
		}

		m, err := MergeRowGroups(f.RowGroups(),
			&RowGroupConfig{
				Schema:  w.Schema(),
				Sorting: w.sorting,
			},
		)
		if err != nil {
			return err
		}

		// Writing the merged row group (rather than copying its rows) lets the
		// writer use its chunk-level fast paths when the sorted chunks do not
		// overlap: verbatim column chunk copies and column-oriented packing toward
		// MaxRowsPerRowGroup. Overlapping chunks fall back to the row-oriented heap
		// merge, and the merge itself applies DropDuplicatedRows when configured.
		if _, err := w.output.WriteRowGroup(m); err != nil {
			return err
		}
	}

	if err := w.output.Flush(); err != nil {
		return err
	}

	return w.output.Close()
}

// Flush sorts any buffered rows and writes them to temporary storage.
// This can be called multiple times to manage memory usage.
// The actual merge and write to output happens on Close.
func (w *SortingWriter[T]) Flush() error {
	return w.sortAndWriteBufferedRows()
}

func (w *SortingWriter[T]) Reset(output io.Writer) {
	w.output.Reset(output)
	w.rowbuf.Reset()
	w.resetSortingBuffer()
}

func (w *SortingWriter[T]) resetSortingBuffer() {
	w.writer.Reset(io.Discard)
	w.numRows = 0

	if w.inMemoryInt64Runs != nil {
		w.inMemoryInt64Runs.reset()
	}

	if w.buffer != nil {
		w.sorting.SortingBuffers.PutBuffer(w.buffer)
		w.buffer = nil
	}
}

// newInMemoryInt64Runs limits the in-memory run store to a fixed-width layout,
// where copying values into compact run slices has predictable memory use.
func newInMemoryInt64Runs(schema *Schema, sorting SortingConfig, pool BufferPool) *inMemoryInt64Runs {
	if _, ok := pool.(*memoryBufferPool); !ok ||
		len(sorting.SortingColumns) != 1 ||
		sorting.SortingColumns[0].Descending() {
		return nil
	}

	if _, ok := schema.Lookup(sorting.SortingColumns[0].Path()...); !ok {
		return nil
	}

	columns := schema.Columns()
	for _, path := range columns {
		column, ok := schema.Lookup(path...)
		if !ok ||
			column.Node.Type().Kind() != Int64 ||
			column.MaxDefinitionLevel != 0 ||
			column.MaxRepetitionLevel != 0 {
			return nil
		}
	}

	return &inMemoryInt64Runs{columns: len(columns)}
}

var errInvalidInMemoryInt64Row = errors.New("invalid row for in-memory INT64 sorting run")

type inMemoryInt64Runs struct {
	columns int
	runs    [][]int64
}

func (r *inMemoryInt64Runs) validateRows(rows []Row) error {
	for _, row := range rows {
		if len(row) != r.columns {
			return errInvalidInMemoryInt64Row
		}
		for column, value := range row {
			if value.Column() != column ||
				value.Kind() != Int64 ||
				value.RepetitionLevel() != 0 ||
				value.DefinitionLevel() != 0 {
				return errInvalidInMemoryInt64Row
			}
		}
	}
	return nil
}

func (r *inMemoryInt64Runs) appendRows(rows []Row) {
	values := make([]int64, len(rows)*r.columns)
	// validateRows establishes positional identity before sorting, which keeps
	// the compact store and RowBuffer's comparator in agreement.
	for i, row := range rows {
		for column, value := range row {
			values[i*r.columns+column] = value.Int64()
		}
	}
	r.runs = append(r.runs, values)
}

func (r *inMemoryInt64Runs) reset() {
	r.runs = nil
}

func (r *inMemoryInt64Runs) writeTo(dst RowWriter, compare func(Row, Row) int, dropDuplicatedRows bool) error {
	readers := make([]RowReader, len(r.runs))
	for i := range r.runs {
		readers[i] = &inMemoryInt64RunReader{
			values:  r.runs[i],
			columns: r.columns,
		}
	}

	var reader RowReader = MergeRowReaders(readers, compare)
	if dropDuplicatedRows {
		reader = DedupeRowReader(reader, compare)
	}
	_, err := CopyRows(dst, reader)
	return err
}

type inMemoryInt64RunReader struct {
	values  []int64
	columns int
	offset  int
}

func (r *inMemoryInt64RunReader) ReadRows(rows []Row) (int, error) {
	n := min(len(rows), len(r.values)/r.columns-r.offset)
	for i := range n {
		values := rows[i][:0]
		for column := range r.columns {
			value := r.values[(r.offset+i)*r.columns+column]
			values = append(values, Int64Value(value).Level(0, 0, column))
		}
		rows[i] = values
	}
	r.offset += n
	if r.offset == len(r.values)/r.columns {
		return n, io.EOF
	}
	return n, nil
}

func (w *SortingWriter[T]) Write(rows []T) (int, error) {
	return w.writeRows(len(rows), func(i, j int) (int, error) { return w.rowbuf.Write(rows[i:j]) })
}

func (w *SortingWriter[T]) WriteRows(rows []Row) (int, error) {
	if w.inMemoryInt64Runs != nil {
		if err := w.inMemoryInt64Runs.validateRows(rows); err != nil {
			return 0, err
		}
	}
	return w.writeRows(len(rows), func(i, j int) (int, error) { return w.rowbuf.WriteRows(rows[i:j]) })
}

func (w *SortingWriter[T]) writeRows(numRows int, writeRows func(i, j int) (int, error)) (int, error) {
	wn := 0

	for wn < numRows {
		if w.rowbuf.NumRows() >= w.maxRows {
			if err := w.sortAndWriteBufferedRows(); err != nil {
				return wn, err
			}
		}

		n := int(w.maxRows - w.rowbuf.NumRows())
		n += wn
		if n > numRows {
			n = numRows
		}

		n, err := writeRows(wn, n)
		wn += n

		if err != nil {
			return wn, err
		}
	}

	return wn, nil
}

func (w *SortingWriter[T]) SetKeyValueMetadata(key, value string) {
	w.output.SetKeyValueMetadata(key, value)
}

func (w *SortingWriter[T]) Schema() *Schema {
	return w.output.Schema()
}

func (w *SortingWriter[T]) sortAndWriteBufferedRows() error {
	if w.rowbuf.Len() == 0 {
		return nil
	}

	sort.Sort(w.rowbuf)

	if w.sorting.DropDuplicatedRows {
		w.rowbuf.rows = w.rowbuf.rows[:w.dedupe.deduplicate(w.rowbuf.rows, w.rowbuf.compare)]
		defer w.dedupe.reset()
	}

	if w.inMemoryInt64Runs != nil {
		if len(w.inMemoryInt64Runs.runs) == MaxRowGroups {
			w.rowbuf.Reset()
			return ErrTooManyRowGroups
		}

		w.inMemoryInt64Runs.appendRows(w.rowbuf.rows)
		w.numRows += w.rowbuf.NumRows()
		w.rowbuf.Reset()
		return nil
	}

	defer w.rowbuf.Reset()
	rows := w.rowbuf.Rows()
	defer rows.Close()

	if w.buffer == nil {
		w.buffer = w.sorting.SortingBuffers.GetBuffer()
		w.writer.Reset(w.buffer)
	}

	n, err := CopyRows(w.writer, rows)
	if err != nil {
		return err
	}

	if err := w.writer.Flush(); err != nil {
		return err
	}

	w.numRows += n
	return nil
}

// File returns a FileView of the written parquet file.
// Only available after Close is called.
func (w *SortingWriter[T]) File() FileView {
	return w.output.File()
}

// EqualSortingColumns compares two slices of sorting columns for equality.
//
// Two sorting column slices are considered equal if they have the same length
// and each corresponding pair of sorting columns is equal. Two sorting columns
// are equal if they have:
//   - The same column path (including nested field paths)
//   - The same sort direction (ascending or descending)
//   - The same nulls handling (nulls first or nulls last)
//
// The comparison is order-sensitive, meaning that [A, B] is not equal to [B, A].
// Both nil and empty slices are considered equal.
//
// This function is useful for:
//   - Validating that merged row groups maintain expected sorting
//   - Comparing sorting configurations between different row groups
//   - Testing sorting column propagation in merge operations
//
// Example:
//
//	cols1 := []SortingColumn{Ascending("name"), Descending("age")}
//	cols2 := []SortingColumn{Ascending("name"), Descending("age")}
//	equal := EqualSortingColumns(cols1, cols2) // returns true
//
//	cols3 := []SortingColumn{Descending("age"), Ascending("name")}
//	equal = EqualSortingColumns(cols1, cols3) // returns false (different order)
//
//	cols4 := []SortingColumn{Ascending("name"), Ascending("age")}
//	equal = EqualSortingColumns(cols1, cols4) // returns false (different direction)
func EqualSortingColumns(a, b []SortingColumn) bool {
	return len(a) == len(b) && slices.EqualFunc(a, b, equalSortingColumn)
}

// equalSortingColumn compares two individual sorting columns for equality.
// Two sorting columns are equal if they have the same path, direction, and nulls handling.
func equalSortingColumn(a, b SortingColumn) bool {
	return slices.Equal(a.Path(), b.Path()) && a.Descending() == b.Descending() && a.NullsFirst() == b.NullsFirst()
}
