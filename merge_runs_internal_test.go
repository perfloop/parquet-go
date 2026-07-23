package parquet

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"slices"
	"testing"
)

// sliceRowReader yields pre-built rows, cut into arbitrary batch sizes.
type sliceRowReader struct {
	rows []Row
	off  int
}

func (r *sliceRowReader) ReadRows(rows []Row) (int, error) {
	if r.off >= len(r.rows) {
		return 0, io.EOF
	}
	n := min(len(rows), len(r.rows)-r.off)
	for i := range n {
		rows[i] = append(rows[i][:0], r.rows[r.off+i]...)
	}
	r.off += n
	return n, nil
}

// referenceMergeRows drains a mergedRowReader built without run detection by
// forcing the streak below the threshold before every read. It preserves the
// exact per-row replay behavior, serving as the oracle for run mode.
func drainMerged(t *testing.T, m RowReader) []Row {
	t.Helper()
	var out []Row
	buf := make([]Row, 7) // odd batch size to exercise batch boundaries
	for {
		n, err := m.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, slices.Clone(row))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

// TestPageRangeRowsClose keeps the Rows lifecycle of a qualifying direct path
// consistent with the RowSeeker contract.
func TestPageRangeRowsClose(t *testing.T) {
	rowGroups := []RowGroup{
		newPageIndexMergeTestRowGroup(t, 0, 0),
		newPageIndexMergeTestRowGroup(t, 500, 1),
	}
	merged, err := MergeRowGroups(rowGroups, SortingRowGroupConfig(SortingColumns(Ascending("key"))))
	if err != nil {
		t.Fatal(err)
	}
	pageMerge, ok := merged.(*mergedRowGroup)
	if !ok {
		t.Fatalf("merged row group type: %T", merged)
	}
	rows, ok := newPageRangeRows(pageMerge)
	if !ok {
		t.Fatal("page-range direct path unavailable")
	}
	if err := rows.Close(); err != nil {
		t.Fatal(err)
	}
	for _, rowIndex := range []int64{0, 1} {
		if err := rows.SeekToRow(rowIndex); err != io.ErrClosedPipe {
			t.Fatalf("seek to row %d after close: got %v, want %v", rowIndex, err, io.ErrClosedPipe)
		}
	}
	if n, err := rows.ReadRows(make([]Row, 1)); n != 0 || err != io.EOF {
		t.Fatalf("read after close: got (%d, %v), want (0, %v)", n, err, io.EOF)
	}
}

type pageIndexMergeTestRow struct {
	Key    int64 `parquet:"key"`
	Source int32 `parquet:"source"`
}

type withoutPageIndexRowGroup struct{ RowGroup }

func (g withoutPageIndexRowGroup) ColumnChunks() []ColumnChunk {
	chunks := append([]ColumnChunk(nil), g.RowGroup.ColumnChunks()...)
	chunks[0] = withoutPageIndexColumnChunk{ColumnChunk: chunks[0]}
	return chunks
}

type withoutPageIndexColumnChunk struct{ ColumnChunk }

func (withoutPageIndexColumnChunk) ColumnIndex() (ColumnIndex, error) {
	return nil, ErrMissingColumnIndex
}

// rowsDecoratingRowGroup retains the wrapped file metadata but substitutes a
// different, still source-sorted Rows stream. It models a legal application
// RowGroup decorator whose ColumnChunks alone cannot authorize direct output.
type rowsDecoratingRowGroup struct {
	RowGroup
	rows RowGroup
}

func (g rowsDecoratingRowGroup) Rows() Rows { return g.rows.Rows() }

func TestPageIndexMergeRejectsRowsDecorator(t *testing.T) {
	metadata := pageIndexMergeTestRowGroups(t)
	assertPageIndexPlanHasDirectComponent(t, metadata)

	decorated := rowsDecoratingRowGroup{
		RowGroup: metadata[1],
		rows: newPageIndexMergeTestRowGroupWithKeys(t,
			append(int64Range(15, 25), int64Range(30, 40)...), 1),
	}
	if chunkTransparentRowGroup(decorated) {
		t.Fatal("Rows decorator unexpectedly marked chunk-transparent")
	}

	options := []RowGroupOption{SortingRowGroupConfig(SortingColumns(Ascending("key")))}
	got := mergeRowsForTest(t, []RowGroup{metadata[0], decorated}, options)
	want := mergeRowsForTest(t, []RowGroup{
		withoutPageIndexRowGroup{RowGroup: metadata[0]},
		decorated,
	}, options)
	if len(got) != len(want) {
		t.Fatalf("row count: got %d, want %d", len(got), len(want))
	}
	for i := range got {
		if !got[i].Equal(want[i]) {
			t.Fatalf("row %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func TestPageIndexMergeRejectsForgedBounds(t *testing.T) {
	for _, dropDuplicatedRows := range []bool{false, true} {
		t.Run(fmt.Sprintf("drop_duplicates=%t", dropDuplicatedRows), func(t *testing.T) {
			forged := pageIndexMergeTestRowGroups(t)
			forgeFirstPageMinimum(t, forged)
			assertForgedBoundsNeedRowMerge(t, forged)

			options := []RowGroupOption{
				SortingRowGroupConfig(
					SortingColumns(Ascending("key")),
					DropDuplicatedRows(dropDuplicatedRows),
				),
			}
			got := mergeRowsForTest(t, forged, options)

			control := pageIndexMergeTestRowGroups(t)
			control[0] = withoutPageIndexRowGroup{RowGroup: control[0]}
			want := mergeRowsForTest(t, control, options)
			if len(got) != len(want) {
				t.Fatalf("row count: got %d, want %d", len(got), len(want))
			}
			for i := range got {
				if !got[i].Equal(want[i]) {
					t.Fatalf("row %d: got %v, want %v", i, got[i], want[i])
				}
			}
		})
	}
}

func pageIndexMergeTestRowGroups(t *testing.T) []RowGroup {
	t.Helper()
	return []RowGroup{
		newPageIndexMergeTestRowGroupWithKeys(t, append(int64Range(0, 10), int64Range(20, 30)...), 0),
		newPageIndexMergeTestRowGroupWithKeys(t, append(int64Range(5, 15), int64Range(30, 40)...), 1),
	}
}

func int64Range(start, end int64) []int64 {
	values := make([]int64, end-start)
	for i := range values {
		values[i] = start + int64(i)
	}
	return values
}

func newPageIndexMergeTestRowGroup(t *testing.T, start int64, source int32) RowGroup {
	t.Helper()
	return newPageIndexMergeTestRowGroupWithKeys(t, int64Range(start, start+512), source)
}

func newPageIndexMergeTestRowGroupWithKeys(t *testing.T, keys []int64, source int32) RowGroup {
	t.Helper()
	var data bytes.Buffer
	writer := NewGenericWriter[pageIndexMergeTestRow](
		&data,
		PageBufferSize(80),
		ColumnIndexSizeLimit(func([]string) int { return 1 << 20 }),
		SortingWriterConfig(SortingColumns(Ascending("key"))),
	)
	for _, key := range keys {
		if _, err := writer.Write([]pageIndexMergeTestRow{{Key: key, Source: source}}); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	reader := bytes.NewReader(data.Bytes())
	file, err := OpenFile(reader, reader.Size())
	if err != nil {
		t.Fatal(err)
	}
	return file.RowGroups()[0]
}

func assertPageIndexPlanHasDirectComponent(t *testing.T, rowGroups []RowGroup) {
	t.Helper()
	sortingColumn := Ascending("key")
	left, columnType, ok := pageMergeRangesOf(rowGroups[0], rowGroups[0].Schema(), sortingColumn)
	if !ok {
		t.Fatal("left index plan unavailable")
	}
	right, _, ok := pageMergeRangesOf(rowGroups[1], rowGroups[1].Schema(), sortingColumn)
	if !ok {
		t.Fatal("right index plan unavailable")
	}
	if _, ok := pageMergePlanOf(left, right, columnType); !ok {
		t.Fatal("page index did not select a direct component")
	}
}

func assertForgedBoundsNeedRowMerge(t *testing.T, rowGroups []RowGroup) {
	t.Helper()
	sortingColumn := Ascending("key")
	left, columnType, ok := pageMergeRangesOf(rowGroups[0], rowGroups[0].Schema(), sortingColumn)
	if !ok {
		t.Fatal("left index plan unavailable")
	}
	right, _, ok := pageMergeRangesOf(rowGroups[1], rowGroups[1].Schema(), sortingColumn)
	if !ok {
		t.Fatal("right index plan unavailable")
	}
	plan, ok := pageMergePlanOf(left, right, columnType)
	if !ok {
		t.Fatal("forged index did not select a direct component")
	}
	if verifyPageMergePlan(rowGroups, rowGroups[0].Schema(), sortingColumn, columnType, plan) {
		t.Fatal("decoded boundaries accepted forged page-index bounds")
	}
}

func forgeFirstPageMinimum(t *testing.T, rowGroups []RowGroup) {
	t.Helper()
	leftIndex, err := rowGroups[0].ColumnChunks()[0].ColumnIndex()
	if err != nil {
		t.Fatal(err)
	}
	rightChunk, ok := rowGroups[1].ColumnChunks()[0].(*FileColumnChunk)
	if !ok {
		t.Fatalf("column chunk type: %T", rowGroups[1].ColumnChunks()[0])
	}
	rightIndex := rightChunk.columnIndex.Load()
	if rightIndex == nil || leftIndex.NumPages() != 2 || rightIndex.NumPages() != 2 {
		t.Fatal("expected two-page indexes")
	}

	// These source-sorted pages are [0..9], [20..29] and [5..14], [30..39].
	// Forging the second source's first minimum from 5 to 10 makes the index
	// planner select it after [0..9], although its decoded first value is 5.
	minimum := int64(10)
	if leftIndex.MaxValue(0).Int64() != 9 || rightIndex.MinValue(0).Int64() != 5 || rightIndex.MaxValue(0).Int64() != 14 {
		t.Fatalf("unexpected first-page bounds: left max=%d, right=[%d,%d]", leftIndex.MaxValue(0).Int64(), rightIndex.MinValue(0).Int64(), rightIndex.MaxValue(0).Int64())
	}
	rightIndex.index.MinValues[0] = append([]byte(nil), ValueOf(minimum).Bytes()...)
}

func mergeRowsForTest(t *testing.T, rowGroups []RowGroup, options []RowGroupOption) []Row {
	t.Helper()
	merged, err := MergeRowGroups(rowGroups, options...)
	if err != nil {
		t.Fatal(err)
	}
	rows := merged.Rows()
	defer rows.Close()
	return drainMerged(t, rows)
}

// TestMergedRowReaderRunDetection verifies that run detection produces exactly
// the same output (including the order of equal rows) as per-row replay, across
// random workloads ranging from fully interleaved to fully disjoint, with
// duplicated keys within and across readers.
func TestMergedRowReaderRunDetection(t *testing.T) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}

	makeRow := func(key int64, reader int, pos int) Row {
		return Row{
			ValueOf(key).Level(0, 0, 0),
			ValueOf(fmt.Sprintf("r%d-%d", reader, pos)).Level(0, 0, 1), // provenance
		}
	}

	prng := rand.New(rand.NewSource(1))

	for _, numReaders := range []int{3, 4, 5, 8, 16} {
		for _, mode := range []string{"interleaved", "disjoint", "boundary_overlap", "duplicates"} {
			t.Run(fmt.Sprintf("k=%d/%s", numReaders, mode), func(t *testing.T) {
				inputs := make([][]Row, numReaders)
				for r := range numReaders {
					numRows := 50 + prng.Intn(200)
					keys := make([]int64, numRows)
					for i := range keys {
						switch mode {
						case "interleaved":
							keys[i] = prng.Int63n(1000)
						case "disjoint":
							keys[i] = int64(r*100_000) + prng.Int63n(10_000)
						case "boundary_overlap":
							keys[i] = int64(r*1000) + prng.Int63n(1100) // ~10% overlap
						case "duplicates":
							keys[i] = prng.Int63n(20) // heavy ties everywhere
						}
					}
					slices.Sort(keys)
					rows := make([]Row, numRows)
					for i, k := range keys {
						rows[i] = makeRow(k, r, i)
					}
					inputs[r] = rows
				}

				newReaders := func() []RowReader {
					readers := make([]RowReader, numReaders)
					for i := range readers {
						readers[i] = &sliceRowReader{rows: inputs[i]}
					}
					return readers
				}

				// Reference: same merger with run detection disabled by keeping
				// the streak permanently below the threshold.
				ref := mergeRowReaders(newReaders(), compare).(*mergedRowReader)
				refOut := drainReference(t, ref)

				got := drainMerged(t, mergeRowReaders(newReaders(), compare))

				if len(got) != len(refOut) {
					t.Fatalf("row count %d, want %d", len(got), len(refOut))
				}
				for i := range got {
					if compare(got[i], refOut[i]) != 0 || got[i][1].String() != refOut[i][1].String() {
						t.Fatalf("row %d differs: got key=%d src=%s, want key=%d src=%s",
							i, got[i][0].Int64(), got[i][1].String(), refOut[i][0].Int64(), refOut[i][1].String())
					}
				}
			})
		}
	}
}

// drainReference drains m with run detection suppressed (streak reset before
// every batch), reproducing pure per-row replay.
func drainReference(t *testing.T, m *mergedRowReader) []Row {
	t.Helper()
	var out []Row
	buf := make([]Row, 7)
	for {
		m.streak = -1 << 30 // never reaches runDetectionStreak within a batch...
		n, err := m.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, slices.Clone(row))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

// TestMergedRowReader2RunDetection verifies that run galloping in the two-way
// merge produces exactly the same output (including the order of equal rows
// and batch-boundary behavior) as per-row merging, using the same merger with
// galloping disabled as the oracle.
func TestMergedRowReader2RunDetection(t *testing.T) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}

	makeRow := func(key int64, reader int, pos int) Row {
		return Row{
			ValueOf(key).Level(0, 0, 0),
			ValueOf(fmt.Sprintf("r%d-%d", reader, pos)).Level(0, 0, 1), // provenance
		}
	}

	prng := rand.New(rand.NewSource(2))

	for _, mode := range []string{"interleaved", "disjoint", "boundary_overlap", "duplicates"} {
		t.Run(mode, func(t *testing.T) {
			inputs := make([][]Row, 2)
			for r := range 2 {
				numRows := 50 + prng.Intn(500)
				keys := make([]int64, numRows)
				for i := range keys {
					switch mode {
					case "interleaved":
						keys[i] = prng.Int63n(1000)
					case "disjoint":
						keys[i] = int64(r*100_000) + prng.Int63n(10_000)
					case "boundary_overlap":
						keys[i] = int64(r*1000) + prng.Int63n(1100) // ~10% overlap
					case "duplicates":
						keys[i] = prng.Int63n(20) // heavy ties everywhere
					}
				}
				slices.Sort(keys)
				rows := make([]Row, numRows)
				for i, k := range keys {
					rows[i] = makeRow(k, r, i)
				}
				inputs[r] = rows
			}

			newMerge := func() *mergedRowReader2 {
				readers := make([]RowReader, 2)
				for i := range readers {
					readers[i] = &sliceRowReader{rows: inputs[i]}
				}
				return mergeRowReaders(readers, compare).(*mergedRowReader2)
			}

			refOut := drainReference2(t, newMerge())
			got := drainMerged(t, newMerge())

			if len(got) != len(refOut) {
				t.Fatalf("row count %d, want %d", len(got), len(refOut))
			}
			for i := range got {
				if compare(got[i], refOut[i]) != 0 || got[i][1].String() != refOut[i][1].String() {
					t.Fatalf("row %d differs: got key=%d src=%s, want key=%d src=%s",
						i, got[i][0].Int64(), got[i][1].String(), refOut[i][0].Int64(), refOut[i][1].String())
				}
			}
		})
	}
}

// drainReference2 drains m with run galloping suppressed (streak reset before
// every batch), reproducing pure per-row two-way merging.
func drainReference2(t *testing.T, m *mergedRowReader2) []Row {
	t.Helper()
	var out []Row
	buf := make([]Row, 7)
	for {
		m.streak = -1 << 30 // never reaches runDetectionStreak within a batch
		n, err := m.ReadRows(buf)
		for _, row := range buf[:n] {
			out = append(out, slices.Clone(row))
		}
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatal(err)
		}
		if n == 0 {
			t.Fatal("no progress")
		}
	}
}

func benchmarkMergedRowReader(b *testing.B, numReaders int, mode string, runs bool) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}
	prng := rand.New(rand.NewSource(1))
	inputs := make([][]Row, numReaders)
	for r := range numReaders {
		const numRows = 10_000
		keys := make([]int64, numRows)
		for i := range keys {
			switch mode {
			case "interleaved":
				keys[i] = prng.Int63n(1_000_000)
			case "boundary_overlap":
				keys[i] = int64(r*numRows) + prng.Int63n(numRows+numRows/10)
			}
		}
		slices.Sort(keys)
		rows := make([]Row, numRows)
		for i, k := range keys {
			rows[i] = Row{ValueOf(k).Level(0, 0, 0)}
		}
		inputs[r] = rows
	}

	buf := make([]Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readers := make([]RowReader, numReaders)
		for i := range readers {
			readers[i] = &sliceRowReader{rows: inputs[i]}
		}
		m := mergeRowReaders(readers, compare).(*mergedRowReader)
		for {
			var err error
			if !runs {
				m.streak = -1 << 30
			}
			_, err = m.ReadRows(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMergedRowReaderRuns(b *testing.B) {
	for _, k := range []int{4, 16} {
		for _, mode := range []string{"boundary_overlap", "interleaved"} {
			b.Run(fmt.Sprintf("k=%d/%s/runs", k, mode), func(b *testing.B) { benchmarkMergedRowReader(b, k, mode, true) })
			b.Run(fmt.Sprintf("k=%d/%s/base", k, mode), func(b *testing.B) { benchmarkMergedRowReader(b, k, mode, false) })
		}
	}
}

func benchmarkMergedRowReader2(b *testing.B, mode string, gallop bool) {
	compare := func(a, b Row) int {
		switch {
		case a[0].Int64() < b[0].Int64():
			return -1
		case a[0].Int64() > b[0].Int64():
			return 1
		default:
			return 0
		}
	}
	prng := rand.New(rand.NewSource(1))
	inputs := make([][]Row, 2)
	for r := range 2 {
		const numRows = 10_000
		keys := make([]int64, numRows)
		for i := range keys {
			switch mode {
			case "interleaved":
				keys[i] = prng.Int63n(1_000_000)
			case "boundary_overlap":
				keys[i] = int64(r*numRows) + prng.Int63n(numRows+numRows/10)
			}
		}
		slices.Sort(keys)
		rows := make([]Row, numRows)
		for i, k := range keys {
			rows[i] = Row{ValueOf(k).Level(0, 0, 0)}
		}
		inputs[r] = rows
	}

	buf := make([]Row, 256)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		readers := make([]RowReader, 2)
		for i := range readers {
			readers[i] = &sliceRowReader{rows: inputs[i]}
		}
		m := mergeRowReaders(readers, compare).(*mergedRowReader2)
		for {
			if !gallop {
				m.streak = -1 << 30
			}
			_, err := m.ReadRows(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkMergedRowReader2Runs(b *testing.B) {
	for _, mode := range []string{"boundary_overlap", "interleaved"} {
		b.Run(fmt.Sprintf("%s/gallop", mode), func(b *testing.B) { benchmarkMergedRowReader2(b, mode, true) })
		b.Run(fmt.Sprintf("%s/base", mode), func(b *testing.B) { benchmarkMergedRowReader2(b, mode, false) })
	}
}
