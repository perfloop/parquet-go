package parquet

import (
	"bytes"
	"io"
	"strconv"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestBoundsInt64WriterPageFill(t *testing.T) {
	for _, test := range [...]struct {
		numValues int
		numPages  int
	}{
		{numValues: combinedBoundsInt64Threshold - 1, numPages: 1},
		{numValues: combinedBoundsInt64Threshold, numPages: 1},
		{numValues: 2*combinedBoundsInt64Threshold - 1, numPages: 2},
	} {
		t.Run(strconv.Itoa(test.numValues)+"-values", func(t *testing.T) {
			values := boundsInt64CutoffValues(test.numValues)
			rows := boundsInt64PageFillRows(values)
			wantMin, wantMax := boundsInt64CutoffOracle(values)

			var output bytes.Buffer
			written, err := writeBoundsInt64PageFill(&output, rows)
			if err != nil {
				t.Fatal(err)
			}
			if written != len(rows) {
				t.Fatalf("writer.Write wrote %d rows, want %d", written, len(rows))
			}

			file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			index, err := file.RowGroups()[0].ColumnChunks()[0].ColumnIndex()
			if err != nil {
				t.Fatal(err)
			}
			if index.NumPages() != test.numPages {
				t.Fatalf("column index has %d pages, want %d", index.NumPages(), test.numPages)
			}

			gotMin, gotMax := index.MinValue(0).Int64(), index.MaxValue(0).Int64()
			for page := 1; page < index.NumPages(); page++ {
				gotMin = min(gotMin, index.MinValue(page).Int64())
				gotMax = max(gotMax, index.MaxValue(page).Int64())
			}
			if gotMin != wantMin || gotMax != wantMax {
				t.Fatalf("page statistics = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
			}
		})
	}
}

func BenchmarkBoundsInt64PageStatistics(b *testing.B) {
	const numValues = combinedBoundsInt64Threshold

	values := boundsInt64CutoffValues(numValues)
	wantMin, wantMax := boundsInt64CutoffOracle(values)
	page := Int64Type.NewPage(0, len(values), encoding.Int64Values(values))
	b.SetBytes(int64(len(values) * 8))

	var min, max []byte
	for b.Loop() {
		statistics := makePageStatistics(page.NumNulls(), pageBoundsOf(page))
		min, max = statistics.MinValue, statistics.MaxValue
	}
	if gotMin, gotMax := Int64.Value(min).Int64(), Int64.Value(max).Int64(); gotMin != wantMin || gotMax != wantMax {
		b.Fatalf("page statistics = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}
}

func BenchmarkBoundsInt64WriterPageFill(b *testing.B) {
	for _, numValues := range [...]int{
		combinedBoundsInt64Threshold - 1,
		combinedBoundsInt64Threshold,
		2*combinedBoundsInt64Threshold - 1,
	} {
		b.Run(strconv.Itoa(numValues)+"-values", func(b *testing.B) {
			values := boundsInt64CutoffValues(numValues)
			rows := boundsInt64PageFillRows(values)
			b.SetBytes(int64(len(values) * 8))

			for b.Loop() {
				written, err := writeBoundsInt64PageFill(io.Discard, rows)
				if err != nil {
					b.Fatal(err)
				}
				if written != len(rows) {
					b.Fatalf("writer.Write wrote %d rows, want %d", written, len(rows))
				}
			}
		})
	}
}

func boundsInt64PageFillRows(values []int64) []boundsInt64WriterRow {
	rows := make([]boundsInt64WriterRow, len(values))
	for i, value := range values {
		rows[i].Value = value
	}
	return rows
}

func writeBoundsInt64PageFill(output io.Writer, rows []boundsInt64WriterRow) (int, error) {
	writer := NewGenericWriter[boundsInt64WriterRow](output,
		PageBufferSize(DefaultPageBufferSize),
		DataPageStatistics(true),
	)
	written, err := writer.Write(rows)
	if err != nil {
		return written, err
	}
	return written, writer.Close()
}
