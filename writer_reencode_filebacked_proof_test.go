package parquet

import (
	"bytes"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
	"time"
)

// zeroDelayReaderAt records overlapping ReadAt calls without adding latency.
// It is deliberately used only to pin the small default-serial L3 baseline;
// production re-encode workers do not depend on this proof-only observer.
type zeroDelayReaderAt struct {
	io.ReaderAt
	active    atomic.Int64
	maxActive atomic.Int64
}

func (r *zeroDelayReaderAt) ReadAt(p []byte, off int64) (int, error) {
	active := r.active.Add(1)
	for {
		max := r.maxActive.Load()
		if active <= max || r.maxActive.CompareAndSwap(max, active) {
			break
		}
	}
	n, err := r.ReaderAt.ReadAt(p, off)
	r.active.Add(-1)
	return n, err
}

// TestWriteRowGroupReencodeFileBackedSerialBaseline proves that the ordinary
// three-column, file-backed L3 path has one active source read at a time. The
// reader adds no delay, so this is an observation of the existing baseline,
// not a scheduling aid for a later implementation.
func TestWriteRowGroupReencodeFileBackedSerialBaseline(t *testing.T) {
	rows := makeCopyTestRows(8_192)

	var source bytes.Buffer
	sourceWriter := NewGenericWriter[copyTestRow](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatal(err)
	}

	reader := &zeroDelayReaderAt{ReaderAt: bytes.NewReader(source.Bytes())}
	file, err := OpenFile(reader, int64(source.Len()))
	if err != nil {
		t.Fatal(err)
	}

	before := reencodePathCounter.Load()
	var output bytes.Buffer
	writer := NewGenericWriter[copyTestRow](&output, Compression(&Zstd))
	if _, err := writer.WriteRowGroup(file.RowGroups()[0]); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if reencodePathCounter.Load() == before {
		t.Fatal("expected the L3 re-encode path to run")
	}
	if max := reader.maxActive.Load(); max != 1 {
		t.Fatalf("maximum simultaneous source reads = %d, want 1 for the default serial baseline", max)
	}
	if got := readCopyTestRows(t, output.Bytes(), len(rows)); !reflect.DeepEqual(got, rows) {
		t.Fatal("serial baseline output differs from source rows")
	}
}

func makeFileBackedWideReencodeSource(tb testing.TB) (RowGroup, []wideRow, int64) {
	tb.Helper()

	const numRows = 100_000
	rows := makeWideRows(numRows)

	var source bytes.Buffer
	sourceWriter := NewGenericWriter[wideRow](
		&source,
		Compression(&Snappy),
		MaxRowsPerRowGroup(numRows),
	)
	if _, err := sourceWriter.Write(rows); err != nil {
		tb.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		tb.Fatal(err)
	}

	file, err := OpenFile(bytes.NewReader(source.Bytes()), int64(source.Len()))
	if err != nil {
		tb.Fatal(err)
	}
	if got := len(file.RowGroups()); got != 1 {
		tb.Fatalf("source row groups = %d, want 1", got)
	}
	return file.RowGroups()[0], rows, int64(source.Len())
}

func rewriteFileBackedWideRowGroup(tb testing.TB, rowGroup RowGroup) []byte {
	tb.Helper()

	var output bytes.Buffer
	writer := NewGenericWriter[wideRow](
		&output,
		Compression(&Zstd), // differs from the source codec, forcing L3 re-encode
		MaxRowsPerRowGroup(rowGroup.NumRows()),
	)
	if _, err := writer.WriteRowGroup(rowGroup); err != nil {
		tb.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}
	return output.Bytes()
}

func verifyFileBackedWideRewrite(tb testing.TB, output []byte, want []wideRow) {
	tb.Helper()

	reader := NewGenericReader[wideRow](bytes.NewReader(output))
	defer reader.Close()

	got := make([]wideRow, len(want))
	read := 0
	for read < len(got) {
		n, err := reader.Read(got[read:])
		read += n
		if err == io.EOF {
			break
		}
		if err != nil {
			tb.Fatal(err)
		}
	}
	if read != len(want) {
		tb.Fatalf("read %d rows, want %d", read, len(want))
	}
	if !reflect.DeepEqual(got, want) {
		tb.Fatal("wide file-backed re-encode output differs from source rows")
	}
}

// BenchmarkWriteRowGroupReencodeFileBackedWide measures one end-to-end L3
// rewrite of a wide, file-backed row group. Source data is Snappy-compressed and
// output uses Zstd, so the L0 copy path cannot apply. The output is read back
// after timing to keep the result observable and validate the measured rewrite.
func BenchmarkWriteRowGroupReencodeFileBackedWide(b *testing.B) {
	rowGroup, rows, sourceSize := makeFileBackedWideReencodeSource(b)
	var output []byte

	b.ReportAllocs()
	b.SetBytes(sourceSize)
	b.ResetTimer()
	for b.Loop() {
		output = rewriteFileBackedWideRowGroup(b, rowGroup)
	}
	b.StopTimer()

	verifyFileBackedWideRewrite(b, output, rows)
}

// BenchmarkWriteRowGroupReencodeFileBackedWideConcurrent measures aggregate
// rewrites with four callers on the same four-core runtime. It guards the
// caller-throughput risk of adding internal CPU parallelism to a single L3
// rewrite; each caller owns its Writer while sharing the immutable file-backed
// source row group.
func BenchmarkWriteRowGroupReencodeFileBackedWideConcurrent(b *testing.B) {
	rowGroup, _, sourceSize := makeFileBackedWideReencodeSource(b)
	var outputBytes atomic.Int64

	b.ReportAllocs()
	b.SetBytes(sourceSize)
	b.SetParallelism(1)
	b.ResetTimer()
	start := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			output := rewriteFileBackedWideRowGroup(b, rowGroup)
			if len(output) == 0 {
				b.Fatal("empty re-encode output")
			}
			outputBytes.Add(int64(len(output)))
		}
	})
	elapsed := time.Since(start)
	b.StopTimer()

	if outputBytes.Load() == 0 {
		b.Fatal("no output produced")
	}
	b.ReportMetric(float64(b.N)/elapsed.Seconds(), "ops/s")
}
