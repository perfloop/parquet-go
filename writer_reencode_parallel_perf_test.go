package parquet

import (
	"bytes"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"
)

// reencodeBenchmarkReaderAt exposes immutable parquet bytes through ReaderAt.
// A FileColumnChunk creates an independent section reader over this source for
// every column, matching the file-backed L3 path without sharing a seek offset.
type reencodeBenchmarkReaderAt struct {
	data []byte
}

func (r reencodeBenchmarkReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if off < 0 || off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n := copy(p, r.data[off:])
	if n != len(p) {
		return n, io.EOF
	}
	return n, nil
}

// reencodeBenchmarkColumnConcurrency is deliberately reflection-based so this
// benchmark compiles on the baseline before the opt-in configuration exists.
// Once ColumnReencodeConcurrency is introduced, it exercises exactly that
// public configuration field; before then it is a no-op and measures the
// degree-one serial path.
func reencodeBenchmarkColumnConcurrency(degree int) WriterOption {
	return writerOption(func(config *WriterConfig) {
		field := reflect.ValueOf(config).Elem().FieldByName("ColumnReencodeConcurrency")
		if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int {
			field.SetInt(int64(degree))
		}
	})
}

func supportsReencodeBenchmarkColumnConcurrency() bool {
	config := DefaultWriterConfig()
	field := reflect.ValueOf(config).Elem().FieldByName("ColumnReencodeConcurrency")
	return field.IsValid() && field.CanSet() && field.Kind() == reflect.Int
}

func newReencodeSpanSource(t testing.TB) ([]copyTestRow, *File) {
	t.Helper()

	rows := makeCopyTestRows(8_000)
	var source bytes.Buffer
	sourceWriter := NewGenericWriter[copyTestRow](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatalf("writing span source: %v", err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatalf("closing span source: %v", err)
	}

	reader := reencodeBenchmarkReaderAt{data: bytes.Clone(source.Bytes())}
	file, err := OpenFile(reader, int64(len(reader.data)))
	if err != nil {
		t.Fatalf("opening span source: %v", err)
	}
	return rows, file
}

func rewriteReencodeSpan(t *testing.T, file *File, rows []copyTestRow, options ...WriterOption) []byte {
	t.Helper()

	var output bytes.Buffer
	writerOptions := make([]WriterOption, 0, len(options)+1)
	writerOptions = append(writerOptions, Compression(&Zstd)) // Force L3, not L0.
	writerOptions = append(writerOptions, options...)
	writer := NewGenericWriter[copyTestRow](&output, writerOptions...)

	before := reencodePathCounter.Load()
	var written int64
	for _, rowGroup := range file.RowGroups() {
		n, err := writer.WriteRowGroup(rowGroup)
		if err != nil {
			t.Fatalf("re-encoding span row group: %v", err)
		}
		written += n
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing span destination: %v", err)
	}
	if written != int64(len(rows)) {
		t.Fatalf("re-encoding span wrote %d rows, want %d", written, len(rows))
	}
	if reencodePathCounter.Load() == before {
		t.Fatal("expected the L3 re-encode path to fire")
	}

	got := readCopyTestRows(t, output.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatal("re-encoded span output differs from source rows")
	}
	return output.Bytes()
}

// reencodeColumnSpanRecorder receives begin/end callbacks from the source
// copyColumnValues invocation. It is observation-only: unlike an artificial
// ReaderAt delay, it never changes the execution schedule or source latency.
type reencodeColumnSpanRecorder struct {
	mu sync.Mutex

	active        int
	maxActive     int
	started       int
	finished      int
	invalidEvents bool
	columns       map[int]struct{}
}

func newReencodeColumnSpanRecorder() *reencodeColumnSpanRecorder {
	return &reencodeColumnSpanRecorder{columns: make(map[int]struct{})}
}

func (r *reencodeColumnSpanRecorder) begin(column int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.columns[column]; exists {
		r.invalidEvents = true
	}
	r.columns[column] = struct{}{}
	r.active++
	r.started++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
}

func (r *reencodeColumnSpanRecorder) end(column int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.columns[column]; !exists {
		r.invalidEvents = true
	} else {
		delete(r.columns, column)
	}
	if r.active == 0 {
		r.invalidEvents = true
	} else {
		r.active--
	}
	r.finished++
}

func (r *reencodeColumnSpanRecorder) reset() {
	r.mu.Lock()
	r.active = 0
	r.maxActive = 0
	r.started = 0
	r.finished = 0
	r.invalidEvents = false
	clear(r.columns)
	r.mu.Unlock()
}

func (r *reencodeColumnSpanRecorder) snapshot() (maxActive, started, finished int, valid bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxActive, r.started, r.finished, r.active == 0 && !r.invalidEvents && len(r.columns) == 0
}

func installReencodeColumnSpanRecorder(t *testing.T) *reencodeColumnSpanRecorder {
	t.Helper()

	recorder := newReencodeColumnSpanRecorder()
	hook := &reencodeColumnCopyTrace{begin: recorder.begin, end: recorder.end}
	if !reencodeColumnCopyTraceHook.CompareAndSwap(nil, hook) {
		t.Fatal("another re-encode column-span recorder is already installed")
	}
	t.Cleanup(func() {
		if !reencodeColumnCopyTraceHook.CompareAndSwap(hook, nil) {
			t.Error("re-encode column-span recorder was replaced before cleanup")
		}
	})
	return recorder
}

func countReencodeColumns(file *File) int {
	count := 0
	for _, rowGroup := range file.RowGroups() {
		count += len(rowGroup.ColumnChunks())
	}
	return count
}

func assertReencodeColumnSpans(t *testing.T, recorder *reencodeColumnSpanRecorder, degree, want int) {
	t.Helper()

	maxActive, started, finished, valid := recorder.snapshot()
	if !valid || started != want || finished != want {
		t.Fatalf("L3 span trace invalid=%v started=%d finished=%d, want %d completed spans", !valid, started, finished, want)
	}
	if maxActive == 0 || maxActive > degree {
		t.Fatalf("L3 span trace had max %d active copies, want 1..%d", maxActive, degree)
	}
}

// TestWriteRowGroupReencodeSerialTrace traces the actual copyColumnValues spans
// in a file-backed L3 rewrite. The degree-one trace proves the baseline's work
// is serial without imposing a source-I/O model. When the opt-in field exists,
// the same fixture verifies bounded worker spans and byte-identical output.
func TestWriteRowGroupReencodeSerialTrace(t *testing.T) {
	previousProcs := runtime.GOMAXPROCS(3)
	defer runtime.GOMAXPROCS(previousProcs)

	rows, file := newReencodeSpanSource(t)
	wantSpans := countReencodeColumns(file)
	recorder := installReencodeColumnSpanRecorder(t)

	serial := rewriteReencodeSpan(t, file, rows)
	assertReencodeColumnSpans(t, recorder, 1, wantSpans)
	maxActive, _, _, _ := recorder.snapshot()
	t.Logf("reencode column-span trace: degree=1 max-active=%d spans=%d", maxActive, wantSpans)

	if !supportsReencodeBenchmarkColumnConcurrency() {
		return
	}

	recorder.reset()
	parallel := rewriteReencodeSpan(t, file, rows, reencodeBenchmarkColumnConcurrency(3))
	if !bytes.Equal(parallel, serial) {
		t.Fatal("parallel L3 output differs byte-for-byte from degree-one output")
	}
	assertReencodeColumnSpans(t, recorder, 3, wantSpans)
	maxActive, _, _, _ = recorder.snapshot()
	t.Logf("reencode column-span trace: degree=3 max-active=%d spans=%d", maxActive, wantSpans)
}

type reencodeCountingWriter struct {
	bytes int64
}

func (w *reencodeCountingWriter) Write(p []byte) (int, error) {
	w.bytes += int64(len(p))
	return len(p), nil
}

// BenchmarkWriteRowGroupReencodeWideParallel measures the complete file-backed
// L3 rewrite of a 24-column, Snappy-to-Zstd row group. It fixes a four-CPU
// budget and asks for four column workers when that opt-in exists; the baseline
// accepts the same option as a no-op and remains the degree-one serial path.
func BenchmarkWriteRowGroupReencodeWideParallel(b *testing.B) {
	const workers = 4
	previousProcs := runtime.GOMAXPROCS(workers)
	defer runtime.GOMAXPROCS(previousProcs)

	rows := makeWideRows(100_000)
	var source bytes.Buffer
	sourceWriter := NewGenericWriter[wideRow](&source, Compression(&Snappy), MaxRowsPerRowGroup(25_000))
	if _, err := sourceWriter.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := sourceWriter.Close(); err != nil {
		b.Fatal(err)
	}

	reader := reencodeBenchmarkReaderAt{data: bytes.Clone(source.Bytes())}
	file, err := OpenFile(reader, int64(len(reader.data)))
	if err != nil {
		b.Fatal(err)
	}
	rowGroups := file.RowGroups()

	b.ReportAllocs()
	b.SetBytes(int64(len(reader.data)))
	b.ResetTimer()
	for b.Loop() {
		output := new(reencodeCountingWriter)
		writer := NewGenericWriter[wideRow](
			output,
			Compression(&Zstd), // Force L3, not L0.
			MaxRowsPerRowGroup(25_000),
			reencodeBenchmarkColumnConcurrency(workers),
		)

		before := reencodePathCounter.Load()
		var written int64
		for _, rowGroup := range rowGroups {
			n, err := writer.WriteRowGroup(rowGroup)
			if err != nil {
				b.Fatal(err)
			}
			written += n
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if written != int64(len(rows)) {
			b.Fatalf("re-encoding wrote %d rows, want %d", written, len(rows))
		}
		if output.bytes == 0 {
			b.Fatal("re-encoding produced no output")
		}
		if got := reencodePathCounter.Load() - before; got != int64(len(rowGroups)) {
			b.Fatalf("L3 re-encode path ran %d times, want %d", got, len(rowGroups))
		}
	}
}
