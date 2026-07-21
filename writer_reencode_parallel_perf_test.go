package parquet

import (
	"bytes"
	"io"
	"reflect"
	"runtime"
	"sync"
	"testing"
	"time"
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

// reencodeTraceReaderAt records concurrent ReadAt calls while retaining the
// ReaderAt contract. Its delay is only a controlled scheduling aid for the
// overlap observation; the paired zero-delay control below guards the ordinary
// in-memory ReaderAt path and compares its serialized output.
type reencodeTraceReaderAt struct {
	reencodeBenchmarkReaderAt
	delay time.Duration

	mu        sync.Mutex
	active    int
	maxActive int
	calls     int
}

func (r *reencodeTraceReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.maxActive {
		r.maxActive = r.active
	}
	r.calls++
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.active--
		r.mu.Unlock()
	}()

	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	return r.reencodeBenchmarkReaderAt.ReadAt(p, off)
}

func (r *reencodeTraceReaderAt) resetTrace() {
	r.mu.Lock()
	r.active = 0
	r.maxActive = 0
	r.calls = 0
	r.mu.Unlock()
}

func (r *reencodeTraceReaderAt) trace() (maxActive, calls int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.maxActive, r.calls
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

func newReencodeTraceSource(t testing.TB, delay time.Duration) ([]copyTestRow, *File, *reencodeTraceReaderAt) {
	t.Helper()

	rows := makeCopyTestRows(2_000)
	var source bytes.Buffer
	sourceWriter := NewGenericWriter[copyTestRow](&source, Compression(&Snappy))
	if _, err := sourceWriter.Write(rows); err != nil {
		t.Fatalf("writing trace source: %v", err)
	}
	if err := sourceWriter.Close(); err != nil {
		t.Fatalf("closing trace source: %v", err)
	}

	reader := &reencodeTraceReaderAt{
		reencodeBenchmarkReaderAt: reencodeBenchmarkReaderAt{data: bytes.Clone(source.Bytes())},
		delay:                     delay,
	}
	file, err := OpenFile(reader, int64(len(reader.data)))
	if err != nil {
		t.Fatalf("opening trace source: %v", err)
	}
	reader.resetTrace() // Exclude OpenFile's header/footer reads from the L3 trace.
	return rows, file, reader
}

func rewriteReencodeTrace(t *testing.T, file *File, rows []copyTestRow, options ...WriterOption) []byte {
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
			t.Fatalf("re-encoding trace row group: %v", err)
		}
		written += n
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing trace destination: %v", err)
	}
	if written != int64(len(rows)) {
		t.Fatalf("re-encoding trace wrote %d rows, want %d", written, len(rows))
	}
	if reencodePathCounter.Load() == before {
		t.Fatal("expected the L3 re-encode path to fire")
	}

	got := readCopyTestRows(t, output.Bytes(), len(rows))
	if !reflect.DeepEqual(got, rows) {
		t.Fatal("re-encoded trace output differs from source rows")
	}
	return output.Bytes()
}

// TestWriteRowGroupReencodeSerialTrace is a controlled source-read trace for
// the serial-work hypothesis. The default path must keep degree one; when the
// opt-in field is present, the same fixture also proves a bounded parallel wave
// preserves the exact serialized output.
func TestWriteRowGroupReencodeSerialTrace(t *testing.T) {
	rows, delayedFile, delayedReader := newReencodeTraceSource(t, time.Millisecond)

	serial := rewriteReencodeTrace(t, delayedFile, rows)
	maxActive, calls := delayedReader.trace()
	if calls == 0 {
		t.Fatal("L3 trace observed no source ReaderAt calls")
	}
	if maxActive != 1 {
		t.Fatalf("serial L3 trace had max %d simultaneous source reads, want 1", maxActive)
	}
	t.Logf("reencode source-read trace: degree=1 max-active=%d calls=%d", maxActive, calls)

	// This zero-delay control is the deployment-shaped in-memory ReaderAt path.
	// It must have the same serial source-read geometry and serialized result as
	// the delayed trace, so the delay cannot create a semantic result by itself.
	controlRows, controlFile, controlReader := newReencodeTraceSource(t, 0)
	if !reflect.DeepEqual(controlRows, rows) {
		t.Fatal("zero-delay control source rows differ from delayed source rows")
	}
	control := rewriteReencodeTrace(t, controlFile, controlRows)
	if !bytes.Equal(control, serial) {
		t.Fatal("zero-delay ReaderAt output differs from delayed trace output")
	}
	maxActive, calls = controlReader.trace()
	if calls == 0 {
		t.Fatal("zero-delay control observed no source ReaderAt calls")
	}
	if maxActive != 1 {
		t.Fatalf("zero-delay serial L3 trace had max %d simultaneous source reads, want 1", maxActive)
	}
	t.Logf("reencode source-read control: delay=0 max-active=%d output=byte-identical", maxActive)

	if !supportsReencodeBenchmarkColumnConcurrency() {
		return
	}

	delayedReader.resetTrace()
	parallel := rewriteReencodeTrace(t, delayedFile, rows, reencodeBenchmarkColumnConcurrency(3))
	if !bytes.Equal(parallel, serial) {
		t.Fatal("parallel L3 output differs byte-for-byte from degree-one output")
	}
	maxActive, calls = delayedReader.trace()
	if calls == 0 {
		t.Fatal("parallel L3 trace observed no source ReaderAt calls")
	}
	if maxActive < 2 || maxActive > 3 {
		t.Fatalf("parallel L3 trace had max %d simultaneous source reads, want 2..3", maxActive)
	}
	t.Logf("reencode source-read trace: degree=3 max-active=%d calls=%d", maxActive, calls)
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
