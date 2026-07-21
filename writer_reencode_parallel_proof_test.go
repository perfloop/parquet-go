package parquet

import (
	"bytes"
	"io"
	"reflect"
	"sync/atomic"
	"testing"
)

// reencodeSerialReaderAt observes overlapping source reads without delaying
// them. It is deliberately test-only: production workers must not depend on
// this observation hook.
type reencodeSerialReaderAt struct {
	reader io.ReaderAt
	active atomic.Int64
	max    atomic.Int64
}

func (r *reencodeSerialReaderAt) ReadAt(p []byte, off int64) (int, error) {
	active := r.active.Add(1)
	for {
		max := r.max.Load()
		if active <= max || r.max.CompareAndSwap(max, active) {
			break
		}
	}

	n, err := r.reader.ReadAt(p, off)
	r.active.Add(-1)
	return n, err
}

func (r *reencodeSerialReaderAt) reset() {
	if active := r.active.Load(); active != 0 {
		panic("resetting ReaderAt while reads are active")
	}
	r.max.Store(0)
}

func (r *reencodeSerialReaderAt) maxActive() int64 {
	return r.max.Load()
}

func reencodeWideSource(t testing.TB, numRows int) []byte {
	t.Helper()

	rows := makeWideRows(numRows)
	var source bytes.Buffer
	writer := NewGenericWriter[wideRow](
		&source,
		Compression(&Snappy),
		MaxRowsPerRowGroup(25_000),
	)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("writing source rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing source writer: %v", err)
	}
	return source.Bytes()
}

// TestWriteRowGroupReencodeDefaultReaderAtSerial proves the default L3 path
// performs its file-backed source reads serially. The observer adds no latency,
// so this check protects only the default degree-one behavior and does not make
// a future parallel implementation depend on benchmark instrumentation.
func TestWriteRowGroupReencodeDefaultReaderAtSerial(t *testing.T) {
	const numRows = 25_000

	source := reencodeWideSource(t, numRows)
	reader := &reencodeSerialReaderAt{reader: bytes.NewReader(source)}
	file, err := OpenFile(reader, int64(len(source)))
	if err != nil {
		t.Fatalf("opening source file: %v", err)
	}
	reader.reset() // Exclude OpenFile's footer and index reads from the observation.

	before := reencodePathCounter.Load()
	var destination bytes.Buffer
	writer := NewGenericWriter[wideRow](
		&destination,
		Compression(&Zstd), // Codec mismatch forces L3 instead of L0 copy.
		MaxRowsPerRowGroup(25_000),
	)
	for _, rowGroup := range file.RowGroups() {
		if _, err := writer.WriteRowGroup(rowGroup); err != nil {
			t.Fatalf("rewriting row group: %v", err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("closing destination writer: %v", err)
	}

	if reencoded := reencodePathCounter.Load() - before; reencoded == 0 {
		t.Fatal("expected the L3 re-encode path to run")
	}
	maxActive := reader.maxActive()
	t.Logf("default L3 max active ReadAt calls: %d", maxActive)
	if maxActive != 1 {
		t.Fatalf("default L3 source reads had max active ReadAt calls = %d, want 1", maxActive)
	}

	output, err := OpenFile(bytes.NewReader(destination.Bytes()), int64(destination.Len()))
	if err != nil {
		t.Fatalf("opening rewritten file: %v", err)
	}
	if got := output.NumRows(); got != numRows {
		t.Fatalf("rewritten row count = %d, want %d", got, numRows)
	}
}

// setReencodeParallelismForBenchmark leaves the benchmark buildable before the
// opt-in configuration exists. The candidate supplies this public WriterConfig
// field; the baseline therefore measures the existing degree-one behavior.
func setReencodeParallelismForBenchmark(config *WriterConfig, degree int) {
	field := reflect.ValueOf(config).Elem().FieldByName("ColumnReencodeConcurrency")
	if field.IsValid() && field.CanSet() && field.Kind() == reflect.Int {
		field.SetInt(int64(degree))
	}
}

// BenchmarkWriteRowGroupReencodeWideColumnParallel measures complete L3
// rewrites of a 24-column Snappy-to-Zstd file-backed workload. The sealed
// command pins GOMAXPROCS=4; a candidate can opt into four concurrent columns
// while the baseline retains the default serial path.
func BenchmarkWriteRowGroupReencodeWideColumnParallel(b *testing.B) {
	const (
		numRows = 100_000
		degree  = 4
	)

	source := reencodeWideSource(b, numRows)
	file, err := OpenFile(bytes.NewReader(source), int64(len(source)))
	if err != nil {
		b.Fatalf("opening source file: %v", err)
	}
	rowGroups := file.RowGroups()
	if len(rowGroups) == 0 {
		b.Fatal("source file has no row groups")
	}

	config, err := NewWriterConfig(
		Compression(&Zstd),
		MaxRowsPerRowGroup(25_000),
	)
	if err != nil {
		b.Fatal(err)
	}
	setReencodeParallelismForBenchmark(config, degree)

	var output []byte
	b.ReportAllocs()
	b.SetBytes(int64(len(source)))
	b.ResetTimer()
	for b.Loop() {
		var destination bytes.Buffer
		writer := NewGenericWriter[wideRow](&destination, config)
		for _, rowGroup := range rowGroups {
			if _, err := writer.WriteRowGroup(rowGroup); err != nil {
				b.Fatal(err)
			}
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		output = destination.Bytes()
	}
	b.StopTimer()

	result, err := OpenFile(bytes.NewReader(output), int64(len(output)))
	if err != nil {
		b.Fatalf("opening benchmark output: %v", err)
	}
	if got := result.NumRows(); got != numRows {
		b.Fatalf("rewritten row count = %d, want %d", got, numRows)
	}
}
