//go:build !purego

package parquet

import (
	"bytes"
	"math"
	"testing"

	"github.com/parquet-go/parquet-go/encoding"
	"github.com/parquet-go/parquet-go/encoding/plain"
)

type benchmarkWriterInt64Row struct {
	Value int64
}

func TestBoundsInt64WithoutAVX512(t *testing.T) {
	if hasAVX512VL {
		t.Skip("run with GODEBUG=cpu.avx512f=off")
	}

	values := makeBenchmarkBoundsInt64Values(DefaultPageBufferSize / 8)
	wantMin, wantMax := scalarBoundsInt64(values)
	gotMin, gotMax := boundsInt64(values)
	if gotMin != wantMin || gotMax != wantMax {
		t.Fatalf("boundsInt64() = (%d, %d), want (%d, %d)", gotMin, gotMax, wantMin, wantMax)
	}
}

var benchmarkWriterInt64Sink int

func BenchmarkPageBoundsInt64WithoutAVX512(b *testing.B) {
	if hasAVX512VL {
		b.Fatal("run with GODEBUG=cpu.avx512f=off")
	}

	values := makeBenchmarkBoundsInt64Values(DefaultPageBufferSize / 8)
	wantMin, wantMax := scalarBoundsInt64(values)
	page := newInt64Page(int64Type{}, 0, int32(len(values)), encoding.Int64Values(values))

	gotMin, gotMax, ok := page.Bounds()
	if !ok || gotMin.Int64() != wantMin || gotMax.Int64() != wantMax {
		b.Fatalf("page.Bounds() = (%d, %d, %t), want (%d, %d, true)", gotMin.Int64(), gotMax.Int64(), ok, wantMin, wantMax)
	}

	b.SetBytes(DefaultPageBufferSize)
	b.ResetTimer()
	for b.Loop() {
		min, max, _ := page.Bounds()
		benchmarkPageBoundsInt64MinSink = min.Int64()
		benchmarkPageBoundsInt64MaxSink = max.Int64()
	}
	b.StopTimer()

	if benchmarkPageBoundsInt64MinSink != wantMin || benchmarkPageBoundsInt64MaxSink != wantMax {
		b.Fatalf("page.Bounds() = (%d, %d), want (%d, %d)", benchmarkPageBoundsInt64MinSink, benchmarkPageBoundsInt64MaxSink, wantMin, wantMax)
	}
}

const benchmarkWriterInt64PageValues = DefaultPageBufferSize*98/100/8 + 1

func BenchmarkWriterInt64DefaultPageSize(b *testing.B) {
	rows := makeBenchmarkWriterInt64Rows(benchmarkWriterInt64PageValues)
	var output bytes.Buffer
	checkBenchmarkWriterInt64Page(b, &output, rows)

	b.SetBytes(int64(len(rows)) * 8)
	b.ResetTimer()
	for b.Loop() {
		output.Reset()
		if n, err := writeBenchmarkWriterInt64Page(&output, rows); err != nil {
			b.Fatal(err)
		} else {
			benchmarkWriterInt64Sink = n + output.Len()
		}
	}
	b.StopTimer()

	if benchmarkWriterInt64Sink == 0 {
		b.Fatal("writer produced no output")
	}
}

func makeBenchmarkWriterInt64Rows(size int) []benchmarkWriterInt64Row {
	rows := make([]benchmarkWriterInt64Row, size)
	state := uint64(size)
	for i := range rows {
		state = nextBenchmarkBoundsState(state)
		rows[i].Value = int64(state)
	}
	rows[len(rows)/3].Value = math.MinInt64
	rows[2*len(rows)/3].Value = math.MaxInt64
	return rows
}

func writeBenchmarkWriterInt64Page(output *bytes.Buffer, rows []benchmarkWriterInt64Row) (int, error) {
	writer := NewGenericWriter[benchmarkWriterInt64Row](output,
		PageBufferSize(DefaultPageBufferSize),
		WriteBufferSize(0),
		DataPageStatistics(true),
		DefaultEncodingFor(Int64, &plain.Encoding{}),
	)
	n, err := writer.Write(rows)
	if err != nil {
		return n, err
	}
	return n, writer.Close()
}

func checkBenchmarkWriterInt64Page(b *testing.B, output *bytes.Buffer, rows []benchmarkWriterInt64Row) {
	b.Helper()
	if n, err := writeBenchmarkWriterInt64Page(output, rows); err != nil {
		b.Fatal(err)
	} else if n != len(rows) {
		b.Fatalf("writer wrote %d rows, want %d", n, len(rows))
	}

	file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		b.Fatal(err)
	}
	columnIndexes := file.ColumnIndexes()
	if len(columnIndexes) != 1 || len(columnIndexes[0].NullPages) != 1 {
		b.Fatalf("column indexes = %d with %d pages, want 1 index with 1 page", len(columnIndexes), len(columnIndexes[0].NullPages))
	}
	stats := file.Metadata().RowGroups[0].Columns[0].MetaData.Statistics
	if stats.MinValue == nil || stats.MaxValue == nil {
		b.Fatal("expected page statistics")
	}
}
