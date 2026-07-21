package parquet_test

import (
	"bytes"
	"fmt"
	"io"
	"math/bits"
	"os"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const deltaBinaryPackedCorpusPath = "testdata/delta_binary_packed.parquet"

type deltaBinaryPackedCorpus struct {
	schema  *parquet.Schema
	rows    []parquet.Row
	profile deltaBinaryPackedCorpusProfile
}

type deltaBinaryPackedCorpusProfile struct {
	sourceBytes           int
	rowGroups             int
	rows                  int
	int64Columns          int
	int64Values           int
	finalTailDeltas       int
	finalTailColumns      int
	maxAbsBits            int
	highConstantColumns   int
	highConstantValueBits int
	tailHistogram         map[int]int
}

func TestDeltaBinaryPackedCorpusProfile(t *testing.T) {
	corpus := loadDeltaBinaryPackedCorpus(t)
	profile := corpus.profile

	if profile.rowGroups != 1 || profile.rows != 200 || profile.int64Columns != 65 || profile.int64Values != 13_000 {
		t.Fatalf("unexpected corpus shape: row_groups=%d rows=%d int64_columns=%d int64_values=%d", profile.rowGroups, profile.rows, profile.int64Columns, profile.int64Values)
	}
	if profile.finalTailDeltas != 71 || profile.finalTailColumns != 65 || !mapsEqual(profile.tailHistogram, map[int]int{71: 65}) {
		t.Fatalf("unexpected final-tail distribution: tail_deltas=%d tail_columns=%d histogram=%s", profile.finalTailDeltas, profile.finalTailColumns, formatTailHistogram(profile.tailHistogram))
	}
	if profile.maxAbsBits != 64 || profile.highConstantColumns != 1 || profile.highConstantValueBits != 63 {
		t.Fatalf("unexpected int64 value regime: max_abs_bits=%d high_constant_columns=%d high_constant_value_bits=%d", profile.maxAbsBits, profile.highConstantColumns, profile.highConstantValueBits)
	}
	t.Logf("delta-binary-packed corpus: source_bytes=%d row_groups=%d rows=%d int64_columns=%d int64_values=%d final_tail_histogram=%s max_abs_bits=%d high_constant_columns=%d high_constant_value_bits=%d", profile.sourceBytes, profile.rowGroups, profile.rows, profile.int64Columns, profile.int64Values, formatTailHistogram(profile.tailHistogram), profile.maxAbsBits, profile.highConstantColumns, profile.highConstantValueBits)
}

func BenchmarkWriterDeltaBinaryPackedCorpusStorage(b *testing.B) {
	corpus := loadDeltaBinaryPackedCorpus(b)
	var output bytes.Buffer
	var outputSize int

	for b.Loop() {
		output.Reset()
		writer := parquet.NewWriter(
			&output,
			corpus.schema,
			parquet.DefaultEncodingFor(parquet.Int64, &parquet.DeltaBinaryPacked),
			parquet.MaxRowsPerRowGroup(int64(len(corpus.rows))),
			parquet.WriteBufferSize(0),
		)
		n, err := writer.WriteRows(corpus.rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(corpus.rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(corpus.rows))
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		outputSize = output.Len()
	}

	verifyDeltaBinaryPackedCorpusOutput(b, output.Bytes(), corpus)
	profile := corpus.profile
	b.ReportMetric(float64(outputSize), "serialized_bytes/op")
	b.ReportMetric(float64(profile.sourceBytes), "corpus_input_bytes/op")
	b.ReportMetric(float64(profile.rowGroups), "corpus_row_groups/op")
	b.ReportMetric(float64(profile.rows), "corpus_rows/op")
	b.ReportMetric(float64(profile.int64Columns), "corpus_int64_columns/op")
	b.ReportMetric(float64(profile.int64Values), "corpus_int64_values/op")
	b.ReportMetric(float64(profile.finalTailDeltas), "corpus_final_tail_deltas/op")
	b.ReportMetric(float64(profile.finalTailColumns), "corpus_final_tail_columns/op")
	b.ReportMetric(float64(profile.maxAbsBits), "corpus_max_abs_bits/op")
	b.ReportMetric(float64(profile.highConstantColumns), "corpus_high_constant_columns/op")
	b.ReportMetric(float64(profile.highConstantValueBits), "corpus_high_constant_value_bits/op")
}

func loadDeltaBinaryPackedCorpus(tb testing.TB) deltaBinaryPackedCorpus {
	tb.Helper()
	input, err := os.Open(deltaBinaryPackedCorpusPath)
	if err != nil {
		tb.Fatal(err)
	}
	defer input.Close()

	info, err := input.Stat()
	if err != nil {
		tb.Fatal(err)
	}
	file, err := parquet.OpenFile(input, info.Size())
	if err != nil {
		tb.Fatal(err)
	}
	rows := readDeltaBinaryPackedCorpusRows(tb, file)
	profile := profileDeltaBinaryPackedCorpus(rows, info.Size())
	profile.rowGroups = len(file.RowGroups())
	return deltaBinaryPackedCorpus{schema: file.Schema(), rows: rows, profile: profile}
}

func readDeltaBinaryPackedCorpusRows(tb testing.TB, file *parquet.File) []parquet.Row {
	tb.Helper()
	rows := make([]parquet.Row, 0, file.NumRows())
	buffer := make([]parquet.Row, 128)
	for _, rowGroup := range file.RowGroups() {
		reader := rowGroup.Rows()
		for {
			n, err := reader.ReadRows(buffer)
			for _, row := range buffer[:n] {
				rows = append(rows, slices.Clone(row))
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				tb.Fatal(err)
			}
		}
		if err := reader.Close(); err != nil {
			tb.Fatal(err)
		}
	}
	return rows
}

func profileDeltaBinaryPackedCorpus(rows []parquet.Row, sourceBytes int64) deltaBinaryPackedCorpusProfile {
	columns := make(map[int][]int64)
	for _, row := range rows {
		for _, value := range row {
			if value.Kind() == parquet.Int64 {
				columns[value.Column()] = append(columns[value.Column()], value.Int64())
			}
		}
	}

	profile := deltaBinaryPackedCorpusProfile{
		sourceBytes:     int(sourceBytes),
		rows:            len(rows),
		finalTailDeltas: -1,
		tailHistogram:   make(map[int]int),
	}
	for _, values := range columns {
		if len(values) == 0 {
			continue
		}
		profile.int64Columns++
		profile.int64Values += len(values)

		tailDeltas := (len(values) - 1) % 128
		profile.tailHistogram[tailDeltas]++
		profile.finalTailColumns++
		if profile.finalTailDeltas < 0 {
			profile.finalTailDeltas = tailDeltas
		} else if profile.finalTailDeltas != tailDeltas {
			profile.finalTailDeltas = -1
		}

		constant := true
		for _, value := range values {
			profile.maxAbsBits = max(profile.maxAbsBits, bits.Len64(absInt64(value)))
			constant = constant && value == values[0]
		}
		if constant && bits.Len64(absInt64(values[0])) >= 48 {
			profile.highConstantColumns++
			profile.highConstantValueBits = max(profile.highConstantValueBits, bits.Len64(absInt64(values[0])))
		}
	}
	return profile
}

func verifyDeltaBinaryPackedCorpusOutput(tb testing.TB, output []byte, corpus deltaBinaryPackedCorpus) {
	tb.Helper()
	file, err := parquet.OpenFile(bytes.NewReader(output), int64(len(output)))
	if err != nil {
		tb.Fatal(err)
	}
	if got, want := file.NumRows(), int64(len(corpus.rows)); got != want {
		tb.Fatalf("file row count mismatch: want=%d got=%d", want, got)
	}
	if got, want := len(file.RowGroups()), corpus.profile.rowGroups; got != want {
		tb.Fatalf("file row-group count mismatch: want=%d got=%d", want, got)
	}

	int64Columns := 0
	for _, chunk := range file.RowGroups()[0].ColumnChunks() {
		column, ok := chunk.(*parquet.FileColumnChunk)
		if !ok {
			tb.Fatal("file column has an unexpected type")
		}
		if column.Type().Kind() != parquet.Int64 {
			continue
		}
		int64Columns++
		if encoding := column.Node().Encoding(); encoding == nil || encoding.Encoding() != parquet.DeltaBinaryPacked.Encoding() {
			tb.Fatalf("column %d encoding = %v, want %v", column.Column(), encoding, parquet.DeltaBinaryPacked.Encoding())
		}
	}
	if int64Columns != corpus.profile.int64Columns {
		tb.Fatalf("int64 column count = %d, want %d", int64Columns, corpus.profile.int64Columns)
	}

	rows := readDeltaBinaryPackedCorpusRows(tb, file)
	if len(rows) != len(corpus.rows) {
		tb.Fatalf("read %d rows, want %d", len(rows), len(corpus.rows))
	}
	for i := range rows {
		if !sameDeltaBinaryPackedCorpusRow(rows[i], corpus.rows[i]) {
			tb.Fatalf("row %d differs after round trip", i)
		}
	}
}

func sameDeltaBinaryPackedCorpusRow(got, want parquet.Row) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i].Kind() != want[i].Kind() || got[i].Column() != want[i].Column() {
			return false
		}
		switch got[i].Kind() {
		case parquet.Int32:
			if got[i].Int32() != want[i].Int32() {
				return false
			}
		case parquet.Int64:
			if got[i].Int64() != want[i].Int64() {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func absInt64(value int64) uint64 {
	if value >= 0 {
		return uint64(value)
	}
	return uint64(-(value + 1)) + 1
}

func mapsEqual(left, right map[int]int) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if right[key] != value {
			return false
		}
	}
	return true
}

func formatTailHistogram(histogram map[int]int) string {
	tails := make([]int, 0, len(histogram))
	for tail := range histogram {
		tails = append(tails, tail)
	}
	sort.Ints(tails)
	parts := make([]string, len(tails))
	for i, tail := range tails {
		parts[i] = fmt.Sprintf("%d:%d", tail, histogram[tail])
	}
	return strings.Join(parts, ",")
}
