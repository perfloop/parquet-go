package parquet_test

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type sortingWriterInMemoryTwoColumnRow struct {
	Key     int64 `parquet:"key"`
	Payload int64 `parquet:"payload"`
}

type sortingWriterInMemoryPayloadKeyRow struct {
	Payload int64 `parquet:"payload"`
	Key     int64 `parquet:"key"`
}

type sortingWriterInMemoryOneColumnRow struct {
	Key int64 `parquet:"key"`
}

type sortingWriterInMemoryWideRow struct {
	Payload0 int64 `parquet:"payload0"`
	Payload1 int64 `parquet:"payload1"`
	Payload2 int64 `parquet:"payload2"`
	Payload3 int64 `parquet:"payload3"`
	Payload4 int64 `parquet:"payload4"`
	Payload5 int64 `parquet:"payload5"`
	Payload6 int64 `parquet:"payload6"`
	Key      int64 `parquet:"key"`
}

func makeSortingWriterInMemoryTwoColumnRows(count, base int) []sortingWriterInMemoryTwoColumnRow {
	rows := make([]sortingWriterInMemoryTwoColumnRow, count)
	for i := range rows {
		key := int64(base + (i*7919)%count)
		rows[i] = sortingWriterInMemoryTwoColumnRow{
			Key:     key,
			Payload: int64((base+i)*104729 + 17),
		}
	}
	return rows
}

func makeSortingWriterInMemoryPayloadKeyRows(count int) []sortingWriterInMemoryPayloadKeyRow {
	rows := make([]sortingWriterInMemoryPayloadKeyRow, count)
	for i := range rows {
		rows[i] = sortingWriterInMemoryPayloadKeyRow{
			Payload: int64(i*104729 + 17),
			Key:     int64((i * 7919) % count),
		}
	}
	return rows
}

func makeSortingWriterInMemoryWideRows(count int) []sortingWriterInMemoryWideRow {
	rows := make([]sortingWriterInMemoryWideRow, count)
	for i := range rows {
		payload := int64(i*104729 + 17)
		rows[i] = sortingWriterInMemoryWideRow{
			Payload0: payload,
			Payload1: payload + 1,
			Payload2: payload + 2,
			Payload3: payload + 3,
			Payload4: payload + 4,
			Payload5: payload + 5,
			Payload6: payload + 6,
			Key:      int64((i * 7919) % count),
		}
	}
	return rows
}

func sortingWriterLiveHeap[T any](b *testing.B, rows []T, sortRowCount int64, options ...parquet.WriterOption) {
	b.Helper()

	options = append(options, parquet.SortingWriterConfig(
		parquet.SortingColumns(parquet.Ascending("key")),
	))
	b.ReportAllocs()
	b.ResetTimer()

	var (
		precloseHeap int64
		retainedHeap int64
	)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		b.StartTimer()
		var output bytes.Buffer
		writer := parquet.NewSortingWriter[T](&output, sortRowCount, options...)
		if n, err := writer.Write(rows); err != nil {
			b.Fatal(err)
		} else if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		var preclose runtime.MemStats
		runtime.ReadMemStats(&preclose)
		precloseHeap = int64(preclose.HeapAlloc) - int64(before.HeapAlloc)

		runtime.GC()
		var retained runtime.MemStats
		runtime.ReadMemStats(&retained)
		retainedHeap = int64(retained.HeapAlloc) - int64(before.HeapAlloc)

		b.StartTimer()
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		if output.Len() == 0 {
			b.Fatal("sorting writer produced no parquet output")
		}
		runtime.KeepAlive(rows)
		runtime.KeepAlive(output)
	}
	b.ReportMetric(float64(precloseHeap), "preclose_heap_B")
	b.ReportMetric(float64(retainedHeap), "retained_heap_B")
}

func BenchmarkSortingWriterRetainedHeapTwoColumns(b *testing.B) {
	const (
		rowCount           = 65536
		sortRowCount int64 = 256
	)
	b.StopTimer()
	rows := makeSortingWriterInMemoryTwoColumnRows(rowCount, 0)
	sortingWriterLiveHeap(b, rows, sortRowCount)
}

func BenchmarkSortingWriterRetainedHeapWideColumns(b *testing.B) {
	const (
		rowCount           = 65536
		sortRowCount int64 = 256
	)
	b.StopTimer()
	rows := makeSortingWriterInMemoryWideRows(rowCount)
	sortingWriterLiveHeap(b, rows, sortRowCount)
}

func BenchmarkSortingWriterRetainedHeapMaxRuns(b *testing.B) {
	b.StopTimer()
	rows := makeSortingWriterInMemoryTwoColumnRows(parquet.MaxRowGroups, 0)
	sortingWriterLiveHeap(b, rows, 1)
}

func BenchmarkSortingWriterNonOverlappingInt64Runs(b *testing.B) {
	const (
		rowCount           = 65536
		sortRowCount int64 = 256
	)
	b.StopTimer()
	rows := make([]sortingWriterInMemoryTwoColumnRow, rowCount)
	for i := range rows {
		rows[i] = sortingWriterInMemoryTwoColumnRow{
			Key:     int64(i),
			Payload: int64(i*104729 + 17),
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	b.StartTimer()

	for b.Loop() {
		var output bytes.Buffer
		writer := parquet.NewSortingWriter[sortingWriterInMemoryTwoColumnRow](
			&output,
			sortRowCount,
			parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending("key")),
			),
		)
		if n, err := writer.Write(rows); err != nil {
			b.Fatal(err)
		} else if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}
		if output.Len() == 0 {
			b.Fatal("sorting writer produced no parquet output")
		}
	}
}

func TestSortingWriterInMemoryInt64RunResetAndWriteRows(t *testing.T) {
	const sortRowCount int64 = 3
	first := makeSortingWriterInMemoryTwoColumnRows(12, 0)
	second := makeSortingWriterInMemoryTwoColumnRows(8, 100)

	var discarded, output bytes.Buffer
	writer := parquet.NewSortingWriter[sortingWriterInMemoryTwoColumnRow](
		&discarded,
		sortRowCount,
		parquet.SortingWriterConfig(
			parquet.SortingBuffers(parquet.NewBufferPool()),
			parquet.SortingColumns(parquet.Ascending("key")),
		),
	)
	if n, err := writer.Write(first); err != nil {
		t.Fatal(err)
	} else if n != len(first) {
		t.Fatalf("wrote %d rows, want %d", n, len(first))
	}
	if err := writer.Flush(); err != nil {
		t.Fatal(err)
	}

	writer.Reset(&output)

	schema := parquet.SchemaOf(sortingWriterInMemoryTwoColumnRow{})
	rows := make([]parquet.Row, len(second))
	for i := range second {
		rows[i] = schema.Deconstruct(nil, second[i])
	}
	if n, err := writer.WriteRows(rows); err != nil {
		t.Fatal(err)
	} else if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := parquet.Read[sortingWriterInMemoryTwoColumnRow](bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	want := make(map[int64]sortingWriterInMemoryTwoColumnRow, len(second))
	for _, row := range second {
		want[row.Key] = row
	}
	assertSortingWriterInMemoryTwoColumnRows(t, got, want)
}

func TestSortingWriterInMemoryInt64RunEligibleLayouts(t *testing.T) {
	t.Run("one-column", func(t *testing.T) {
		input := []sortingWriterInMemoryOneColumnRow{
			{Key: 3}, {Key: 9}, {Key: 1}, {Key: 7},
			{Key: 0}, {Key: 8}, {Key: 2}, {Key: 10},
			{Key: 4}, {Key: 11}, {Key: 5}, {Key: 6},
		}
		var output bytes.Buffer
		writer := parquet.NewSortingWriter[sortingWriterInMemoryOneColumnRow](
			&output,
			3,
			parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending("key")),
			),
		)
		if n, err := writer.Write(input); err != nil {
			t.Fatal(err)
		} else if n != len(input) {
			t.Fatalf("wrote %d rows, want %d", n, len(input))
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}
		got, err := parquet.Read[sortingWriterInMemoryOneColumnRow](bytes.NewReader(output.Bytes()), int64(output.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(input) {
			t.Fatalf("read %d rows, want %d", len(got), len(input))
		}
		for i, row := range got {
			if row.Key != int64(i) {
				t.Fatalf("row %d has key %d, want %d", i, row.Key, i)
			}
		}
	})

	t.Run("wide-key-last-write-rows", func(t *testing.T) {
		input := makeSortingWriterInMemoryWideRows(32)
		schema := parquet.SchemaOf(sortingWriterInMemoryWideRow{})
		rows := make([]parquet.Row, len(input))
		want := make(map[int64]sortingWriterInMemoryWideRow, len(input))
		for i := range input {
			rows[i] = schema.Deconstruct(nil, input[i])
			want[input[i].Key] = input[i]
		}

		var output bytes.Buffer
		writer := parquet.NewSortingWriter[sortingWriterInMemoryWideRow](
			&output,
			5,
			parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending("key")),
			),
		)
		if n, err := writer.WriteRows(rows); err != nil {
			t.Fatal(err)
		} else if n != len(rows) {
			t.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Close(); err != nil {
			t.Fatal(err)
		}

		got, err := parquet.Read[sortingWriterInMemoryWideRow](bytes.NewReader(output.Bytes()), int64(output.Len()))
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(input) {
			t.Fatalf("read %d rows, want %d", len(got), len(input))
		}
		for i, row := range got {
			if i > 0 && got[i-1].Key >= row.Key {
				t.Fatalf("rows %d and %d are not in ascending key order", i-1, i)
			}
			if want[row.Key] != row {
				t.Fatalf("row %d = %+v, want %+v", i, row, want[row.Key])
			}
		}
	})
}

func TestSortingWriterInMemoryInt64RunWriteRowsValidation(t *testing.T) {
	input := makeSortingWriterInMemoryPayloadKeyRows(12)
	schema := parquet.SchemaOf(sortingWriterInMemoryPayloadKeyRow{})
	valid := make([]parquet.Row, len(input))
	want := make(map[int64]sortingWriterInMemoryPayloadKeyRow, len(input))
	for i := range input {
		valid[i] = schema.Deconstruct(nil, input[i])
		want[input[i].Key] = input[i]
	}

	for _, test := range []struct {
		name string
		rows []parquet.Row
	}{
		{
			name: "conflicting-column-index",
			rows: []parquet.Row{{
				parquet.Int64Value(2).Level(0, 0, 1),
				parquet.Int64Value(10).Level(0, 0, 0),
			}},
		},
		{
			name: "unindexed-value",
			rows: []parquet.Row{{
				parquet.Int64Value(2),
				parquet.Int64Value(10).Level(0, 0, 1),
			}},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			var output bytes.Buffer
			writer := parquet.NewSortingWriter[sortingWriterInMemoryPayloadKeyRow](
				&output,
				3,
				parquet.SortingWriterConfig(
					parquet.SortingColumns(parquet.Ascending("key")),
				),
			)
			if n, err := writer.WriteRows(test.rows); err == nil || n != 0 {
				t.Fatalf("WriteRows() = (%d, %v), want (0, validation error)", n, err)
			}
			if n, err := writer.WriteRows(valid); err != nil {
				t.Fatal(err)
			} else if n != len(valid) {
				t.Fatalf("wrote %d rows, want %d", n, len(valid))
			}
			if err := writer.Close(); err != nil {
				t.Fatal(err)
			}
			got, err := parquet.Read[sortingWriterInMemoryPayloadKeyRow](bytes.NewReader(output.Bytes()), int64(output.Len()))
			if err != nil {
				t.Fatal(err)
			}
			assertSortingWriterInMemoryPayloadKeyRows(t, got, want)
		})
	}
}

func TestSortingWriterInMemoryInt64RunsMaxRowGroups(t *testing.T) {
	rows := make([]sortingWriterInMemoryTwoColumnRow, parquet.MaxRowGroups+1)
	for i := range rows {
		rows[i] = sortingWriterInMemoryTwoColumnRow{Key: int64(i), Payload: int64(i*104729 + 17)}
	}

	var output bytes.Buffer
	writer := parquet.NewSortingWriter[sortingWriterInMemoryTwoColumnRow](
		&output,
		1,
		parquet.SortingWriterConfig(
			parquet.SortingColumns(parquet.Ascending("key")),
		),
	)
	if n, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	} else if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := writer.Flush(); !errors.Is(err, parquet.ErrTooManyRowGroups) {
		t.Fatalf("Flush() error = %v, want %v", err, parquet.ErrTooManyRowGroups)
	}
	writer.Reset(&bytes.Buffer{})
}

func assertSortingWriterInMemoryTwoColumnRows(t *testing.T, got []sortingWriterInMemoryTwoColumnRow, want map[int64]sortingWriterInMemoryTwoColumnRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}
	for i, row := range got {
		if i > 0 && got[i-1].Key >= row.Key {
			t.Fatalf("rows %d and %d are not in ascending key order", i-1, i)
		}
		if want[row.Key] != row {
			t.Fatalf("row %d = %+v, want %+v", i, row, want[row.Key])
		}
	}
}

func assertSortingWriterInMemoryPayloadKeyRows(t *testing.T, got []sortingWriterInMemoryPayloadKeyRow, want map[int64]sortingWriterInMemoryPayloadKeyRow) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("read %d rows, want %d", len(got), len(want))
	}
	for i, row := range got {
		if i > 0 && got[i-1].Key >= row.Key {
			t.Fatalf("rows %d and %d are not in ascending key order", i-1, i)
		}
		if want[row.Key] != row {
			t.Fatalf("row %d = %+v, want %+v", i, row, want[row.Key])
		}
	}
}
