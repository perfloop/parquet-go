package parquet_test

import (
	"bytes"
	"runtime"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func inUseProfileBytes() int64 {
	for {
		n, _ := runtime.MemProfile(nil, true)
		records := make([]runtime.MemProfileRecord, n)
		n, ok := runtime.MemProfile(records, true)
		if !ok {
			continue
		}

		var bytes int64
		for _, record := range records[:n] {
			bytes += record.InUseBytes()
		}
		return bytes
	}
}

// BenchmarkSortingWriterRetainedProfileOverlappingMaxRuns samples in-use heap
// profile bytes after all one-row runs flush and before Close releases them.
func BenchmarkSortingWriterRetainedProfileOverlappingMaxRuns(b *testing.B) {
	b.StopTimer()
	rows := make([]sortingWriterInMemoryTwoColumnRow, parquet.MaxRowGroups)
	for i := range rows {
		rows[i] = sortingWriterInMemoryTwoColumnRow{
			Key:     int64(i % 256),
			Payload: int64(i*104729 + 17),
		}
	}

	// Heap profiling estimates the retained set and keeps this resource metric
	// independently sampleable; retained_heap_B reports the direct MemStats view.
	memProfileRate := runtime.MemProfileRate
	runtime.MemProfileRate = 64 * 1024
	defer func() { runtime.MemProfileRate = memProfileRate }()

	b.ReportAllocs()
	b.ResetTimer()

	var (
		profileBytes int64
		retainedHeap int64
	)
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		runtime.GC()
		beforeProfile := inUseProfileBytes()

		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		b.StartTimer()
		var output bytes.Buffer
		writer := parquet.NewSortingWriter[sortingWriterInMemoryTwoColumnRow](
			&output,
			1,
			parquet.SortingWriterConfig(
				parquet.SortingColumns(parquet.Ascending("key")),
			),
		)
		if n, err := writer.Write(rows); err != nil {
			b.Fatal(err)
		} else if n != len(rows) {
			b.Fatalf("wrote %d rows, want %d", n, len(rows))
		}
		if err := writer.Flush(); err != nil {
			b.Fatal(err)
		}
		b.StopTimer()

		runtime.GC()
		profileBytes = inUseProfileBytes() - beforeProfile

		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		retainedHeap = int64(after.HeapAlloc) - int64(before.HeapAlloc)

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
	b.ReportMetric(float64(profileBytes), "retained_profile_B")
	b.ReportMetric(float64(retainedHeap), "retained_heap_B")
}
