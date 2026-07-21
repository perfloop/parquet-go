package parquet

import (
	"bytes"
	"io"
	"testing"
)

// BenchmarkWriteRowGroupReencodePacked measures the codec-mismatch packed
// segment route. Its input is a sorted merge of file-backed Snappy segments;
// the Zstd destination rules out verbatim copies, so one WriteRowGroup call
// packs all segments through packSegmentsByColumn.
func BenchmarkWriteRowGroupReencodePacked(b *testing.B) {
	const (
		numSegments    = 8
		rowsPerSegment = reencodeValueBufferSize + 1
		maxRows        = numSegments * rowsPerSegment
	)

	rowGroups := make([]RowGroup, numSegments)
	var sourceBytes int64
	for segment := range numSegments {
		rows := makeWideRows(rowsPerSegment)
		for i := range rows {
			rows[i].C0 += int64(segment * rowsPerSegment)
		}

		var data bytes.Buffer
		writer := NewGenericWriter[wideRow](
			&data,
			Compression(&Snappy),
			SortingWriterConfig(SortingColumns(Ascending("C0"))),
			MaxRowsPerRowGroup(rowsPerSegment),
		)
		if _, err := writer.Write(rows); err != nil {
			b.Fatalf("writing source segment %d: %v", segment, err)
		}
		if err := writer.Close(); err != nil {
			b.Fatalf("closing source segment %d: %v", segment, err)
		}

		file, err := OpenFile(bytes.NewReader(data.Bytes()), int64(data.Len()))
		if err != nil {
			b.Fatalf("opening source segment %d: %v", segment, err)
		}
		if got := len(file.RowGroups()); got != 1 {
			b.Fatalf("source segment %d has %d row groups, want 1", segment, got)
		}
		rowGroups[segment] = file.RowGroups()[0]
		sourceBytes += int64(data.Len())
	}

	merged, err := MergeRowGroups(
		rowGroups,
		&RowGroupConfig{Schema: SchemaOf(wideRow{})},
		SortingRowGroupConfig(SortingColumns(Ascending("C0"))),
	)
	if err != nil {
		b.Fatal(err)
	}

	defer func(copyDisabled, reencodeDisabled bool) {
		disableWriteCopy = copyDisabled
		disableWriteReencode = reencodeDisabled
	}(disableWriteCopy, disableWriteReencode)
	disableWriteCopy = false
	disableWriteReencode = false

	b.ReportAllocs()
	b.SetBytes(sourceBytes)
	b.ResetTimer()
	for b.Loop() {
		beforeCopy := copyPathCounter.Load()
		beforeReencode := reencodePathCounter.Load()

		writer := NewGenericWriter[wideRow](
			io.Discard,
			Compression(&Zstd),
			SortingWriterConfig(SortingColumns(Ascending("C0"))),
			MaxRowsPerRowGroup(maxRows),
		)
		if _, err := writer.WriteRowGroup(merged); err != nil {
			b.Fatal(err)
		}
		if err := writer.Close(); err != nil {
			b.Fatal(err)
		}

		if copied := copyPathCounter.Load() - beforeCopy; copied != 0 {
			b.Fatalf("codec mismatch copied %d column chunks, want 0", copied)
		}
		if reencoded := reencodePathCounter.Load() - beforeReencode; reencoded != 1 {
			b.Fatalf("packed L3 writes = %d, want 1", reencoded)
		}
	}
}
