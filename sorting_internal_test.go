package parquet

import (
	"io"
	"testing"
)

func TestInMemoryInt64RunsByteLimit(t *testing.T) {
	runs := inMemoryInt64Runs{columns: 2}
	row := Row{Int64Value(1).Level(0, 0, 0), Int64Value(2).Level(0, 0, 1)}

	runs.bytes = maxInMemoryInt64RunBytes - 16
	if runs.shouldMaterialize([]Row{row}) {
		t.Fatal("run at the byte limit materialized")
	}

	runs.bytes++
	if !runs.shouldMaterialize([]Row{row}) {
		t.Fatal("run over the byte limit stayed compact")
	}
}

func TestSortingWriterSkipsCompactRunsForLeafCompression(t *testing.T) {
	schema := NewSchema("test", Group{
		"payload": Required(Leaf(Int64Type)),
		"key":     Required(Compressed(Leaf(Int64Type), &Snappy)),
	})
	writer := NewSortingWriter[any](io.Discard, 2,
		schema,
		SortingWriterConfig(SortingColumns(Ascending("key"))),
	)
	if writer.inMemoryInt64Runs != nil {
		t.Fatal("compressed leaf selected compact runs")
	}
	if writer.writer == nil {
		t.Fatal("compressed leaf did not select the temporary writer")
	}
}
