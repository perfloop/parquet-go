package parquet

import (
	"bytes"
	"io"
	"testing"
)

// BenchmarkWriteRowGroupV1Fallback measures the 200k-row codec rewrite when
// a DataPageV1 source is rewritten as DataPageV2. The page-version mismatch
// retains the decoded-value L3 path rather than raw-page transcoding.
func BenchmarkWriteRowGroupV1Fallback(b *testing.B) {
	const numRows = 200_000
	rows := makeBenchRows(numRows)

	var src bytes.Buffer
	sw := NewGenericWriter[benchRow](
		&src,
		Compression(&Snappy),
		DataPageVersion(1),
		MaxRowsPerRowGroup(50_000),
	)
	if _, err := sw.Write(rows); err != nil {
		b.Fatal(err)
	}
	if err := sw.Close(); err != nil {
		b.Fatal(err)
	}
	file, err := OpenFile(bytes.NewReader(src.Bytes()), int64(src.Len()))
	if err != nil {
		b.Fatal(err)
	}
	rowGroups := file.RowGroups()

	b.ReportAllocs()
	b.SetBytes(int64(src.Len()))
	b.ResetTimer()
	for b.Loop() {
		w := NewGenericWriter[benchRow](
			io.Discard,
			Compression(&Zstd),
			DataPageVersion(2),
			MaxRowsPerRowGroup(50_000),
		)
		for _, rowGroup := range rowGroups {
			if _, err := w.WriteRowGroup(rowGroup); err != nil {
				b.Fatal(err)
			}
		}
		if err := w.Close(); err != nil {
			b.Fatal(err)
		}
	}
}
