package parquet

import "testing"

var bufferWriteRowsEmptyThirdFallbackSchema = NewSchema("buffer_write_rows_empty_third_fallback", Group{
	"first":  Required(Leaf(ByteArrayType)),
	"second": Required(Leaf(ByteArrayType)),
})

func makeBufferWriteRowsEmptyThirdFallbackRows() []Row {
	rows := make([]Row, 1024)
	for i := range rows {
		rows[i] = Row{
			ValueOf([]byte{}).Level(0, 0, 0),
			ValueOf([]byte{}).Level(0, 0, 1),
		}
	}
	third := rows[2]
	rows[2] = Row{third[1], third[0]}
	return rows
}

func BenchmarkBufferWriteRowsEmptyThirdFallbackByteArrays(b *testing.B) {
	rows := makeBufferWriteRowsEmptyThirdFallbackRows()
	firstValuesCap := 0
	secondValuesCap := 0

	b.ReportAllocs()
	for b.Loop() {
		buffer := NewBuffer(bufferWriteRowsEmptyThirdFallbackSchema)
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}
		if got := buffer.NumRows(); got != int64(len(rows)) {
			b.Fatalf("buffer holds %d rows, want %d", got, len(rows))
		}

		firstValuesCap = buffer.columns[0].(*byteArrayColumnBuffer).values.Cap()
		secondValuesCap = buffer.columns[1].(*byteArrayColumnBuffer).values.Cap()
	}
	b.ReportMetric(float64(firstValuesCap), "first-values-cap")
	b.ReportMetric(float64(secondValuesCap), "second-values-cap")
}
