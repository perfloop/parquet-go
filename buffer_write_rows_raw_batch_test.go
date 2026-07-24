package parquet_test

import (
	"errors"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const (
	bufferWriteRowsBatchSize    = 1024
	bufferWriteRowsPayloadSize  = 64
	bufferWriteRowsResetBatches = 8
)

var bufferWriteRowsByteArraySchema = parquet.NewSchema("buffer_write_rows", parquet.Group{
	"first":  parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
	"second": parquet.Required(parquet.Leaf(parquet.ByteArrayType)),
})

func makeBufferWriteRowsByteArrayRows(n int) ([]parquet.Row, []byte) {
	payloads := make([]byte, n*2*bufferWriteRowsPayloadSize)
	rows := make([]parquet.Row, n)

	for i := range rows {
		firstStart := (2 * i) * bufferWriteRowsPayloadSize
		secondStart := firstStart + bufferWriteRowsPayloadSize
		first := payloads[firstStart : firstStart+bufferWriteRowsPayloadSize]
		second := payloads[secondStart : secondStart+bufferWriteRowsPayloadSize]

		for j := range first {
			first[j] = byte(i + j)
			second[j] = byte(i*3 + j)
		}

		rows[i] = parquet.Row{
			parquet.ValueOf(first).Level(0, 0, 0),
			parquet.ValueOf(second).Level(0, 0, 1),
		}
	}

	return rows, payloads
}

func cloneBufferWriteRows(rows []parquet.Row) []parquet.Row {
	cloned := make([]parquet.Row, len(rows))
	for i := range rows {
		cloned[i] = rows[i].Clone()
	}
	return cloned
}

func TestBufferWriteRowsRawByteArrayBatches(t *testing.T) {
	for _, test := range []struct {
		name              string
		finalNoncanonical bool
	}{
		{name: "canonical"},
		{name: "final_noncanonical", finalNoncanonical: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			rows, payloads := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
			want := cloneBufferWriteRows(rows)

			if test.finalNoncanonical {
				last := rows[len(rows)-1]
				rows[len(rows)-1] = parquet.Row{last[1], last[0]}
			}

			buffer := parquet.NewBuffer(bufferWriteRowsByteArraySchema)
			n, err := buffer.WriteRows(rows)
			if err != nil {
				t.Fatal(err)
			}
			if n != len(rows) {
				t.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
			}

			for i := range payloads {
				payloads[i] ^= 0xff
			}

			reader := buffer.Rows()
			got := make([]parquet.Row, len(want))
			n, err = reader.ReadRows(got)
			if closeErr := reader.Close(); closeErr != nil {
				t.Fatal(closeErr)
			}
			if err != nil && !errors.Is(err, io.EOF) {
				t.Fatal(err)
			}
			if n != len(want) {
				t.Fatalf("Rows read %d rows, want %d", n, len(want))
			}

			for i := range want {
				if !got[i].Equal(want[i]) {
					t.Fatalf("row %d mismatch:\n got: %#v\nwant: %#v", i, got[i], want[i])
				}
			}
		})
	}
}

func benchmarkBufferWriteRowsByteArrays(b *testing.B, rows []parquet.Row) {
	buffer := parquet.NewBuffer(bufferWriteRowsByteArraySchema)
	batchesSinceReset := 0

	b.ReportAllocs()
	for b.Loop() {
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows wrote %d rows, want %d", n, len(rows))
		}

		batchesSinceReset++
		if batchesSinceReset == bufferWriteRowsResetBatches {
			buffer.Reset()
			batchesSinceReset = 0
		}
	}

	if got, want := buffer.NumRows(), int64(batchesSinceReset*len(rows)); got != want {
		b.Fatalf("buffer holds %d rows, want %d", got, want)
	}
}

func BenchmarkBufferWriteRowsCanonicalByteArrays(b *testing.B) {
	rows, _ := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
	benchmarkBufferWriteRowsByteArrays(b, rows)
}

func BenchmarkBufferWriteRowsFinalNoncanonicalByteArrays(b *testing.B) {
	rows, _ := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
	last := rows[len(rows)-1]
	rows[len(rows)-1] = parquet.Row{last[1], last[0]}
	benchmarkBufferWriteRowsByteArrays(b, rows)
}
