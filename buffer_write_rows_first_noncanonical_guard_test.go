package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkBufferWriteRowsFirstNoncanonicalByteArrays(b *testing.B) {
	rows, _ := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
	first := rows[0]
	rows[0] = parquet.Row{first[1], first[0]}
	benchmarkBufferWriteRowsByteArrays(b, rows)
}
