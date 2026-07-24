package parquet_test

import (
	"testing"

	"github.com/parquet-go/parquet-go"
)

func BenchmarkBufferWriteRowsVariableCanonicalByteArrays(b *testing.B) {
	rows, _ := makeBufferWriteRowsByteArrayRows(bufferWriteRowsBatchSize)
	first := rows[0]
	rows[0] = parquet.Row{
		parquet.ValueOf(first[0].ByteArray()[:1]).Level(0, 0, 0),
		parquet.ValueOf(first[1].ByteArray()[:1]).Level(0, 0, 1),
	}
	benchmarkBufferWriteRowsByteArrays(b, rows)
}
