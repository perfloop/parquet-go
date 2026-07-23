package parquet_test

import (
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/parquet-go/parquet-go"
)

type bufferWriteRowsByteArrayRecord struct {
	A []byte
	B []byte
	C []byte
	D []byte
	E []byte
	F []byte
	G []byte
	H []byte
}

func TestBufferWriteRowsRawRows(t *testing.T) {
	t.Run("canonical byte arrays retain payload ownership", func(t *testing.T) {
		schema := parquet.SchemaOf(bufferWriteRowsByteArrayRecord{})
		want := []bufferWriteRowsByteArrayRecord{
			makeBufferWriteRowsByteArrayRecord(0, 17),
			makeBufferWriteRowsByteArrayRecord(1, 31),
			makeBufferWriteRowsByteArrayRecord(2, 47),
		}
		rows := make([]parquet.Row, len(want))
		for i := range want {
			rows[i] = schema.Deconstruct(nil, want[i])
		}

		buffer := parquet.NewBuffer(schema)
		if n, err := buffer.WriteRows(rows); err != nil || n != len(rows) {
			t.Fatalf("WriteRows() = %d, %v; want %d, nil", n, err, len(rows))
		}

		// Buffer must retain its own byte-array payloads after accepting raw rows.
		// The direct span path may remove temporary Value copies, but not this copy.
		for i := range want[0].A {
			want[0].A[i] ^= 0xFF
		}

		gotRows := readBufferRows(t, buffer, len(rows))
		got := make([]bufferWriteRowsByteArrayRecord, len(gotRows))
		for i := range gotRows {
			if err := schema.Reconstruct(&got[i], gotRows[i]); err != nil {
				t.Fatalf("Reconstruct(%d): %v", i, err)
			}
		}

		// Undo the input mutation to make want the immutable pre-write value.
		for i := range want[0].A {
			want[0].A[i] ^= 0xFF
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip mismatch:\nwant: %#v\n got: %#v", want, got)
		}
	})

	t.Run("noncanonical column order", func(t *testing.T) {
		type record struct {
			First  []byte
			Second []byte
		}

		schema := parquet.SchemaOf(record{})
		want := []record{
			{First: []byte("first-0"), Second: []byte("second-0")},
			{First: []byte("first-1"), Second: []byte("second-1")},
		}
		rows := []parquet.Row{
			{
				parquet.ByteArrayValue(want[0].Second).Level(0, 0, 1),
				parquet.ByteArrayValue(want[0].First).Level(0, 0, 0),
			},
			{
				parquet.ByteArrayValue(want[1].Second).Level(0, 0, 1),
				parquet.ByteArrayValue(want[1].First).Level(0, 0, 0),
			},
		}

		buffer := parquet.NewBuffer(schema)
		if n, err := buffer.WriteRows(rows); err != nil || n != len(rows) {
			t.Fatalf("WriteRows() = %d, %v; want %d, nil", n, err, len(rows))
		}

		gotRows := readBufferRows(t, buffer, len(rows))
		got := make([]record, len(gotRows))
		for i := range gotRows {
			if err := schema.Reconstruct(&got[i], gotRows[i]); err != nil {
				t.Fatalf("Reconstruct(%d): %v", i, err)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip mismatch:\nwant: %#v\n got: %#v", want, got)
		}
	})

	t.Run("optional and repeated rows", func(t *testing.T) {
		type record struct {
			Required []byte
			Optional *string
			Repeated []string
		}

		first := "first"
		second := "second"
		want := []record{
			{Required: []byte("required-0"), Optional: &first, Repeated: []string{"red", "green"}},
			{Required: []byte("required-1"), Optional: nil, Repeated: []string{"blue"}},
			{Required: []byte("required-2"), Optional: &second, Repeated: []string{"orange", "purple", "yellow"}},
		}
		schema := parquet.SchemaOf(record{})
		rows := make([]parquet.Row, len(want))
		for i := range want {
			rows[i] = schema.Deconstruct(nil, want[i])
		}

		buffer := parquet.NewBuffer(schema)
		if n, err := buffer.WriteRows(rows); err != nil || n != len(rows) {
			t.Fatalf("WriteRows() = %d, %v; want %d, nil", n, err, len(rows))
		}

		gotRows := readBufferRows(t, buffer, len(rows))
		got := make([]record, len(gotRows))
		for i := range gotRows {
			if err := schema.Reconstruct(&got[i], gotRows[i]); err != nil {
				t.Fatalf("Reconstruct(%d): %v", i, err)
			}
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip mismatch:\nwant: %#v\n got: %#v", want, got)
		}
	})
}

func BenchmarkBufferWriteRowsCanonicalByteArrays(b *testing.B) {
	const (
		rowsPerBatch = 1024
		payloadBytes = 64
		batches      = 8
	)

	schema := parquet.SchemaOf(bufferWriteRowsByteArrayRecord{})
	rows := make([]parquet.Row, rowsPerBatch)
	for i := range rows {
		rows[i] = schema.Deconstruct(nil, makeBufferWriteRowsByteArrayRecord(i, payloadBytes))
	}

	buffer := parquet.NewBuffer(schema)
	b.SetBytes(int64(rowsPerBatch * payloadBytes * 8))

	batchesWritten := 0
	for b.Loop() {
		n, err := buffer.WriteRows(rows)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(rows) {
			b.Fatalf("WriteRows() = %d; want %d", n, len(rows))
		}

		batchesWritten++
		if batchesWritten == batches {
			if n := buffer.NumRows(); n != int64(rowsPerBatch*batches) {
				b.Fatalf("NumRows() = %d; want %d", n, rowsPerBatch*batches)
			}
			buffer.Reset()
			batchesWritten = 0
		}
	}
}

func makeBufferWriteRowsByteArrayRecord(row, size int) bufferWriteRowsByteArrayRecord {
	return bufferWriteRowsByteArrayRecord{
		A: makeBufferWriteRowsPayload(row, 0, size),
		B: makeBufferWriteRowsPayload(row, 1, size),
		C: makeBufferWriteRowsPayload(row, 2, size),
		D: makeBufferWriteRowsPayload(row, 3, size),
		E: makeBufferWriteRowsPayload(row, 4, size),
		F: makeBufferWriteRowsPayload(row, 5, size),
		G: makeBufferWriteRowsPayload(row, 6, size),
		H: makeBufferWriteRowsPayload(row, 7, size),
	}
}

func makeBufferWriteRowsPayload(row, column, size int) []byte {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte(row*31 + column*17 + i)
	}
	return payload
}

func readBufferRows(t *testing.T, buffer *parquet.Buffer, numRows int) []parquet.Row {
	t.Helper()

	rows := buffer.Rows()
	defer rows.Close()

	result := make([]parquet.Row, numRows)
	n, err := rows.ReadRows(result)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ReadRows(): %v", err)
	}
	if n != len(result) {
		t.Fatalf("ReadRows() = %d rows; want %d", n, len(result))
	}
	return result
}
