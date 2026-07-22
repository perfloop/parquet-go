package parquet_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/variant"
)

// plainByteArrayVariantFixture is an in-memory VARIANT file whose projected
// string leaf is deliberately plain encoded. The payloads vary by row and are
// consumed by the benchmarks so the scan cannot be optimized away.
type plainByteArrayVariantFixture struct {
	data     []byte
	payloads [][]byte
	checksum uint64
}

func newPlainByteArrayVariantFixture(tb testing.TB, numRows, payloadSize, pageBufferSize int) plainByteArrayVariantFixture {
	tb.Helper()

	shredded, err := parquet.ShreddedVariant(parquet.Group{
		"s": parquet.String(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	schema := parquet.NewSchema("table", parquet.Group{
		"id":  parquet.Int(32),
		"var": parquet.Optional(shredded),
	})

	payloads := make([][]byte, numRows)
	rows := make([]shreddedVariantRow, numRows)
	var checksum uint64
	for i := range rows {
		payload := make([]byte, payloadSize)
		for j := range payload {
			payload[j] = byte('a' + (i*31+j*17+payloadSize)%26)
		}
		payload[0] = byte('a' + i%26)
		payload[len(payload)-1] = byte('a' + (i*7+13)%26)
		payloads[i] = payload
		checksum = plainByteArrayChecksum(checksum, payload)
		rows[i] = shreddedVariantRow{
			ID: int32(i),
			Var: encodeRawVariant(variant.MakeObject([]variant.Field{{
				Name:  "s",
				Value: variant.String(string(payload)),
			}})),
		}
	}

	var out bytes.Buffer
	writer := parquet.NewGenericWriter[shreddedVariantRow](
		&out,
		schema,
		parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.Plain),
		parquet.PageBufferSize(pageBufferSize),
	)
	if _, err := writer.Write(rows); err != nil {
		tb.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		tb.Fatal(err)
	}

	return plainByteArrayVariantFixture{
		data:     out.Bytes(),
		payloads: payloads,
		checksum: checksum,
	}
}

func plainByteArrayChecksum(sum uint64, value []byte) uint64 {
	return sum*0x9e3779b97f4a7c15 + uint64(value[0])<<8 + uint64(value[len(value)-1])
}

func plainByteArrayWindowChecksum(sum uint64, slab []byte, offsets []uint32) uint64 {
	for i := 0; i+1 < len(offsets); i++ {
		sum = plainByteArrayChecksum(sum, slab[offsets[i]:offsets[i+1]])
	}
	return sum
}

func benchmarkVariantReaderPlainByteArrayWindow(b *testing.B, numRows, payloadSize, pageBufferSize, windowSize int) {
	fixture := newPlainByteArrayVariantFixture(b, numRows, payloadSize, pageBufferSize)
	file, err := parquet.OpenFile(bytes.NewReader(fixture.data), int64(len(fixture.data)))
	if err != nil {
		b.Fatal(err)
	}
	reader, err := parquet.NewVariantReader(file.RowGroups()[0], "var")
	if err != nil {
		b.Fatal(err)
	}
	cursor := reader.Path("s")
	b.Cleanup(func() {
		if err := reader.Close(); err != nil {
			b.Error(err)
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := reader.SeekToRow(0); err != nil {
			b.Fatal(err)
		}
		var checksum uint64
		for {
			_, err := reader.Next(windowSize)
			if err == io.EOF {
				break
			}
			if err != nil {
				b.Fatal(err)
			}
			slab, offsets := cursor.ByteArrays()
			checksum = plainByteArrayWindowChecksum(checksum, slab, offsets)
		}
		if checksum != fixture.checksum {
			b.Fatalf("checksum = %x, want %x", checksum, fixture.checksum)
		}
	}
}

// BenchmarkVariantReaderPlainByteArraySinglePageWindow scans 4 MiB of
// runtime-generated plain STRING data in 256-row windows. The 8 MiB page
// buffer keeps every requested window in one typed-value page.
func BenchmarkVariantReaderPlainByteArraySinglePageWindow(b *testing.B) {
	benchmarkVariantReaderPlainByteArrayWindow(b, 4096, 1024, 8<<20, 256)
}

// BenchmarkVariantReaderPlainByteArrayCrossPageWindow exercises the fallback
// path where a 256-row window spans several small typed-value pages.
func BenchmarkVariantReaderPlainByteArrayCrossPageWindow(b *testing.B) {
	benchmarkVariantReaderPlainByteArrayWindow(b, 1024, 1024, 4<<10, 256)
}

// BenchmarkVariantReaderPlainByteArrayPayloadSweep is a controlled workload
// check: with the same row count and single-page windowing, the large payload
// case must remain more expensive than the small case on both proof arms.
func BenchmarkVariantReaderPlainByteArrayPayloadSweep(b *testing.B) {
	b.Run("payload_128", func(b *testing.B) {
		benchmarkVariantReaderPlainByteArrayWindow(b, 1024, 128, 8<<20, 256)
	})
	b.Run("payload_4096", func(b *testing.B) {
		benchmarkVariantReaderPlainByteArrayWindow(b, 1024, 4096, 8<<20, 256)
	})
}
