package parquet

import (
	"bytes"
	"fmt"
	"io"
	"sync"
	"testing"
)

type staleCursorBufferPool struct {
	mu   sync.Mutex
	gets int
	puts int
}

func (p *staleCursorBufferPool) GetBuffer() io.ReadWriteSeeker {
	p.mu.Lock()
	p.gets++
	p.mu.Unlock()
	return &staleCursorBuffer{data: []byte{0xFF}, offset: 1}
}

func (p *staleCursorBufferPool) PutBuffer(io.ReadWriteSeeker) {
	p.mu.Lock()
	p.puts++
	p.mu.Unlock()
}

type staleCursorBuffer struct {
	data   []byte
	offset int64
}

func (b *staleCursorBuffer) Read(p []byte) (int, error) {
	if b.offset >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[b.offset:])
	b.offset += int64(n)
	return n, nil
}

func (b *staleCursorBuffer) Write(p []byte) (int, error) {
	end := b.offset + int64(len(p))
	if end > int64(len(b.data)) {
		b.data = append(b.data, make([]byte, int(end)-len(b.data))...)
	}
	copy(b.data[b.offset:end], p)
	b.offset = end
	return len(p), nil
}

func (b *staleCursorBuffer) Seek(offset int64, whence int) (int64, error) {
	next := offset
	switch whence {
	case io.SeekStart:
	case io.SeekCurrent:
		next += b.offset
	case io.SeekEnd:
		next += int64(len(b.data))
	default:
		return 0, fmt.Errorf("invalid seek whence %d", whence)
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek offset %d", next)
	}
	b.offset = next
	return next, nil
}

func TestWriterBloomHashJournalResetsBufferCursor(t *testing.T) {
	pool := new(staleCursorBufferPool)
	output := new(bytes.Buffer)
	writer := NewWriter(output,
		NewSchema("row", Group{"value": String()}),
		BloomFilters(SplitBlockFilter(10, "value")),
		DefaultEncoding(&Plain),
		PageBufferSize(64),
		WriteBufferSize(0),
		ColumnPageBuffers(pool),
	)

	rows := make([]Row, 128)
	for i := range rows {
		rows[i] = Row{ValueOf(fmt.Sprintf("value-%03d", i)).Level(0, 0, 0)}
	}
	if n, err := writer.WriteRows(rows); err != nil {
		t.Fatal(err)
	} else if n != len(rows) {
		t.Fatalf("wrote %d rows, want %d", n, len(rows))
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	file, err := OpenFile(bytes.NewReader(output.Bytes()), int64(output.Len()))
	if err != nil {
		t.Fatal(err)
	}
	filter := file.RowGroups()[0].ColumnChunks()[0].BloomFilter()
	if filter == nil {
		t.Fatal("missing bloom filter")
	}
	for i, row := range rows {
		ok, err := filter.Check(row[0])
		if err != nil {
			t.Fatalf("checking row %d: %v", i, err)
		}
		if !ok {
			t.Fatalf("bloom filter does not contain row %d", i)
		}
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()
	if pool.puts != pool.gets {
		t.Fatalf("returned %d buffers, acquired %d", pool.puts, pool.gets)
	}
}
