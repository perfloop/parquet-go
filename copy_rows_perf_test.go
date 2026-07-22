package parquet

import (
	"fmt"
	"io"
	"testing"
)

type copyRowsSchemaReader struct {
	schema *Schema
	rows   []Row
	direct bool
}

func (r *copyRowsSchemaReader) Schema() *Schema { return r.schema }

func (r *copyRowsSchemaReader) ReadRows(rows []Row) (int, error) {
	if len(r.rows) == 0 {
		return 0, io.EOF
	}

	n := copy(rows, r.rows)
	r.rows = r.rows[n:]
	if len(r.rows) == 0 {
		return n, io.EOF
	}
	return n, nil
}

func (r *copyRowsSchemaReader) WriteRowsTo(dst RowWriter) (int64, error) {
	r.direct = true
	n, err := dst.WriteRows(r.rows)
	r.rows = r.rows[n:]
	return int64(n), err
}

type copyRowsSchemaWriter struct {
	schema *Schema
	rows   []Row
}

func (w *copyRowsSchemaWriter) Schema() *Schema { return w.schema }

func (w *copyRowsSchemaWriter) WriteRows(rows []Row) (int, error) {
	for _, row := range rows {
		w.rows = append(w.rows, row.Clone())
	}
	return len(rows), nil
}

func TestCopyRowsSchemaComparison(t *testing.T) {
	rows := []Row{{Int32Value(42).Level(0, 0, 0)}}

	t.Run("same schema pointer preserves direct copy", func(t *testing.T) {
		schema := copyRowsWideSchema(1, Int32Type)
		src := &copyRowsSchemaReader{schema: schema, rows: slicesCloneRows(rows)}
		dst := &copyRowsSchemaWriter{schema: schema}

		n, err := CopyRows(dst, src)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(rows)) {
			t.Fatalf("copied %d rows, want %d", n, len(rows))
		}
		if !src.direct {
			t.Fatal("CopyRows did not retain the direct reader-to-writer path")
		}
		assertCopyRowsEqual(t, dst.rows, rows)
	})

	t.Run("distinct equal schemas preserve direct copy", func(t *testing.T) {
		src := &copyRowsSchemaReader{
			schema: copyRowsWideSchema(1, Int32Type),
			rows:   slicesCloneRows(rows),
		}
		dst := &copyRowsSchemaWriter{schema: copyRowsWideSchema(1, Int32Type)}

		n, err := CopyRows(dst, src)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(rows)) {
			t.Fatalf("copied %d rows, want %d", n, len(rows))
		}
		if !src.direct {
			t.Fatal("CopyRows converted structurally equal schemas")
		}
		assertCopyRowsEqual(t, dst.rows, rows)
	})

	t.Run("incompatible schemas convert rows", func(t *testing.T) {
		src := &copyRowsSchemaReader{
			schema: copyRowsWideSchema(1, Int32Type),
			rows:   slicesCloneRows(rows),
		}
		dst := &copyRowsSchemaWriter{schema: copyRowsWideSchema(1, Int64Type)}

		n, err := CopyRows(dst, src)
		if err != nil {
			t.Fatal(err)
		}
		if n != int64(len(rows)) {
			t.Fatalf("copied %d rows, want %d", n, len(rows))
		}
		if src.direct {
			t.Fatal("CopyRows bypassed conversion for incompatible schemas")
		}
		want := []Row{{Int64Value(42).Level(0, 0, 0)}}
		assertCopyRowsEqual(t, dst.rows, want)
	})
}

func BenchmarkCopyRowsWideSameSchema(b *testing.B) {
	schema := copyRowsWideSchema(128, Int64Type)
	src := &copyRowsSchemaReader{schema: schema}
	dst := &copyRowsSchemaWriter{schema: schema}

	for b.Loop() {
		n, err := CopyRows(dst, src)
		if err != nil {
			b.Fatal(err)
		}
		if n != 0 {
			b.Fatalf("copied %d rows, want 0", n)
		}
	}
}

func copyRowsWideSchema(width int, typ Type) *Schema {
	fields := make(Group, width)
	for i := 0; i < width; i++ {
		fields[fmt.Sprintf("field_%03d", i)] = Leaf(typ)
	}
	return NewSchema("copy_rows", fields)
}

func slicesCloneRows(rows []Row) []Row {
	clone := make([]Row, len(rows))
	for i, row := range rows {
		clone[i] = row.Clone()
	}
	return clone
}

func assertCopyRowsEqual(t *testing.T, got, want []Row) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("copied %d rows, want %d", len(got), len(want))
	}
	for i := range want {
		if !got[i].Equal(want[i]) {
			t.Fatalf("row %d = %v, want %v", i, got[i], want[i])
		}
	}
}
