package parquet_test

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
)

const reconstructMapWideEntries = 32

type reconstructMapWideRecord struct {
	Entries map[string]reconstructMapWideValue
}

type reconstructMapWideValue struct {
	Name     string
	Metrics  []int64 `parquet:",list"`
	Samples  []int64 `parquet:",list"`
	Optional *int64
	Nested   reconstructMapWideNested
}

type reconstructMapWideNested struct {
	A int64
	B int64
	C int64
	D int64
}

func makeReconstructMapWideRecord(variant int) reconstructMapWideRecord {
	entries := make(map[string]reconstructMapWideValue, reconstructMapWideEntries)

	for i := range reconstructMapWideEntries {
		base := int64(variant*1000 + i)
		metrics := make([]int64, 6+(i+variant)%3)
		samples := make([]int64, 4+(i+2*variant)%4)

		for j := range metrics {
			metrics[j] = base + int64(j)
		}
		for j := range samples {
			samples[j] = base*10 + int64(j)
		}

		var optional *int64
		if (i+variant)%3 != 0 {
			optional = new(int64)
			*optional = base * 100
		}

		entries[fmt.Sprintf("entry-%02d-%02d", variant, i)] = reconstructMapWideValue{
			Name:     fmt.Sprintf("value-%02d-%02d", variant, i),
			Metrics:  metrics,
			Samples:  samples,
			Optional: optional,
			Nested: reconstructMapWideNested{
				A: base,
				B: base + 1,
				C: base + 2,
				D: base + 3,
			},
		}
	}

	return reconstructMapWideRecord{Entries: entries}
}

func TestReconstructMapWide(t *testing.T) {
	schema := parquet.SchemaOf(reconstructMapWideRecord{})

	for _, variant := range []int{0, 1, 2, 3} {
		t.Run(fmt.Sprintf("variant-%d", variant), func(t *testing.T) {
			want := makeReconstructMapWideRecord(variant)
			row := schema.Deconstruct(nil, want)
			var got reconstructMapWideRecord

			if err := schema.Reconstruct(&got, row); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("reconstructed record mismatch:\nwant: %#v\ngot:  %#v", want, got)
			}
		})
	}
}

func TestReconstructMapWideConcurrent(t *testing.T) {
	schema := parquet.SchemaOf(reconstructMapWideRecord{})
	inputs := []reconstructMapWideRecord{
		makeReconstructMapWideRecord(0),
		makeReconstructMapWideRecord(1),
		makeReconstructMapWideRecord(2),
		makeReconstructMapWideRecord(3),
	}
	rows := make([]parquet.Row, len(inputs))

	for i := range inputs {
		rows[i] = schema.Deconstruct(nil, inputs[i])
	}

	const workers = 8
	const iterationsPerWorker = 16
	errs := make(chan error, workers)
	var wg sync.WaitGroup

	for worker := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := range iterationsPerWorker {
				index := (worker + i) % len(inputs)
				var got reconstructMapWideRecord

				if err := schema.Reconstruct(&got, rows[index]); err != nil {
					errs <- err
					return
				}
				if !reflect.DeepEqual(got, inputs[index]) {
					errs <- fmt.Errorf("worker %d iteration %d reconstructed a different record", worker, i)
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}
}

func BenchmarkReconstructMapWide(b *testing.B) {
	schema := parquet.SchemaOf(reconstructMapWideRecord{})
	inputs := []reconstructMapWideRecord{
		makeReconstructMapWideRecord(0),
		makeReconstructMapWideRecord(1),
		makeReconstructMapWideRecord(2),
		makeReconstructMapWideRecord(3),
	}
	rows := make([]parquet.Row, len(inputs))

	for i := range inputs {
		rows[i] = schema.Deconstruct(nil, inputs[i])
	}

	var warm reconstructMapWideRecord
	if err := schema.Reconstruct(&warm, rows[0]); err != nil {
		b.Fatal(err)
	}
	if !reflect.DeepEqual(warm, inputs[0]) {
		b.Fatal("warm reconstruction did not preserve the input")
	}

	var got reconstructMapWideRecord
	for i := 0; b.Loop(); i++ {
		got = reconstructMapWideRecord{}
		if err := schema.Reconstruct(&got, rows[i%len(rows)]); err != nil {
			b.Fatal(err)
		}
	}

	if want := inputs[(b.N-1)%len(inputs)]; !reflect.DeepEqual(got, want) {
		b.Fatalf("reconstructed record mismatch:\nwant: %#v\ngot:  %#v", want, got)
	}
}
