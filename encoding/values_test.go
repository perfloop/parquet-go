package encoding_test

import (
	"testing"
	"unsafe"

	"github.com/parquet-go/parquet-go/encoding"
)

func TestValuesSize(t *testing.T) {
	t.Log(unsafe.Sizeof(encoding.Values{}))
}

func TestValuesDataOnValue(t *testing.T) {
	data, offsets := func() encoding.Values {
		return encoding.Values{}
	}().Data()

	if data != nil || offsets != nil {
		t.Fatalf("Values.Data() = (%v, %v), want (nil, nil)", data, offsets)
	}
}
