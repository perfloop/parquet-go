package variant

import (
	"testing"
)

func TestInvariantViolationPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on invariant violation, but did not panic")
		}
	}()

	var b MetadataBuilder
	enc := encoder{
		b:       &b,
		scratch: make([]byte, 10),
	}
	positions := []elementPos{
		{start: 0, end: 4},
		{start: 5, end: 9}, // gap of 1 byte
	}
	enc.assertSliceContiguous(positions)
}

func TestInvariantViolationOutOfBoundsPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on out-of-bounds start, but did not panic")
		}
	}()

	var b MetadataBuilder
	enc := encoder{
		b:       &b,
		scratch: make([]byte, 5),
	}
	positions := []elementPos{
		{start: 10, end: 12},
	}
	enc.assertSliceContiguous(positions)
}

func TestObjectInvariantViolationPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic on object invariant violation, but did not panic")
		}
	}()

	var b MetadataBuilder
	enc := encoder{
		b:       &b,
		scratch: make([]byte, 10),
	}
	entries := []encodedField{
		{start: 0, end: 4},
		{start: 5, end: 9}, // gap of 1 byte
	}
	enc.assertObjectContiguous(entries)
}
