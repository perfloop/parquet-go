package variant

import (
	"fmt"
	"testing"
)

func (e *encoder) assertSliceContiguous(positions []elementPos) {
	if len(positions) == 0 {
		return
	}
	firstStart := positions[0].start
	if firstStart > len(e.scratch) {
		panic(fmt.Sprintf("invariant violation: firstStart %d > len(scratch) %d", firstStart, len(e.scratch)))
	}
	for i := 1; i < len(positions); i++ {
		if positions[i].start != positions[i-1].end {
			panic(fmt.Sprintf("invariant violation: non-contiguous siblings at index %d: start %d != prior end %d", i, positions[i].start, positions[i-1].end))
		}
	}
}

func (e *encoder) assertObjectContiguous(entries []encodedField) {
	if len(entries) == 0 {
		return
	}
	firstStart := entries[0].start
	if firstStart > len(e.scratch) {
		panic(fmt.Sprintf("invariant violation: firstStart %d > len(scratch) %d", firstStart, len(e.scratch)))
	}
	for i := 1; i < len(entries); i++ {
		if entries[i].start != entries[i-1].end {
			panic(fmt.Sprintf("invariant violation: non-contiguous siblings at index %d: start %d != prior end %d", i, entries[i].start, entries[i-1].end))
		}
	}
}

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
