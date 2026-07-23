package variant

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

// TestUnmarshalCompatibility keeps Unmarshal aligned with the public result
// produced by Decode followed by Value.GoValue. It exercises the spec corpus,
// accepted non-canonical layouts, trailing bytes, malformed input, and the
// binary ownership guarantee that a direct decoder must retain.
func TestUnmarshalCompatibility(t *testing.T) {
	for name := range specExpectedValues() {
		t.Run("spec/"+name, func(t *testing.T) {
			metadata, value := mustLoadSpecCase(t, name)

			m, err := DecodeMetadata(metadata)
			if err != nil {
				t.Fatalf("DecodeMetadata: %v", err)
			}
			decoded, err := Decode(m, value)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}

			got, err := Unmarshal(metadata, value)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if want := decoded.GoValue(); !reflect.DeepEqual(got, want) {
				t.Errorf("Unmarshal = %#v, want Decode(...).GoValue() = %#v", got, want)
			}
		})
	}

	t.Run("unsorted object fields", func(t *testing.T) {
		var b MetadataBuilder
		b.Add("a")
		b.Add("b")
		_, metadata := b.Build()

		// Fields are listed as {b, a}; the reader accepts this non-canonical
		// order as long as the field names remain unique.
		value := []byte{0x02, 0x02, 0x01, 0x00, 0x00, 0x01, 0x02, 0x00, 0x00}
		got, err := Unmarshal(metadata, value)
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		want := map[string]any{"a": nil, "b": nil}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Unmarshal = %#v, want %#v", got, want)
		}
	})

	t.Run("trailing bytes", func(t *testing.T) {
		metadata, value := mustLoadSpecCase(t, "object_nested")
		want, err := Unmarshal(metadata, value)
		if err != nil {
			t.Fatalf("Unmarshal without trailing bytes: %v", err)
		}

		padded := append(append([]byte(nil), value...), 0xDE, 0xAD)
		got, err := Unmarshal(metadata, padded)
		if err != nil {
			t.Fatalf("Unmarshal with trailing bytes: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("Unmarshal with trailing bytes = %#v, want %#v", got, want)
		}
	})

	t.Run("binary does not alias input", func(t *testing.T) {
		metadata, value, err := Marshal([]byte{1, 2, 3, 4})
		if err != nil {
			t.Fatalf("Marshal: %v", err)
		}
		got, err := Unmarshal(metadata, value)
		if err != nil {
			t.Fatalf("Unmarshal: %v", err)
		}
		binary, ok := got.([]byte)
		if !ok {
			t.Fatalf("Unmarshal type = %T, want []byte", got)
		}
		want := append([]byte(nil), binary...)
		value[len(value)-1] ^= 0xFF
		if !bytes.Equal(binary, want) {
			t.Errorf("Unmarshal result aliases input: got %v, want %v", binary, want)
		}
	})

	var empty MetadataBuilder
	_, emptyMetadata := empty.Build()
	var names MetadataBuilder
	names.Add("a")
	names.Add("b")
	_, objectMetadata := names.Build()

	for _, test := range []struct {
		name     string
		metadata []byte
		value    []byte
		wantErr  string
	}{
		{
			name:     "empty value",
			metadata: emptyMetadata,
			wantErr:  "empty data",
		},
		{
			name:     "invalid short string UTF-8",
			metadata: emptyMetadata,
			value:    []byte{0x05, 0xFF},
			wantErr:  "valid UTF-8",
		},
		{
			// Fields {b, a, b}; the duplicate is non-adjacent and must still
			// be rejected after the decoder detects the unsorted layout.
			name:     "non-adjacent duplicate field",
			metadata: objectMetadata,
			value:    []byte{0x02, 0x03, 0x01, 0x00, 0x01, 0x00, 0x01, 0x02, 0x03, 0x00, 0x00, 0x00},
			wantErr:  "duplicate object field",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := Unmarshal(test.metadata, test.value)
			if err == nil {
				t.Fatalf("Unmarshal succeeded, want error containing %q", test.wantErr)
			}
			if !strings.Contains(err.Error(), test.wantErr) {
				t.Errorf("Unmarshal error = %q, want substring %q", err, test.wantErr)
			}
		})
	}
}
