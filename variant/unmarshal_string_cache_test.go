package variant

import (
	"strings"
	"testing"
)

func TestUnmarshalStringDoesNotAliasInput(t *testing.T) {
	for _, test := range []struct {
		name string
		want string
	}{
		{name: "short", want: "variant string"},
		{name: "long", want: strings.Repeat("v", 64)},
	} {
		t.Run(test.name, func(t *testing.T) {
			metadata, value, err := Marshal(test.want)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			got, err := Unmarshal(metadata, value)
			if err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			text, ok := got.(string)
			if !ok {
				t.Fatalf("Unmarshal type = %T, want string", got)
			}
			value[len(value)-1] ^= 0xFF
			if text != test.want {
				t.Errorf("Unmarshal result aliases input: got %q, want %q", text, test.want)
			}
		})
	}
}
