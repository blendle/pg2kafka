package eventqueue

import (
	"bytes"
	"testing"
)

var byteStringMarshalJSONtests = []struct {
	in  ByteString
	out []byte
}{
	{ByteString(nil), []byte(`null`)},
	{ByteString(""), []byte(`""`)},
	{ByteString("hello"), []byte(`"hello"`)},
}

func TestByteString_MarshalJSON(t *testing.T) {
	for _, tt := range byteStringMarshalJSONtests {
		t.Run(string(tt.out), func(t *testing.T) {
			actual, err := tt.in.MarshalJSON()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(actual, tt.out) {
				t.Errorf("%q => MarshalJSON() => %q, want: %q", tt.in, actual, tt.out)
			}
		})
	}
}

var byteStringUnmarshalJSON = []struct {
	in  []byte
	out ByteString
	err bool
}{
	{[]byte(`null`), ByteString(nil), false},
	{[]byte(`""`), ByteString(""), false},
	{[]byte(`"hello"`), ByteString("hello"), false},
	{[]byte(`1`), ByteString(nil), true},
	{[]byte(`[]`), ByteString(nil), true},
	{[]byte(`{}`), ByteString(nil), true},
}

func TestByteString_UnmarshalJSON(t *testing.T) {
	for _, tt := range byteStringUnmarshalJSON {
		t.Run(string(tt.in), func(t *testing.T) {
			var actual ByteString

			err := actual.UnmarshalJSON(tt.in)
			if (err == nil) == tt.err {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(actual, tt.out) {
				t.Errorf("UnmarshalJSON(%q) => %q, want: %q", tt.in, actual, tt.out)
			}
		})
	}
}

var byteStringValue = []struct {
	in  ByteString
	out string
}{
	{ByteString(nil), ""},
	{ByteString(""), ""},
	{ByteString("hello"), "hello"},
	{ByteString("1"), "1"},
}

func TestByteString_Value(t *testing.T) {
	for _, tt := range byteStringValue {
		t.Run(tt.out, func(t *testing.T) {
			actual, err := tt.in.Value()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if actual != tt.out {
				t.Errorf("%q => Value() => %q, want: %q", tt.in, actual, tt.out)
			}
		})
	}
}

var byteStringScan = []struct {
	in  interface{}
	out ByteString
	err bool
}{
	{nil, ByteString(nil), false},
	{"", ByteString(""), false},
	{"hello", ByteString("hello"), false},
	{[]byte(`null`), ByteString(nil), true},
	{1, ByteString(nil), true},
}

func TestByteString_Scan(t *testing.T) {
	for _, tt := range byteStringScan {
		t.Run(string(tt.out), func(t *testing.T) {
			var actual ByteString

			err := actual.Scan(tt.in)
			if (err == nil) == tt.err {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(actual, tt.out) {
				t.Errorf("Scan(%q) => %q, want: %q", tt.in, actual, tt.out)
			}
		})
	}
}
