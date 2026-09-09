package proc

import (
	"strings"
	"testing"
)

func TestLastLine(t *testing.T) {
	long := strings.Repeat("x", MaxResult+1)
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"empty", "", ""},
		{"single", "12\n", "12"},
		{"last wins", "1\n2\n3\n", "3"},
		{"trailing blanks ignored", "5\n\n  \n\r\n", "5"},
		{"crlf and spaces trimmed", " 5 \r\n", "5"},
		{"partial last line", "1\n42", "42"},
		{"whitespace-only partial ignored", "9\n   ", "9"},
		{"max length kept", strings.Repeat("9", MaxResult) + "\n", strings.Repeat("9", MaxResult)},
		{"too long invalidates", "5\n" + long + "\n", ""},
		{"too long partial invalidates", "5\n" + long, ""},
		{"valid line after too long", long + "\n7\n", "7"},
		{"blank after too long stays empty", long + "\n\n", ""},
	}
	for _, tc := range tests {
		for _, chunk := range []int{0, 1, 3} {
			t.Run(tc.name+"/chunk="+string(rune('0'+chunk)), func(t *testing.T) {
				var w lastLine
				in := tc.in
				if chunk == 0 {
					if _, err := w.Write([]byte(in)); err != nil {
						t.Fatal(err)
					}
				} else {
					for len(in) > 0 {
						n := min(chunk, len(in))
						if _, err := w.Write([]byte(in[:n])); err != nil {
							t.Fatal(err)
						}
						in = in[n:]
					}
				}
				if got := w.Line(); got != tc.want {
					t.Fatalf("Line() = %q, want %q", got, tc.want)
				}
			})
		}
	}
}

func TestLastLineBoundsBuffer(t *testing.T) {
	var w lastLine
	for range 1 << 20 {
		if _, err := w.Write([]byte("y")); err != nil {
			t.Fatal(err)
		}
	}
	if len(w.buf) > MaxResult || cap(w.buf) > 2*MaxResult {
		t.Fatalf("buffer grew to len %d cap %d", len(w.buf), cap(w.buf))
	}
	if got := w.Line(); got != "" {
		t.Fatalf("Line() = %q, want empty", got)
	}
}
