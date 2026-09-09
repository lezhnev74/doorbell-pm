package proc

import (
	"bytes"
	"strings"
	"sync"
)

// MaxResult is the longest result line kept, in bytes before trimming. A
// longer line is not truncated but invalidates the result.
const MaxResult = 64

// lastLine is an io.Writer that remembers the most recent non-empty line of
// what was written, trimmed of surrounding whitespace and \r. Memory is
// bounded: at most MaxResult bytes of a partial line are held and the rest of
// an oversized line is dropped until its newline.
type lastLine struct {
	mu   sync.Mutex
	buf  []byte // partial line so far
	long bool   // the partial line already exceeded MaxResult
	last string
}

func (w *lastLine) Write(b []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	rest := b
	for len(rest) > 0 {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			w.take(rest)
			break
		}
		w.take(rest[:i])
		w.endLine()
		rest = rest[i+1:]
	}
	return len(b), nil
}

func (w *lastLine) take(b []byte) {
	if w.long {
		return
	}
	if len(w.buf)+len(b) > MaxResult {
		w.long = true
		w.buf = w.buf[:0]
		return
	}
	w.buf = append(w.buf, b...)
}

func (w *lastLine) endLine() {
	if w.long {
		w.last = ""
	} else if s := strings.TrimSpace(string(w.buf)); s != "" {
		w.last = s
	}
	w.long = false
	w.buf = w.buf[:0]
}

// Line returns the kept line. A trailing partial line counts as a line, so
// output without a final newline is not lost.
func (w *lastLine) Line() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.long || len(w.buf) > 0 {
		w.endLine()
	}
	return w.last
}
