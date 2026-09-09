package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"doorbell-pm/internal/hint"
)

// maxHintBody bounds a hint request; a hint is a handful of keys.
const maxHintBody = 64 << 10

// hintHandler accepts `{"<prefix><channel>": count, ...}` and forwards one
// hint per key. The whole body is validated before anything is forwarded so
// a request is either fully accepted (202) or fully rejected (400/404).
func (s *Server) hintHandler(out chan<- hint.Hint) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hints, err := s.parseHints(r.Body)
		if err != nil {
			s.reject(w, r, err)
			return
		}
		for _, h := range hints {
			select {
			case out <- h:
			case <-r.Context().Done():
				writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "shutting down"})
				return
			}
		}
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "accepted"})
	}
}

// requestError carries the status a bad hint request is answered with.
type requestError struct {
	status int
	msg    string
}

func (e *requestError) Error() string { return e.msg }

func badRequest(format string, a ...any) error {
	return &requestError{status: http.StatusBadRequest, msg: fmt.Sprintf(format, a...)}
}

// parseHints decodes and validates the body without side effects.
func (s *Server) parseHints(body io.Reader) ([]hint.Hint, error) {
	var raw map[string]int
	dec := json.NewDecoder(io.LimitReader(body, maxHintBody))
	if err := dec.Decode(&raw); err != nil {
		return nil, badRequest("bad json: %v", err)
	}
	if len(raw) == 0 {
		return nil, badRequest("no hints in body")
	}
	hints := make([]hint.Hint, 0, len(raw))
	for key, count := range raw {
		if count < 0 {
			return nil, badRequest("%q: count must be >= 0, got %d", key, count)
		}
		channel, ok := strings.CutPrefix(key, s.prefix)
		if _, known := s.channels[channel]; !ok || !known {
			return nil, &requestError{status: http.StatusNotFound, msg: fmt.Sprintf("%q: unknown pool", key)}
		}
		hints = append(hints, hint.Hint{Source: hint.SourceHTTP, Pool: channel, Count: count})
	}
	return hints, nil
}

func (s *Server) reject(w http.ResponseWriter, r *http.Request, err error) {
	var re *requestError
	if !errors.As(err, &re) {
		re = &requestError{status: http.StatusInternalServerError, msg: err.Error()}
	}
	s.log.Debug("hint_rejected", "source", hint.SourceHTTP, "status", re.status, "err", re.msg, "remote", r.RemoteAddr)
	writeJSON(w, re.status, map[string]string{"error": re.msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
