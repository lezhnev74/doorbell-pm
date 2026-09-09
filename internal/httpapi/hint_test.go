package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
)

func testConfig(t *testing.T) config.HTTP {
	t.Helper()
	cfg, err := config.Parse(`
http: {addr: "127.0.0.1:0"}
pools:
  encoding: {command: [w]}
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	return cfg.HTTP
}

// newTestServer serves the handler through httptest and returns the base URL
// and the buffered hint channel it feeds.
func newTestServer(t *testing.T, cfg config.HTTP) (string, chan hint.Hint) {
	t.Helper()
	out := make(chan hint.Hint, 8)
	s := New(cfg, "jobs:", []string{"encoding", "rep-queue"}, nil, nil, nil)
	ts := httptest.NewServer(s.Handler(out))
	t.Cleanup(ts.Close)
	return ts.URL, out
}

func post(t *testing.T, url, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func recv(t *testing.T, out chan hint.Hint) hint.Hint {
	t.Helper()
	select {
	case h := <-out:
		return h
	case <-time.After(time.Second):
		t.Fatal("no hint received")
		return hint.Hint{}
	}
}

func TestHintAccepted(t *testing.T) {
	url, out := newTestServer(t, testConfig(t))
	code, body := post(t, url+"/hint", `{"jobs:encoding": 100}`)
	if code != http.StatusAccepted {
		t.Fatalf("status %d %s, want 202", code, body)
	}
	if h := recv(t, out); h.Pool != "encoding" || h.Count != 100 {
		t.Fatalf("got %+v, want encoding/100", h)
	}
}

func TestHintMultipleKeys(t *testing.T) {
	url, out := newTestServer(t, testConfig(t))
	if code, body := post(t, url+"/hint", `{"jobs:encoding": 1, "jobs:rep-queue": 2}`); code != http.StatusAccepted {
		t.Fatalf("status %d %s, want 202", code, body)
	}
	got := map[string]int{}
	for range 2 {
		h := recv(t, out)
		got[h.Pool] = h.Count
	}
	if got["encoding"] != 1 || got["rep-queue"] != 2 {
		t.Fatalf("got %v", got)
	}
}

func TestHintRejected(t *testing.T) {
	tests := []struct {
		name, body string
		status     int
	}{
		{"bad json", `{"jobs:encoding": `, http.StatusBadRequest},
		{"not an object", `[1]`, http.StatusBadRequest},
		{"empty object", `{}`, http.StatusBadRequest},
		{"string count", `{"jobs:encoding": "10"}`, http.StatusBadRequest},
		{"float count", `{"jobs:encoding": 1.5}`, http.StatusBadRequest},
		{"negative count", `{"jobs:encoding": -1}`, http.StatusBadRequest},
		{"unknown pool", `{"jobs:nope": 1}`, http.StatusNotFound},
		{"missing prefix", `{"encoding": 1}`, http.StatusNotFound},
		{"one bad key rejects all", `{"jobs:encoding": 1, "jobs:nope": 1}`, http.StatusNotFound},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			url, out := newTestServer(t, testConfig(t))
			code, body := post(t, url+"/hint", tc.body)
			if code != tc.status {
				t.Fatalf("status %d %s, want %d", code, body, tc.status)
			}
			if !strings.Contains(body, `"error"`) {
				t.Fatalf("body %q has no error", body)
			}
			select {
			case h := <-out:
				t.Fatalf("hint forwarded: %+v", h)
			default:
			}
		})
	}
}

func TestHintWrongMethod(t *testing.T) {
	url, _ := newTestServer(t, testConfig(t))
	resp, err := http.Get(url + "/hint")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", resp.StatusCode)
	}
}

func TestHintCustomPath(t *testing.T) {
	cfg := testConfig(t)
	cfg.HintPath = "/ring"
	url, out := newTestServer(t, cfg)
	if code, _ := post(t, url+"/ring", `{"jobs:encoding": 1}`); code != http.StatusAccepted {
		t.Fatalf("custom path status %d, want 202", code)
	}
	recv(t, out)
	if code, _ := post(t, url+"/hint", `{"jobs:encoding": 1}`); code != http.StatusNotFound {
		t.Fatalf("default path status %d, want 404", code)
	}
}

func TestRunServesAndStops(t *testing.T) {
	s := New(testConfig(t), "jobs:", []string{"encoding"}, nil, nil, nil)
	out := make(chan hint.Hint, 1)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.Run(ctx, out) }()

	deadline := time.Now().Add(2 * time.Second)
	for s.Addr() == "" {
		if time.Now().After(deadline) {
			t.Fatal("server did not listen")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if code, body := post(t, "http://"+s.Addr()+"/hint", `{"jobs:encoding": 3}`); code != http.StatusAccepted {
		t.Fatalf("status %d %s, want 202", code, body)
	}
	if h := recv(t, out); h.Count != 3 {
		t.Fatalf("got %+v", h)
	}

	cancel()
	select {
	case err := <-errc:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}
}

func TestRunListenError(t *testing.T) {
	cfg := testConfig(t)
	cfg.Addr = "256.0.0.1:1"
	if err := New(cfg, "jobs:", nil, nil, nil, nil).Run(context.Background(), nil); err == nil {
		t.Fatal("Run succeeded on a bad addr")
	}
}
