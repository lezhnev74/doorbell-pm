package httpapi

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/pool"
)

func getJSON(t *testing.T, url string, v any) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	b, _ := io.ReadAll(resp.Body)
	if err := json.Unmarshal(b, v); err != nil {
		t.Fatalf("decode %q: %v", b, err)
	}
	return resp.StatusCode
}

func newHealthServer(t *testing.T, cfg config.HTTP, stats StatsFunc) string {
	t.Helper()
	ts := httptest.NewServer(New(cfg, "jobs:", nil, stats, nil, nil).Handler(nil))
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestHealthReflectsStats(t *testing.T) {
	t0 := time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
	url := newHealthServer(t, testConfig(t), func() map[string]pool.Stats {
		return map[string]pool.Stats{
			"encoding": {Running: 1, Concurrency: 4, LastHint: t0, LastExit: t0.Add(time.Second),
				LastExitCode: 3, LastTasks: 7, HasLastTasks: true, BreakerOpenUntil: t0.Add(5 * time.Second), BreakerLevel: 2},
			"reports": {Concurrency: 1},
		}
	})

	var got healthResponse
	if code := getJSON(t, url+"/healthz", &got); code != http.StatusOK {
		t.Fatalf("status %d, want 200", code)
	}
	if got.Status != "ok" || len(got.Pools) != 2 {
		t.Fatalf("got %+v", got)
	}
	enc := got.Pools["encoding"]
	if enc.Running != 1 || enc.Concurrency != 4 ||
		enc.LastHint == nil || !enc.LastHint.Equal(t0) ||
		enc.LastExit == nil || !enc.LastExit.Equal(t0.Add(time.Second)) ||
		enc.LastExitCode == nil || *enc.LastExitCode != 3 ||
		enc.LastTasks == nil || *enc.LastTasks != 7 ||
		enc.Breaker.OpenUntil == nil || !enc.Breaker.OpenUntil.Equal(t0.Add(5*time.Second)) || enc.Breaker.Level != 2 {
		t.Fatalf("encoding = %+v", enc)
	}
	rep := got.Pools["reports"]
	if rep.Running != 0 || rep.Concurrency != 1 || rep.LastHint != nil || rep.LastExit != nil ||
		rep.LastExitCode != nil || rep.LastTasks != nil || rep.Breaker.OpenUntil != nil || rep.Breaker.Level != 0 {
		t.Fatalf("reports = %+v", rep)
	}
}

func TestHealthNullsForNever(t *testing.T) {
	url := newHealthServer(t, testConfig(t), func() map[string]pool.Stats {
		return map[string]pool.Stats{"encoding": {Concurrency: 2}}
	})
	var raw map[string]any
	getJSON(t, url+"/healthz", &raw)
	enc := raw["pools"].(map[string]any)["encoding"].(map[string]any)
	for _, k := range []string{"last_hint", "last_exit", "last_exit_code", "last_tasks"} {
		if v, ok := enc[k]; !ok || v != nil {
			t.Fatalf("%s = %v, want null", k, v)
		}
	}
	if v := enc["breaker"].(map[string]any)["open_until"]; v != nil {
		t.Fatalf("open_until = %v, want null", v)
	}
}

func TestHealthNoStats(t *testing.T) {
	url := newHealthServer(t, testConfig(t), nil)
	var got healthResponse
	if code := getJSON(t, url+"/healthz", &got); code != http.StatusOK || got.Status != "ok" || len(got.Pools) != 0 {
		t.Fatalf("status %d body %+v", code, got)
	}
}

func TestHealthCustomPath(t *testing.T) {
	cfg := testConfig(t)
	cfg.HealthPath = "/alive"
	url := newHealthServer(t, cfg, nil)
	var got healthResponse
	if code := getJSON(t, url+"/alive", &got); code != http.StatusOK {
		t.Fatalf("custom path status %d, want 200", code)
	}
	resp, err := http.Get(url + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("default path status %d, want 404", resp.StatusCode)
	}
}

func TestHealthWrongMethod(t *testing.T) {
	url := newHealthServer(t, testConfig(t), nil)
	resp, err := http.Post(url+"/healthz", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status %d, want 405", resp.StatusCode)
	}
}
