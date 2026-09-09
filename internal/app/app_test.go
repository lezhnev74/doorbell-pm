package app

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"doorbell-pm/internal/config"
	"doorbell-pm/test/worker/workertest"
)

var workerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "doorbell-app")
	if err != nil {
		panic(err)
	}
	workerBin, err = workertest.Build(dir)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// lockedBuffer is a race-free sink for the capturing logger.
type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// testConfig parses src with the worker binary and a log file wired in and
// returns the finalized config and the worker log path.
func testConfig(t *testing.T, src string) (config.Config, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "worker.log")
	src = strings.NewReplacer("WORKER_BIN", workerBin, "WORKER_LOG_PATH", logPath).Replace(src)
	cfg, err := config.Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	return cfg, logPath
}

// runApp starts the app in the background and returns it, the log sink and a
// stop function that cancels and returns Run's error.
func runApp(t *testing.T, cfg config.Config) (*App, *lockedBuffer, func() error) {
	t.Helper()
	buf := &lockedBuffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	a := New(cfg, "test", log)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	var once sync.Once
	var err error
	stop := func() error {
		once.Do(func() {
			cancel()
			select {
			case err = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run did not return after cancel")
			}
		})
		return err
	}
	t.Cleanup(func() { _ = stop() })
	return a, buf, stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func countLines(path, prefix string) int {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

func TestRunHintsFromRedisAndHTTP(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg, workerLog := testConfig(t, `
http: {addr: "127.0.0.1:0"}
redis: {addr: "`+mr.Addr()+`"}
shutdown_timeout: 5s
pools:
  _defaults:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 30s}
    grace_shutdown: 2s
  encoding:
    concurrency: 3
    command: [WORKER_BIN, encoding]
  reports:
    channel: rep
    command: [WORKER_BIN, reports]
  legacy:
    enabled: false
    command: [WORKER_BIN, legacy]
`)
	a, buf, stop := runApp(t, cfg)
	waitFor(t, "http listener", func() bool { return a.HTTPAddr() != "" })
	base := "http://" + a.HTTPAddr()

	// Redis hint: concurrency caps the spawn.
	waitFor(t, "redis subscription", func() bool { return strings.Contains(buf.String(), "redis subscribed") })
	mr.Publish("jobs:encoding", "10")
	waitFor(t, "3 encoding workers", func() bool { return countLines(workerLog, "started") == 3 })
	time.Sleep(50 * time.Millisecond)
	if n := countLines(workerLog, "started"); n != 3 {
		t.Fatalf("started %d workers, want 3", n)
	}

	// HTTP hint through a custom channel.
	resp, err := http.Post(base+"/hint", "application/json", strings.NewReader(`{"jobs:rep": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("hint status %d, want 202", resp.StatusCode)
	}
	waitFor(t, "reports worker", func() bool { return countLines(workerLog, "started") == 4 })

	// Disabled pool is not routable over http.
	resp, err = http.Post(base+"/hint", "application/json", strings.NewReader(`{"jobs:legacy": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled pool hint status %d, want 404", resp.StatusCode)
	}

	// Health reflects the pools.
	resp, err = http.Get(base + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	var health struct {
		Pools map[string]struct{ Running int }
	}
	if err := json.Unmarshal(body, &health); err != nil {
		t.Fatalf("healthz: %v\n%s", err, body)
	}
	if health.Pools["encoding"].Running != 3 || health.Pools["reports"].Running != 1 {
		t.Fatalf("healthz = %s", body)
	}
	if _, ok := health.Pools["legacy"]; ok {
		t.Fatalf("disabled pool in healthz: %s", body)
	}

	// Shutdown terminates every worker and Run returns clean.
	if err := stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if n := countLines(workerLog, "term"); n != 4 {
		t.Fatalf("%d workers got TERM, want 4\n%s", n, buf.String())
	}
}

func TestRunFailsWhenHTTPListenFails(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	cfg, _ := testConfig(t, `
http: {addr: "`+ln.Addr().String()+`"}
redis: {enabled: false}
pools:
  encoding: {command: [WORKER_BIN]}
`)
	_, _, stop := runApp(t, cfg)
	err = stop()
	if err == nil || !strings.Contains(err.Error(), "http source") {
		t.Fatalf("Run returned %v, want http source error", err)
	}
}

func TestRunShutdownBoundedByTimeout(t *testing.T) {
	cfg, workerLog := testConfig(t, `
http: {addr: "127.0.0.1:0"}
redis: {enabled: false}
shutdown_timeout: 300ms
pools:
  encoding:
    command: [WORKER_BIN]
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 30s, WORKER_IGNORE_TERM: "1"}
    grace_shutdown: 1s
`)
	a, _, stop := runApp(t, cfg)
	waitFor(t, "http listener", func() bool { return a.HTTPAddr() != "" })
	resp, err := http.Post("http://"+a.HTTPAddr()+"/hint", "application/json", strings.NewReader(`{"jobs:encoding": 1}`))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	waitFor(t, "worker", func() bool { return countLines(workerLog, "started") == 1 })

	start := time.Now()
	err = stop()
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("shutdown took %s, want about 300ms", elapsed)
	}
	if err == nil || !strings.Contains(err.Error(), "pool encoding: shutdown") {
		t.Fatalf("Run returned %v, want pool shutdown timeout", err)
	}
	// The kill still completes in the background.
	waitFor(t, "worker killed", func() bool { return a.pools["encoding"].Running() == 0 })
}
