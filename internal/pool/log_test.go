package pool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/proc/proctest"
)

// logSink captures json log records written from any goroutine.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *logSink) Write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(b)
}

func (s *logSink) records(t *testing.T) []map[string]any {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []map[string]any
	for _, line := range strings.Split(strings.TrimSpace(s.buf.String()), "\n") {
		if line == "" {
			continue
		}
		var r map[string]any
		if err := json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatalf("bad log line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// find returns the records with msg, waiting briefly so records written by
// reaper goroutines are not missed.
func (s *logSink) find(t *testing.T, msg string) []map[string]any {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		var got []map[string]any
		for _, r := range s.records(t) {
			if r["msg"] == msg {
				got = append(got, r)
			}
		}
		if len(got) > 0 || time.Now().After(deadline) {
			return got
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// one returns the single record with msg.
func (s *logSink) one(t *testing.T, msg string) map[string]any {
	t.Helper()
	got := s.find(t, msg)
	if len(got) != 1 {
		t.Fatalf("%d records with msg %q, want 1: %v", len(got), msg, got)
	}
	return got[0]
}

func newLoggedSink(t *testing.T, concurrency int, mod func(*config.PoolConfig)) (*Pool, *proctest.FakeSpawner, *clock.FakeClock, *logSink) {
	t.Helper()
	sink := &logSink{}
	log := slog.New(slog.NewJSONHandler(sink, &slog.HandlerOptions{Level: slog.LevelDebug}))
	p, s, clk := newLoggedPool(t, concurrency, mod, log)
	return p, s, clk, sink
}

// assertKeys fails when r lacks a key of want or, for a non-nil want value,
// carries a different one. Numbers are compared as json floats.
func assertKeys(t *testing.T, r map[string]any, want map[string]any) {
	t.Helper()
	for k, v := range want {
		got, ok := r[k]
		if !ok {
			t.Errorf("record %v lacks key %q", r, k)
			continue
		}
		if v != nil && got != v {
			t.Errorf("record %v: %s = %v (%T), want %v (%T)", r, k, got, got, v, v)
		}
	}
}

func TestSpawnAndExitLogFields(t *testing.T) {
	p, s, _, sink := newLoggedSink(t, 2, nil)
	p.Hint(context.Background(), hint.SourceHTTP, 1)
	pr := s.Procs()[0]

	assertKeys(t, sink.one(t, "spawn"), map[string]any{
		"level": "INFO", "pool": "enc", "pid": float64(pr.PID()), "source": "http", "running": float64(1),
	})

	pr.Finish(3)
	waitRunning(t, p, 0)
	exit := sink.one(t, "exit")
	assertKeys(t, exit, map[string]any{
		"level": "INFO", "pool": "enc", "pid": float64(pr.PID()), "code": float64(3),
		"result": "failure", "running": float64(0), "duration": nil,
	})
	if _, ok := exit["reason"]; ok {
		t.Errorf("self exit carries a kill reason: %v", exit)
	}
}

func TestBreakerAndDropLogFields(t *testing.T) {
	p, s, _, sink := newLoggedSink(t, 4, func(c *config.PoolConfig) { c.ExitFailureThreshold = 1 })
	ctx := context.Background()
	p.Hint(ctx, hint.SourceRedis, 1)
	s.Procs()[0].Finish(1)
	waitRunning(t, p, 0)

	// The breaker's escalation level must not shadow slog's own level key.
	assertKeys(t, sink.one(t, "breaker_trip"), map[string]any{
		"level": "ERROR", "pool": "enc", "code": float64(1), "breaker_level": float64(1),
		"failures": nil, "window": nil, "cooldown": nil, "open_until": nil,
	})

	p.Hint(ctx, hint.SourceHTTP, 7)
	assertKeys(t, sink.one(t, "drop"), map[string]any{
		"level": "DEBUG", "pool": "enc", "reason": "exit_cooldown", "source": "http",
		"channel": "enc", "count": float64(7), "open_until": nil,
	})
}

func TestSpawnErrorLogFields(t *testing.T) {
	p, s, _, sink := newLoggedSink(t, 1, nil)
	s.Err = errors.New("exec: not found")
	p.Hint(context.Background(), hint.SourceRedis, 1)
	assertKeys(t, sink.one(t, "spawn_error"), map[string]any{
		"level": "ERROR", "pool": "enc", "source": "redis", "err": "exec: not found",
	})
}

func TestTTLKillLogFields(t *testing.T) {
	p, s, clk, sink := newLoggedSink(t, 1, func(c *config.PoolConfig) { c.TTL = time.Minute })
	p.Hint(context.Background(), hint.SourceHTTP, 1)
	pr := s.Procs()[0]
	clk.Advance(time.Minute)
	waitSignal(t, pr, os.Signal(syscall.SIGTERM))
	pr.Finish(0)
	waitRunning(t, p, 0)

	assertKeys(t, sink.one(t, "ttl_kill"), map[string]any{
		"level": "INFO", "pool": "enc", "pid": float64(pr.PID()), "ttl": nil, "signal": "SIGTERM", "grace": nil,
	})
	assertKeys(t, sink.one(t, "exit"), map[string]any{
		"pid": float64(pr.PID()), "result": "killed", "reason": "ttl", "code": float64(0),
	})
}

func TestShutdownLogFields(t *testing.T) {
	p, s, _, sink := newLoggedSink(t, 2, nil)
	ctx := context.Background()
	p.Hint(ctx, hint.SourceHTTP, 2)
	go func() {
		for _, pr := range s.Procs() {
			waitSignal(t, pr, os.Signal(syscall.SIGTERM))
			pr.Kill(syscall.SIGTERM)
		}
	}()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}

	assertKeys(t, sink.one(t, "shutdown"), map[string]any{"level": "INFO", "pool": "enc", "running": float64(2)})
	kills := sink.find(t, "shutdown_kill")
	if len(kills) != 2 {
		t.Fatalf("%d shutdown_kill records, want 2", len(kills))
	}
	for _, r := range kills {
		assertKeys(t, r, map[string]any{"pool": "enc", "pid": nil, "signal": "SIGTERM", "grace": nil})
	}
	for _, r := range sink.find(t, "exit") {
		assertKeys(t, r, map[string]any{"result": "killed", "reason": "shutdown", "signal": "SIGTERM"})
	}
	// Hints after Shutdown are dropped, not spawned.
	p.Hint(ctx, hint.SourceHTTP, 1)
	assertKeys(t, sink.one(t, "drop"), map[string]any{"reason": "shutdown", "source": "http", "count": float64(1)})
}

func TestPokeLogsHint(t *testing.T) {
	p, _, clk, sink := newLoggedSink(t, 1, func(c *config.PoolConfig) { c.Poke = time.Minute; c.PokeCount = 1 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	p.Start(ctx)
	clk.Advance(time.Minute)
	assertKeys(t, sink.one(t, "hint"), map[string]any{
		"pool": "enc", "source": "poke", "channel": "enc", "count": float64(1), "spawned": float64(1),
	})
}
