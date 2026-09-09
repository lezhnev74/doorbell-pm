package redisin

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
)

func testConfig(t *testing.T, addr, extra string) config.Redis {
	t.Helper()
	cfg, err := config.Parse(`
redis: {addr: "` + addr + `", reconnect_min: 10ms, reconnect_max: 40ms` + extra + `}
pools:
  encoding: {command: [w]}
`)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	return cfg.Redis
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

func (l *lockedBuffer) count(s string) int { return strings.Count(l.String(), s) }

// start runs the source in the background and returns its hint channel and
// the log sink. The source is stopped at test cleanup.
func start(t *testing.T, cfg config.Redis, clk clock.Clock) (chan hint.Hint, *lockedBuffer) {
	t.Helper()
	buf := &lockedBuffer{}
	log := slog.New(slog.NewTextHandler(buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	out := make(chan hint.Hint, 8)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- New(cfg, clk, log, nil).Run(ctx, out) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v, want nil", err)
			}
		case <-time.After(2 * time.Second):
			t.Error("Run did not return after cancel")
		}
	})
	return out, buf
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitSubscribed(t *testing.T, buf *lockedBuffer, n int) {
	t.Helper()
	waitFor(t, "subscription", func() bool { return buf.count("redis subscribed") >= n })
}

func recv(t *testing.T, out chan hint.Hint) hint.Hint {
	t.Helper()
	select {
	case h := <-out:
		return h
	case <-time.After(2 * time.Second):
		t.Fatal("no hint received")
		return hint.Hint{}
	}
}

func noHint(t *testing.T, out chan hint.Hint) {
	t.Helper()
	select {
	case h := <-out:
		t.Fatalf("unexpected hint %+v", h)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestPublishBecomesHint(t *testing.T) {
	srv := miniredis.RunT(t)
	out, buf := start(t, testConfig(t, srv.Addr(), ""), nil)
	waitSubscribed(t, buf, 1)

	srv.Publish("jobs:encoding", "100")
	if h := recv(t, out); h.Pool != "encoding" || h.Count != 100 {
		t.Fatalf("got %+v, want encoding/100", h)
	}
}

func TestCustomPrefixStripped(t *testing.T) {
	srv := miniredis.RunT(t)
	out, buf := start(t, testConfig(t, srv.Addr(), `, channel_prefix: "q."`), nil)
	waitSubscribed(t, buf, 1)

	srv.Publish("jobs:encoding", "1")
	srv.Publish("q.encoding", "2")
	if h := recv(t, out); h.Pool != "encoding" || h.Count != 2 {
		t.Fatalf("got %+v, want encoding/2", h)
	}
	noHint(t, out)
}

func TestBadPayloadDropped(t *testing.T) {
	srv := miniredis.RunT(t)
	out, buf := start(t, testConfig(t, srv.Addr(), ""), nil)
	waitSubscribed(t, buf, 1)

	for _, p := range []string{"abc", "", "-1", "1.5"} {
		srv.Publish("jobs:encoding", p)
	}
	waitFor(t, "drops", func() bool { return buf.count("reason=bad_payload") == 4 })
	noHint(t, out)

	srv.Publish("jobs:encoding", " 3 ")
	if h := recv(t, out); h.Count != 3 {
		t.Fatalf("got %+v, want count 3 after bad payloads", h)
	}
}

func TestResubscribesAfterRestart(t *testing.T) {
	srv := miniredis.RunT(t)
	out, buf := start(t, testConfig(t, srv.Addr(), ""), nil)
	waitSubscribed(t, buf, 1)

	srv.Close()
	waitFor(t, "reconnect log", func() bool { return buf.count("reconnecting") >= 1 })
	if err := srv.Restart(); err != nil {
		t.Fatal(err)
	}
	waitSubscribed(t, buf, 2)

	srv.Publish("jobs:encoding", "7")
	if h := recv(t, out); h.Count != 7 {
		t.Fatalf("got %+v, want count 7 after restart", h)
	}
}

func TestAuthHonoured(t *testing.T) {
	srv := miniredis.RunT(t)
	srv.RequireAuth("s3cret")

	out, buf := start(t, testConfig(t, srv.Addr(), `, password: s3cret`), nil)
	waitSubscribed(t, buf, 1)
	srv.Publish("jobs:encoding", "1")
	if h := recv(t, out); h.Count != 1 {
		t.Fatalf("got %+v, want count 1", h)
	}

	_, bad := start(t, testConfig(t, srv.Addr(), `, password: wrong`), nil)
	waitFor(t, "auth failure", func() bool { return bad.count("reconnecting") >= 1 })
	if bad.count("redis subscribed") != 0 {
		t.Fatal("subscribed with a wrong password")
	}
}

func TestBackoffGrows(t *testing.T) {
	srv := miniredis.RunT(t)
	addr := srv.Addr()
	srv.Close() // nothing listens there now

	clk := clock.NewFake(time.Unix(0, 0))
	_, buf := start(t, testConfig(t, addr, ""), clk)

	// Each failed attempt arms exactly one backoff timer; the wait between
	// attempts is 10ms, 20ms, 40ms, 40ms (capped).
	attempts := func(n int) func() bool { return func() bool { return buf.count("reconnecting") == n } }
	waitFor(t, "attempt 1", attempts(1))
	waitFor(t, "timer", func() bool { return clk.Pending() == 1 })

	for i, step := range []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond, 40 * time.Millisecond} {
		clk.Advance(step - time.Millisecond)
		time.Sleep(20 * time.Millisecond)
		if got := buf.count("reconnecting"); got != i+1 {
			t.Fatalf("attempt %d fired before %s elapsed (attempts=%d)", i+2, step, got)
		}
		clk.Advance(time.Millisecond)
		waitFor(t, "next attempt", attempts(i+2))
		waitFor(t, "timer", func() bool { return clk.Pending() == 1 })
	}
}

func TestBackoffResetsAfterSubscribe(t *testing.T) {
	srv := miniredis.RunT(t)
	addr := srv.Addr()
	srv.Close()

	clk := clock.NewFake(time.Unix(0, 0))
	_, buf := start(t, testConfig(t, addr, ""), clk)
	waitFor(t, "attempt 1", func() bool { return buf.count("reconnecting") == 1 })
	waitFor(t, "timer", func() bool { return clk.Pending() == 1 })
	clk.Advance(10 * time.Millisecond)
	waitFor(t, "attempt 2", func() bool { return buf.count("retry_in=20ms") == 1 })
	waitFor(t, "timer", func() bool { return clk.Pending() == 1 })

	if err := srv.Restart(); err != nil {
		t.Fatal(err)
	}
	clk.Advance(20 * time.Millisecond)
	waitSubscribed(t, buf, 1)

	srv.Close()
	waitFor(t, "attempt after loss", func() bool { return buf.count("reconnecting") == 3 })
	if buf.count("retry_in=10ms") != 2 {
		t.Fatalf("backoff did not reset to reconnect_min after a subscription:\n%s", buf.String())
	}
}

func TestPattern(t *testing.T) {
	for in, want := range map[string]string{
		"jobs:": "jobs:*",
		"":      "*",
		"a*b?":  `a\*b\?*`,
	} {
		if got := pattern(in); got != want {
			t.Errorf("pattern(%q) = %q, want %q", in, got, want)
		}
	}
}
