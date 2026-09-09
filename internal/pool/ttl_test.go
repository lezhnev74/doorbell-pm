package pool

import (
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/proc/proctest"
)

func newTTLPool(t *testing.T, ttl time.Duration, mod func(*config.PoolConfig)) (*Pool, *proctest.FakeSpawner, *clock.FakeClock) {
	t.Helper()
	return newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.TTL = ttl
		if mod != nil {
			mod(c)
		}
	})
}

// waitSignal polls until pr has received sig.
func waitSignal(t *testing.T, pr *proctest.FakeProcess, sig os.Signal) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if slices.Contains(pr.Signals(), sig) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("signals = %v, want %v", pr.Signals(), sig)
}

func TestTTLTerminatesWithTermSignal(t *testing.T) {
	p, s, clk := newTTLPool(t, 10*time.Second, func(c *config.PoolConfig) {
		c.ExitFailureThreshold = 1
		c.TermSignal = syscall.SIGINT
	})
	p.Hint(ctx, "test", 1)
	pr := s.Procs()[0]

	clk.Advance(9 * time.Second)
	if len(pr.Signals()) != 0 {
		t.Fatalf("signalled before ttl: %v", pr.Signals())
	}
	clk.Advance(time.Second)
	waitSignal(t, pr, os.Signal(syscall.SIGINT))

	// A ttl kill is neutral for the breaker even if the worker exits non-zero.
	pr.Finish(1)
	waitRunning(t, p, 0)
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint after ttl kill spawned %d, want 1", got)
	}
}

func TestTTLKillsGroupAfterGrace(t *testing.T) {
	p, s, clk := newTTLPool(t, time.Minute, func(c *config.PoolConfig) {
		c.GraceShutdown = 20 * time.Millisecond
	})
	p.Hint(ctx, "test", 1)
	pr := s.Procs()[0]

	clk.Advance(time.Minute)
	waitSignal(t, pr, os.Signal(syscall.SIGTERM))
	waitSignal(t, pr, os.Signal(syscall.SIGKILL))
	waitRunning(t, p, 0)
}

func TestExitBeforeTTLStopsTimer(t *testing.T) {
	p, s, clk := newTTLPool(t, 10*time.Second, nil)
	p.Hint(ctx, "test", 1)
	pr := s.Procs()[0]
	if clk.Pending() != 1 {
		t.Fatalf("pending timers = %d, want 1", clk.Pending())
	}

	pr.Finish(0)
	waitRunning(t, p, 0)
	if clk.Pending() != 0 {
		t.Fatalf("pending timers = %d after exit, want 0", clk.Pending())
	}
	clk.Advance(time.Hour)
	if len(pr.Signals()) != 0 {
		t.Fatalf("exited worker was signalled: %v", pr.Signals())
	}
}

func TestTTLZeroNeverFires(t *testing.T) {
	p, s, clk := newTTLPool(t, 0, nil)
	p.Hint(ctx, "test", 1)
	if clk.Pending() != 0 {
		t.Fatalf("pending timers = %d with ttl 0, want 0", clk.Pending())
	}
	clk.Advance(24 * time.Hour)
	if got := len(s.Procs()[0].Signals()); got != 0 {
		t.Fatalf("worker signalled with ttl 0: %v", s.Procs()[0].Signals())
	}
	if got := p.Running(); got != 1 {
		t.Fatalf("running = %d, want 1", got)
	}
}
