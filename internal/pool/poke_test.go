package pool

import (
	"context"
	"testing"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/proc/proctest"
)

func newPokePool(t *testing.T, poke time.Duration, count int) (*Pool, *proctest.FakeSpawner, *clock.FakeClock) {
	t.Helper()
	return newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.Poke = poke
		c.PokeCount = count
	})
}

func TestPokeHintsEveryInterval(t *testing.T) {
	p, s, clk := newPokePool(t, 5*time.Minute, 2)
	pctx, cancel := context.WithCancel(ctx)
	defer cancel()
	p.Start(pctx)
	if clk.Pending() != 1 {
		t.Fatalf("pending tickers = %d, want 1", clk.Pending())
	}

	clk.Advance(5*time.Minute - time.Second)
	if got := len(s.Procs()); got != 0 {
		t.Fatalf("spawned %d before first tick, want 0", got)
	}
	clk.Advance(time.Second)
	waitRunning(t, p, 2)
	clk.Advance(5 * time.Minute)
	waitRunning(t, p, 4)

	// Pool is full: another tick spawns nothing.
	clk.Advance(5 * time.Minute)
	time.Sleep(5 * time.Millisecond)
	if got := len(s.Procs()); got != 4 {
		t.Fatalf("spawned %d total, want 4", got)
	}
}

func TestPokeStopsOnCancel(t *testing.T) {
	p, s, clk := newPokePool(t, time.Minute, 1)
	pctx, cancel := context.WithCancel(ctx)
	p.Start(pctx)
	cancel()
	p.bg.Wait()
	if clk.Pending() != 0 {
		t.Fatalf("pending tickers = %d after cancel, want 0", clk.Pending())
	}
	clk.Advance(time.Hour)
	if got := len(s.Procs()); got != 0 {
		t.Fatalf("spawned %d after cancel, want 0", got)
	}
}

func TestPokeZeroNeverTicks(t *testing.T) {
	p, s, clk := newPokePool(t, 0, 1)
	p.Start(ctx)
	if clk.Pending() != 0 {
		t.Fatalf("pending tickers = %d with poke 0, want 0", clk.Pending())
	}
	clk.Advance(24 * time.Hour)
	if got := len(s.Procs()); got != 0 {
		t.Fatalf("spawned %d with poke 0, want 0", got)
	}
}
