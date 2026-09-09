package pool

import (
	"testing"
	"time"

	"doorbell-pm/internal/config"
)

func TestStatsSnapshot(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, func(c *config.PoolConfig) { c.ExitFailureThreshold = 1 })

	st := p.Stats()
	if st.Running != 0 || st.Concurrency != 4 || !st.LastHint.IsZero() || !st.LastExit.IsZero() ||
		!st.BreakerOpenUntil.IsZero() || st.BreakerLevel != 0 {
		t.Fatalf("fresh stats = %+v", st)
	}

	clk.Advance(time.Second)
	p.Hint(ctx, "test", 2)
	st = p.Stats()
	if st.Running != 2 || !st.LastHint.Equal(clk.Now()) || !st.LastExit.IsZero() {
		t.Fatalf("after hint stats = %+v", st)
	}

	clk.Advance(time.Second)
	s.Procs()[0].Finish(7)
	waitRunning(t, p, 1)
	st = p.Stats()
	if st.Running != 1 || !st.LastExit.Equal(clk.Now()) || st.LastExitCode != 7 {
		t.Fatalf("after exit stats = %+v", st)
	}
	if want := clk.Now().Add(5 * time.Second); !st.BreakerOpenUntil.Equal(want) || st.BreakerLevel != 1 {
		t.Fatalf("breaker stats = %+v, want open until %v level 1", st, want)
	}

	clk.Advance(5 * time.Second)
	if st = p.Stats(); !st.BreakerOpenUntil.IsZero() || st.BreakerLevel != 1 {
		t.Fatalf("after cooldown stats = %+v, want closed at level 1", st)
	}
}

func TestStatsLastHintSetWhenDroppedByBreaker(t *testing.T) {
	p, s, clk := newBreakerPool(t, 1, func(c *config.PoolConfig) { c.ExitFailureThreshold = 1 })
	p.Hint(ctx, "test", 1)
	s.Procs()[0].Finish(1)
	waitRunning(t, p, 0)

	clk.Advance(time.Second)
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint inside cooldown spawned %d", got)
	}
	if st := p.Stats(); !st.LastHint.Equal(clk.Now()) {
		t.Fatalf("last hint = %v, want %v", st.LastHint, clk.Now())
	}
}
