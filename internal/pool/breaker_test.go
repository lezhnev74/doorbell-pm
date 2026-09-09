package pool

import (
	"context"
	"errors"
	"os"
	"slices"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/proc/proctest"
)

var ctx = context.Background()

// exit spawns one worker and ends it with code, waiting for the reaper.
func exit(t *testing.T, p *Pool, s *proctest.FakeSpawner, code int) {
	t.Helper()
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint spawned %d, want 1", got)
	}
	before := p.Running() - 1
	procs := s.Procs()
	procs[len(procs)-1].Finish(code)
	waitRunning(t, p, before)
}

// trip produces threshold failures back to back.
func trip(t *testing.T, p *Pool, s *proctest.FakeSpawner) {
	t.Helper()
	for i := 0; i < p.cfg.ExitFailureThreshold; i++ {
		exit(t, p, s, 1)
	}
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint after %d failures spawned %d, want 0", p.cfg.ExitFailureThreshold, got)
	}
}

// cooldown advances the fake clock in 1s steps until a hint spawns again and
// returns how long the pool stayed closed.
func cooldown(t *testing.T, p *Pool, s *proctest.FakeSpawner) time.Duration {
	t.Helper()
	var d time.Duration
	for ; d < time.Hour; d += time.Second {
		if p.Hint(ctx, "test", 1) == 1 {
			procs := s.Procs()
			procs[len(procs)-1].Finish(0)
			waitRunning(t, p, 0)
			return d
		}
		p.clock.(interface{ Advance(time.Duration) }).Advance(time.Second)
	}
	t.Fatal("pool never reopened")
	return 0
}

func TestBreakerTripsOnThirdFailure(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, nil)
	exit(t, p, s, 1)
	exit(t, p, s, 1)
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint after two failures spawned %d, want 1", got)
	}
	s.Procs()[2].Finish(1)
	waitRunning(t, p, 0)
	if got := p.Hint(ctx, "test", 4); got != 0 {
		t.Fatalf("hint after third failure spawned %d, want 0", got)
	}
}

func TestBreakerReopensAfterCooldown(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, nil)
	trip(t, p, s)
	clk.Advance(4 * time.Second)
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint inside cooldown spawned %d, want 0", got)
	}
	clk.Advance(time.Second)
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint after cooldown spawned %d, want 1", got)
	}
}

func TestSpawnErrorCountsAsFailure(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, nil)
	s.Err = errors.New("exec: not found")
	for i := 0; i < 3; i++ {
		p.Hint(ctx, "test", 1)
	}
	s.Err = nil
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint after three spawn errors spawned %d, want 0", got)
	}
	clk.Advance(5 * time.Second)
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint after cooldown spawned %d, want 1", got)
	}
}

func TestKilledByDoorbellIsNeutral(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.TTL = time.Second
		c.GraceShutdown = 10 * time.Millisecond
	})
	p.Hint(ctx, "test", 3)
	clk.Advance(time.Second) // ttl fires, workers ignore TERM and get KILLed
	waitRunning(t, p, 0)
	for _, pr := range s.Procs() {
		if !slices.Contains(pr.Signals(), os.Signal(syscall.SIGKILL)) {
			t.Fatalf("pid %d signals = %v, want SIGKILL", pr.PID(), pr.Signals())
		}
	}
	if got := p.Hint(ctx, "test", 3); got != 3 {
		t.Fatalf("hint after three kills spawned %d, want 3", got)
	}
}

func TestExternalSignalIsFailure(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, nil)
	p.Hint(ctx, "test", 3)
	for _, pr := range s.Procs() {
		pr.Kill(syscall.SIGKILL)
	}
	waitRunning(t, p, 0)
	if got := p.Hint(ctx, "test", 3); got != 0 {
		t.Fatalf("hint after three external kills spawned %d, want 0", got)
	}
}

func TestOkExitCodesDoNotCount(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, func(c *config.PoolConfig) { c.OkExitCodes = []int{0, 3} })
	for i := 0; i < 3; i++ {
		exit(t, p, s, 3)
	}
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint after three ok exits spawned %d, want 1", got)
	}
}

func TestCooldownGrowsAndCaps(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, func(c *config.PoolConfig) { c.ExitCooldownMax = 12 * time.Second })
	trip(t, p, s)
	for i, want := range []time.Duration{5 * time.Second, 10 * time.Second, 12 * time.Second, 12 * time.Second} {
		if got := cooldownNoReset(t, p, s); got != want {
			t.Fatalf("trip %d: cooldown = %s, want %s", i+1, got, want)
		}
	}
}

// cooldownNoReset is cooldown() but the probe worker fails and two more
// failures follow at the same instant, so the pool re-trips one level up.
func cooldownNoReset(t *testing.T, p *Pool, s *proctest.FakeSpawner) time.Duration {
	t.Helper()
	var d time.Duration
	for ; d < time.Hour; d += time.Second {
		if p.Hint(ctx, "test", 1) == 1 {
			procs := s.Procs()
			procs[len(procs)-1].Finish(1)
			waitRunning(t, p, 0)
			// two more failures in the same instant complete the ring
			exit(t, p, s, 1)
			exit(t, p, s, 1)
			if got := p.Hint(ctx, "test", 1); got != 0 {
				t.Fatalf("pool did not re-trip")
			}
			return d
		}
		p.clock.(interface{ Advance(time.Duration) }).Advance(time.Second)
	}
	t.Fatal("pool never reopened")
	return 0
}

func TestLevelResetsOnOkExitWithEmptyWindow(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, nil)
	trip(t, p, s)
	if got := cooldown(t, p, s); got != 5*time.Second {
		t.Fatalf("first cooldown = %s, want 5s", got)
	}
	// probe exited ok with an empty window: level is back to 0
	trip(t, p, s)
	if got := cooldown(t, p, s); got != 5*time.Second {
		t.Fatalf("cooldown after reset = %s, want 5s", got)
	}
}

func TestOkExitInsideWindowKeepsLevel(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, nil)
	trip(t, p, s)
	clk.Advance(5 * time.Second)
	exit(t, p, s, 1) // failure inside the window
	exit(t, p, s, 0) // ok exit does not reset the level
	exit(t, p, s, 1)
	exit(t, p, s, 1) // third failure in the ring: trip at level 1
	if got := cooldown(t, p, s); got != 10*time.Second {
		t.Fatalf("second cooldown = %s, want 10s", got)
	}
}

func TestLevelDecaysAfterMaxCooldown(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, func(c *config.PoolConfig) { c.ExitCooldownMax = 12 * time.Second })
	trip(t, p, s)
	clk.Advance(5*time.Second + 12*time.Second)
	trip(t, p, s)
	if got := cooldown(t, p, s); got != 5*time.Second {
		t.Fatalf("cooldown after decay = %s, want 5s", got)
	}
}

func TestZeroDisablesBreaker(t *testing.T) {
	for name, mod := range map[string]func(*config.PoolConfig){
		"threshold": func(c *config.PoolConfig) { c.ExitFailureThreshold = 0 },
		"cooldown":  func(c *config.PoolConfig) { c.ExitCooldown = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			p, s, _ := newBreakerPool(t, 4, mod)
			for i := 0; i < 5; i++ {
				exit(t, p, s, 1)
			}
			if got := p.Hint(ctx, "test", 1); got != 1 {
				t.Fatalf("hint after five failures spawned %d, want 1", got)
			}
		})
	}
}

func TestFailuresOutsideWindowNeverTrip(t *testing.T) {
	p, s, clk := newBreakerPool(t, 4, nil)
	for i := 0; i < 8; i++ {
		exit(t, p, s, i%2) // 1 0 1 0 ..., failures 8s apart in a 10s window
		clk.Advance(4 * time.Second)
	}
	if got := p.Hint(ctx, "test", 1); got != 1 {
		t.Fatalf("hint spawned %d, want 1", got)
	}
}

func TestFastFailuresLeaveLongWorkerAlone(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, nil)
	p.Hint(ctx, "test", 4)
	procs := s.Procs()
	for _, pr := range procs[:3] {
		pr.Finish(1)
	}
	waitRunning(t, p, 1)
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint after trip spawned %d, want 0", got)
	}
	long := procs[3]
	if long.Finished() || len(long.Signals()) != 0 {
		t.Fatalf("long worker touched: finished=%v signals=%v", long.Finished(), long.Signals())
	}
}
