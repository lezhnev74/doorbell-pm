package pool

import (
	"context"
	"errors"
	"log/slog"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/proc/proctest"
)

func newPool(t *testing.T, concurrency int) (*Pool, *proctest.FakeSpawner) {
	t.Helper()
	p, s, _ := newBreakerPool(t, concurrency, nil)
	return p, s
}

// newBreakerPool builds a pool with breaker defaults (threshold 3, window
// 10s, cooldown 5s x2 up to 5m) that mod may override.
func newBreakerPool(t *testing.T, concurrency int, mod func(*config.PoolConfig)) (*Pool, *proctest.FakeSpawner, *clock.FakeClock) {
	t.Helper()
	return newLoggedPool(t, concurrency, mod, nil)
}

// newLoggedPool is newBreakerPool with a logger, for tests that assert on
// log records.
func newLoggedPool(t *testing.T, concurrency int, mod func(*config.PoolConfig), log *slog.Logger) (*Pool, *proctest.FakeSpawner, *clock.FakeClock) {
	t.Helper()
	spawner := &proctest.FakeSpawner{}
	clk := clock.NewFake(time.Unix(0, 0))
	cfg := config.PoolConfig{
		Name:                   "enc",
		Channel:                "enc",
		Command:                []string{"worker", "--queue=enc"},
		Concurrency:            concurrency,
		Dir:                    "/work",
		Env:                    map[string]string{"A": "1"},
		InheritEnv:             true,
		OkExitCodes:            []int{0},
		ExitFailureThreshold:   3,
		ExitFailureWindow:      10 * time.Second,
		ExitCooldown:           5 * time.Second,
		ExitCooldownMax:        5 * time.Minute,
		ExitCooldownMultiplier: 2,
		GraceShutdown:          time.Second,
		TermSignal:             syscall.SIGTERM,
	}
	if mod != nil {
		mod(&cfg)
	}
	return New(cfg, spawner, clk, log, nil), spawner, clk
}

func TestHintSpawnsUpToConcurrency(t *testing.T) {
	p, s := newPool(t, 4)
	ctx := context.Background()

	if got := p.Hint(ctx, "test", 100); got != 4 {
		t.Fatalf("first hint spawned %d, want 4", got)
	}
	if got := p.Hint(ctx, "test", 10); got != 0 {
		t.Fatalf("second hint spawned %d, want 0", got)
	}
	if got := p.Running(); got != 4 {
		t.Fatalf("running = %d, want 4", got)
	}
	if got := len(s.Procs()); got != 4 {
		t.Fatalf("spawned %d procs, want 4", got)
	}
}

func TestHintZeroSpawnsNothing(t *testing.T) {
	p, s := newPool(t, 4)
	if got := p.Hint(context.Background(), "test", 0); got != 0 {
		t.Fatalf("spawned %d, want 0", got)
	}
	if len(s.Procs()) != 0 {
		t.Fatal("hint 0 spawned a process")
	}
}

func TestReapFreesSlots(t *testing.T) {
	p, s := newPool(t, 4)
	ctx := context.Background()
	p.Hint(ctx, "test", 4)

	procs := s.Procs()
	procs[0].Finish(0)
	procs[1].Finish(1)
	waitRunning(t, p, 2)

	if got := p.Hint(ctx, "test", 10); got != 2 {
		t.Fatalf("hint after two exits spawned %d, want 2", got)
	}
	if got := p.Running(); got != 4 {
		t.Fatalf("running = %d, want 4", got)
	}
}

func TestSpawnErrorStopsHint(t *testing.T) {
	p, s := newPool(t, 4)
	s.Err = errors.New("exec: not found")
	if got := p.Hint(context.Background(), "test", 4); got != 0 {
		t.Fatalf("spawned %d with failing spawner, want 0", got)
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("running = %d after spawn error, want 0", got)
	}
}

func TestSpecFromConfig(t *testing.T) {
	p, s := newPool(t, 1)
	p.Hint(context.Background(), "test", 1)
	spec := s.Specs()[0]
	if spec.Cmd[0] != "worker" || spec.Dir != "/work" || spec.Env["A"] != "1" || !spec.InheritEnv {
		t.Fatalf("spec = %+v", spec)
	}
}

func waitRunning(t *testing.T, p *Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if p.Running() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("running = %d, want %d", p.Running(), want)
}
