package pool

import (
	"context"
	"os"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/config"
)

func TestShutdownTerminatesAllAndWaits(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.GraceShutdown = 20 * time.Millisecond
		c.ExitFailureThreshold = 1
	})
	if got := p.Hint(ctx, "test", 2); got != 2 {
		t.Fatalf("hint spawned %d, want 2", got)
	}

	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	for _, pr := range s.Procs() {
		if sig := pr.Signals(); len(sig) == 0 || sig[0] != os.Signal(syscall.SIGTERM) {
			t.Fatalf("pid %d signals = %v, want SIGTERM first", pr.PID(), sig)
		}
		if !pr.Finished() {
			t.Fatalf("pid %d still running after Shutdown", pr.PID())
		}
	}
	if got := p.Running(); got != 0 {
		t.Fatalf("running = %d after Shutdown, want 0", got)
	}
	if got := p.Hint(ctx, "test", 1); got != 0 {
		t.Fatalf("hint after Shutdown spawned %d, want 0", got)
	}
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("second Shutdown = %v", err)
	}
}

func TestShutdownCooperativeExitIsKilledNotFailure(t *testing.T) {
	p, s, _ := newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.GraceShutdown = time.Minute
		c.ExitFailureThreshold = 1
		c.TermSignal = syscall.SIGINT
	})
	p.Hint(ctx, "test", 1)
	pr := s.Procs()[0]

	go func() {
		waitSignal(t, pr, os.Signal(syscall.SIGINT))
		pr.Finish(1) // non-zero, but killed by doorbell: neutral for the breaker
	}()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := pr.Signals(); len(got) != 1 {
		t.Fatalf("signals = %v, want only SIGINT", got)
	}
	if p.breaker.open(p.clock.Now()) {
		t.Fatal("breaker tripped by a shutdown kill")
	}
}

func TestShutdownHonoursContext(t *testing.T) {
	p, _, _ := newBreakerPool(t, 4, func(c *config.PoolConfig) {
		c.GraceShutdown = time.Hour
	})
	p.Hint(ctx, "test", 1)

	sctx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
	defer cancel()
	if err := p.Shutdown(sctx); err != context.DeadlineExceeded {
		t.Fatalf("Shutdown = %v, want DeadlineExceeded", err)
	}
}

func TestShutdownStopsPoke(t *testing.T) {
	p, s, clk := newPokePool(t, time.Minute, 1)
	p.Start(ctx)
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if clk.Pending() != 0 {
		t.Fatalf("pending tickers = %d after Shutdown, want 0", clk.Pending())
	}
	clk.Advance(time.Hour)
	if got := len(s.Procs()); got != 0 {
		t.Fatalf("spawned %d after Shutdown, want 0", got)
	}
}
