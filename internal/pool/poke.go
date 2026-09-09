package pool

import (
	"context"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/hint"
)

// Start launches the poke ticker: every poke interval the pool hints itself
// with poke_count so work resumes without an external hint, at a bounded
// rate. No-op when poke is 0. The ticker stops when ctx ends or Shutdown
// is called.
func (p *Pool) Start(ctx context.Context) {
	if p.cfg.Poke <= 0 {
		return
	}
	t := p.clock.NewTicker(p.cfg.Poke)
	p.bg.Add(1)
	go func() {
		defer p.bg.Done()
		defer t.Stop()
		p.pokeLoop(ctx, t)
	}()
}

func (p *Pool) pokeLoop(ctx context.Context, t clock.Ticker) {
	for p.awaitTick(ctx, t) {
		n := p.Hint(ctx, hint.SourcePoke, p.cfg.PokeCount)
		p.log.Debug("hint", "source", hint.SourcePoke, "channel", p.cfg.Channel, "count", p.cfg.PokeCount, "spawned", n)
	}
}

// awaitTick blocks until the next tick and reports whether the pool is still
// running; false once ctx ends or Shutdown is called.
func (p *Pool) awaitTick(ctx context.Context, t clock.Ticker) bool {
	select {
	case <-ctx.Done():
		return false
	case <-p.stop:
		return false
	case <-t.C():
		return ctx.Err() == nil
	}
}
