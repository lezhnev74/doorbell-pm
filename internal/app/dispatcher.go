// Package app composes hint sources, the dispatcher and the pools into one
// runnable service.
package app

import (
	"context"
	"log/slog"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
)

// Hinter is the part of a pool the dispatcher needs.
type Hinter interface {
	Hint(ctx context.Context, source string, n int) int
}

// Metrics receives the hints the dispatcher drops (reason
// unknown_pool|disabled; pool "" when unknown). nil in NewDispatcher means
// a no-op.
type Metrics interface {
	Drop(source, pool, reason string)
}

type nopMetrics struct{}

func (nopMetrics) Drop(string, string, string) {}

// Dispatcher routes hints to pools by channel name. A channel that maps to a
// disabled pool or to no pool at all is dropped with a log line.
type Dispatcher struct {
	routes  map[string]route
	log     *slog.Logger
	metrics Metrics
}

// route is one channel's destination; a nil target means the pool exists
// in the config but is disabled.
type route struct {
	pool   string
	target Hinter
}

// NewDispatcher builds routes from the resolved pool configs. pools holds a
// Hinter per enabled pool name; a config whose name is missing from pools
// or that is disabled routes to a drop. A nil log means slog.Default(); a
// nil m means no metrics.
func NewDispatcher(cfgs []config.PoolConfig, pools map[string]Hinter, log *slog.Logger, m Metrics) *Dispatcher {
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = nopMetrics{}
	}
	d := &Dispatcher{routes: make(map[string]route, len(cfgs)), log: log, metrics: m}
	for _, c := range cfgs {
		r := route{pool: c.Name}
		if c.Enabled {
			r.target = pools[c.Name]
		}
		d.routes[c.Channel] = r
	}
	return d
}

// Dispatch delivers one hint and returns how many workers its pool started.
// Unknown channels and disabled pools spawn nothing.
func (d *Dispatcher) Dispatch(ctx context.Context, h hint.Hint) int {
	r, ok := d.routes[h.Pool]
	switch {
	case !ok:
		d.log.Warn("drop", "reason", "unknown_pool", "source", h.Source, "channel", h.Pool, "count", h.Count)
		d.metrics.Drop(h.Source, "", "unknown_pool")
		return 0
	case r.target == nil:
		d.log.Debug("drop", "reason", "disabled", "source", h.Source, "pool", r.pool, "channel", h.Pool, "count", h.Count)
		d.metrics.Drop(h.Source, r.pool, "disabled")
		return 0
	}
	n := r.target.Hint(ctx, h.Source, h.Count)
	d.log.Debug("hint", "source", h.Source, "pool", r.pool, "channel", h.Pool, "count", h.Count, "spawned", n)
	return n
}

// Run dispatches every hint from in until ctx ends or in is closed.
func (d *Dispatcher) Run(ctx context.Context, in <-chan hint.Hint) {
	for {
		select {
		case <-ctx.Done():
			return
		case h, ok := <-in:
			if !ok {
				return
			}
			d.Dispatch(ctx, h)
		}
	}
}
