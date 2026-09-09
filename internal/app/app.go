package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/httpapi"
	"doorbell-pm/internal/metrics"
	"doorbell-pm/internal/pool"
	"doorbell-pm/internal/proc"
	"doorbell-pm/internal/redisin"
)

// hintBuffer decouples sources from the dispatcher; a hint is tiny, so a
// small buffer absorbs bursts without letting sources block on a slow spawn.
const hintBuffer = 64

// SpawnerFunc builds the spawner for one pool. The default is
// proc.NewExecSpawner for every pool.
type SpawnerFunc func(config.PoolConfig) proc.Spawner

// App wires the enabled hint sources through the dispatcher into the pools
// and owns their lifecycle.
type App struct {
	cfg        config.Config
	log        *slog.Logger
	pools      map[string]*pool.Pool // enabled pools by name
	dispatcher *Dispatcher
	http       *httpapi.Server        // nil when http is disabled
	metrics    *httpapi.MetricsServer // nil unless metrics.addr is set
	sources    map[string]hint.Source
}

// New builds an app from a finalized config with real processes and the wall
// clock. version labels the build_info metric. A nil log means
// slog.Default().
func New(cfg config.Config, version string, log *slog.Logger) *App {
	return newApp(cfg, version, log, clock.Real{}, nil)
}

// newApp is New with injectable clock and spawner factory for tests.
func newApp(cfg config.Config, version string, log *slog.Logger, clk clock.Clock, spawnerFor SpawnerFunc) *App {
	if log == nil {
		log = slog.Default()
	}
	if spawnerFor == nil {
		spawnerFor = func(config.PoolConfig) proc.Spawner { return proc.NewExecSpawner() }
	}
	a := &App{
		cfg:     cfg,
		log:     log,
		pools:   map[string]*pool.Pool{},
		sources: map[string]hint.Source{},
	}
	cfgs := cfg.ResolvedPools()
	names, channels := enabledPools(cfgs)

	// Typed nils would not be nil behind the interfaces, so keep each one
	// untyped while metrics are off.
	var (
		poolMetrics  pool.Metrics
		dispMetrics  Metrics
		redisMetrics redisin.Metrics
		handler      http.Handler
	)
	if cfg.Metrics.IsEnabled() {
		m := metrics.New(cfg.Metrics, version, names, a.stats)
		poolMetrics, dispMetrics, redisMetrics, handler = m, m, m, m.Handler()
	}

	hinters := a.newPools(cfgs, spawnerFor, clk, poolMetrics)
	a.dispatcher = NewDispatcher(cfgs, hinters, log, dispMetrics)
	a.newSources(channels, clk, handler, redisMetrics)
	return a
}

// enabledPools lists the names and channels of the enabled pools, in order.
func enabledPools(cfgs []config.PoolConfig) (names, channels []string) {
	for _, pc := range cfgs {
		if pc.Enabled {
			names = append(names, pc.Name)
			channels = append(channels, pc.Channel)
		}
	}
	return names, channels
}

// newPools builds one pool per enabled config and returns them as hinters
// for the dispatcher.
func (a *App) newPools(cfgs []config.PoolConfig, spawnerFor SpawnerFunc, clk clock.Clock, m pool.Metrics) map[string]Hinter {
	hinters := map[string]Hinter{}
	for _, pc := range cfgs {
		if !pc.Enabled {
			continue
		}
		p := pool.New(pc, spawnerFor(pc), clk, a.log, m)
		a.pools[pc.Name] = p
		hinters[pc.Name] = p
	}
	return hinters
}

// newSources wires the hint sources and the listeners. The metrics handler
// goes to its own listener when metrics.addr is set, otherwise onto the
// hint listener.
func (a *App) newSources(channels []string, clk clock.Clock, metricsHandler http.Handler, redisMetrics redisin.Metrics) {
	cfg := a.cfg
	mainMetrics := metricsHandler
	if metricsHandler != nil && cfg.Metrics.Addr != "" {
		a.metrics = httpapi.NewMetricsServer(cfg.HTTP, cfg.Metrics.Addr, metricsHandler, a.log)
		mainMetrics = nil
	}
	if cfg.HTTP.IsEnabled() {
		a.http = httpapi.New(cfg.HTTP, cfg.Redis.ChannelPrefix, channels, a.stats, mainMetrics, a.log)
		a.sources[hint.SourceHTTP] = a.http
	}
	if cfg.Redis.IsEnabled() {
		a.sources[hint.SourceRedis] = redisin.New(cfg.Redis, clk, a.log, redisMetrics)
	}
}

// MetricsAddr is the bound address of the standalone metrics listener,
// empty until it is listening or when metrics.addr is not set.
func (a *App) MetricsAddr() string {
	if a.metrics == nil {
		return ""
	}
	return a.metrics.Addr()
}

// HTTPAddr is the bound http listener address, empty until it is listening
// or when http is disabled.
func (a *App) HTTPAddr() string {
	if a.http == nil {
		return ""
	}
	return a.http.Addr()
}

// stats feeds the health endpoint.
func (a *App) stats() map[string]pool.Stats {
	out := make(map[string]pool.Stats, len(a.pools))
	for name, p := range a.pools {
		out[name] = p.Stats()
	}
	return out
}

// Run starts everything and blocks until ctx ends or a source fails, then
// shuts down in order: sources, dispatcher, pools (each with its own
// grace_shutdown), the whole sequence bounded by shutdown_timeout. It
// returns nil after a clean stop, otherwise the source error and whatever
// went wrong during shutdown, joined.
func (a *App) Run(ctx context.Context) error {
	runCtx, stopRun := context.WithCancel(context.Background())
	defer stopRun()

	hints := make(chan hint.Hint, hintBuffer)
	for _, p := range a.pools {
		p.Start(runCtx)
	}
	dispatched := make(chan struct{})
	go func() {
		defer close(dispatched)
		a.dispatcher.Run(runCtx, hints)
	}()
	sources, errc := a.startSources(runCtx, hints)
	a.log.Info("started", "pools", len(a.pools), "sources", len(a.sources))

	var errs []error
	select {
	case <-ctx.Done():
		a.log.Info("shutting down", "timeout", a.cfg.ShutdownTimeout)
	case err := <-errc:
		a.log.Error("source failed, shutting down", "err", err)
		errs = append(errs, err)
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), a.cfg.ShutdownTimeout.Std())
	defer cancel()
	stopRun()
	errs = append(errs, a.stop(stopCtx, sources, dispatched, errc)...)
	return errors.Join(errs...)
}

// startSources runs every hint source and the standalone metrics listener in
// the background. The first failure of each lands in errc, which has room
// for all of them.
func (a *App) startSources(ctx context.Context, hints chan hint.Hint) (*sync.WaitGroup, chan error) {
	var wg sync.WaitGroup
	errc := make(chan error, len(a.sources)+1)
	for name, src := range a.sources {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := src.Run(ctx, hints); err != nil {
				errc <- fmt.Errorf("%s source: %w", name, err)
			}
		}()
	}
	if a.metrics != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := a.metrics.Run(ctx); err != nil {
				errc <- fmt.Errorf("metrics listener: %w", err)
			}
		}()
	}
	return &wg, errc
}

// stop waits for the sources and the dispatcher, then terminates the pools,
// every step bounded by ctx. Failures that raced the run context are
// reported too, since the sources are done by then.
func (a *App) stop(ctx context.Context, sources *sync.WaitGroup, dispatched <-chan struct{}, errc chan error) []error {
	var errs []error
	if err := await(ctx, sources.Wait); err != nil {
		errs = append(errs, fmt.Errorf("stop sources: %w", err))
	}
	if err := await(ctx, func() { <-dispatched }); err != nil {
		errs = append(errs, fmt.Errorf("stop dispatcher: %w", err))
	}
	errs = append(errs, a.shutdownPools(ctx)...)
	for len(errc) > 0 {
		errs = append(errs, <-errc)
	}
	return errs
}

// shutdownPools terminates every pool concurrently and collects the errors.
func (a *App) shutdownPools(ctx context.Context) []error {
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
	)
	for name, p := range a.pools {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := p.Shutdown(ctx); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("pool %s: shutdown: %w", name, err))
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}

// await runs wait in the background and returns ctx.Err() if ctx ends
// before it does.
func await(ctx context.Context, wait func()) error {
	done := make(chan struct{})
	go func() {
		wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
