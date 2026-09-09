// Package pool keeps a bounded number of worker processes running for one
// configured pool. It depends only on proc.Spawner and clock.Clock so the
// logic is unit-testable without real processes or sleeping.
package pool

import (
	"context"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"sync"
	"syscall"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/proc"
)

// Kill reasons, also the `code` label of a killed exit.
const (
	KillTTL      = "ttl"
	KillShutdown = "shutdown"
)

// Pool tracks how many workers are alive and spawns more on demand, never
// exceeding the configured concurrency.
type Pool struct {
	cfg     config.PoolConfig
	spawner proc.Spawner
	clock   clock.Clock
	log     *slog.Logger
	metrics Metrics

	mu           sync.Mutex
	running      int
	breaker      *breaker
	lastHint     time.Time // zero until the first hint
	lastExit     time.Time // zero until the first reaped exit
	lastExitCode int
	lastTasks    int           // count from the last exit that reported one
	hasLastTasks bool          // false until a worker reported a count
	stopping     bool          // set by Shutdown: hints are dropped
	stop         chan struct{} // closed by Shutdown: reapers terminate their worker
	reapers      sync.WaitGroup
	bg           sync.WaitGroup // poke ticker
}

// New returns a pool with no running workers. A nil log means
// slog.Default(); a nil m means no metrics.
func New(cfg config.PoolConfig, spawner proc.Spawner, clk clock.Clock, log *slog.Logger, m Metrics) *Pool {
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = nopMetrics{}
	}
	return &Pool{
		cfg:     cfg,
		spawner: spawner,
		clock:   clk,
		log:     log.With("pool", cfg.Name),
		metrics: m,
		breaker: newBreaker(cfg),
		stop:    make(chan struct{}),
	}
}

// Name is the pool's config key.
func (p *Pool) Name() string { return p.cfg.Name }

// Running is the number of workers that have been spawned and not yet reaped.
func (p *Pool) Running() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.running
}

// Stats is a point-in-time snapshot of the pool for /healthz and metrics.
// Zero LastHint / LastExit mean "never"; LastExitCode is valid only when
// LastExit is set. LastTasks is the count from the most recent exit that
// reported one, valid only when HasLastTasks. BreakerOpenUntil is zero while
// spawning is allowed.
type Stats struct {
	Running          int
	Concurrency      int
	LastHint         time.Time
	LastExit         time.Time
	LastExitCode     int
	LastTasks        int
	HasLastTasks     bool
	BreakerOpenUntil time.Time
	BreakerLevel     int
}

// Stats returns a consistent snapshot taken under the pool mutex.
func (p *Pool) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := Stats{
		Running:      p.running,
		Concurrency:  p.cfg.Concurrency,
		LastHint:     p.lastHint,
		LastExit:     p.lastExit,
		LastExitCode: p.lastExitCode,
		LastTasks:    p.lastTasks,
		HasLastTasks: p.hasLastTasks,
		BreakerLevel: p.breaker.level,
	}
	if p.breaker.open(p.clock.Now()) {
		s.BreakerOpenUntil = p.breaker.openUntil
	}
	return s
}

// Hint asks for up to n more workers on behalf of source (hint.SourceRedis,
// hint.SourceHTTP, hint.SourcePoke, hint.SourceWorker) and returns how many were started:
// min(n, concurrency - running), fewer if a spawn fails, none while the
// failure breaker is open. The lock is held across the spawns so concurrent
// hints cannot oversubscribe the pool.
func (p *Pool) Hint(ctx context.Context, source string, n int) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopping {
		p.log.Debug("drop", "reason", "shutdown", "source", source, "channel", p.cfg.Channel, "count", n)
		p.metrics.Drop(source, p.cfg.Name, "shutdown")
		return 0
	}
	p.lastHint = p.clock.Now()
	p.metrics.Hint(source, p.cfg.Name)
	if p.breaker.open(p.lastHint) {
		p.log.Debug("drop", "reason", "exit_cooldown", "source", source, "channel", p.cfg.Channel, "count", n, "open_until", p.breaker.openUntil)
		p.metrics.Drop(source, p.cfg.Name, "exit_cooldown")
		return 0
	}
	want := min(n, p.cfg.Concurrency-p.running)
	spawned := 0
	for ; spawned < want; spawned++ {
		if !p.spawnLocked(ctx, source) {
			break
		}
	}
	return spawned
}

// spawnLocked starts one worker and its reaper on behalf of the hint
// source. A spawn error counts as an instant failure. Caller holds p.mu.
func (p *Pool) spawnLocked(ctx context.Context, source string) bool {
	pr, err := p.spawner.Spawn(ctx, p.spec())
	if err != nil {
		p.log.Error("spawn_error", "source", source, "err", err)
		p.metrics.SpawnError(p.cfg.Name)
		p.failureLocked("err", err)
		return false
	}
	p.running++
	p.metrics.Spawn(p.cfg.Name)
	p.log.Info("spawn", "pid", pr.PID(), "source", source, "running", p.running)
	p.reapers.Add(1)
	go p.reap(pr, p.clock.Now(), p.ttlTimer())
	return true
}

// ttlTimer arms the ttl deadline for a worker spawned now, nil when ttl is
// 0 (forever). Armed synchronously so the deadline is set before Hint
// returns.
func (p *Pool) ttlTimer() clock.Timer {
	if p.cfg.TTL <= 0 {
		return nil
	}
	return p.clock.NewTimer(p.cfg.TTL)
}

func (p *Pool) spec() proc.Spec {
	return proc.Spec{
		Cmd:          p.cfg.Command,
		Dir:          p.cfg.Dir,
		Env:          p.cfg.Env,
		InheritEnv:   p.cfg.InheritEnv,
		ResultStream: proc.Stream(p.cfg.ResultStream),
	}
}

// exitRecord is what reap learned about one finished worker.
type exitRecord struct {
	pid        int
	exit       proc.Exit
	killReason string // "" when the worker ended by itself
	result     string // ok, failure or killed
	running    int    // workers left after this one
	duration   time.Duration
	tasks      int
	hasTasks   bool
}

// reap waits for one worker to end, releases its slot, feeds the breaker and,
// when the worker reported tasks on an ok exit, hints the pool for as many
// more. It is the only reader of pr.Done, so it also owns the ttl kill.
func (p *Pool) reap(pr proc.Process, started time.Time, ttl clock.Timer) {
	defer p.reapers.Done()
	r := exitRecord{pid: pr.PID()}
	r.exit, r.killReason = p.wait(pr, ttl)
	r.tasks, r.hasTasks = parseTasks(r.exit.Result)

	exitedAt := p.clock.Now()
	r.result, r.running = p.recordExit(r, exitedAt)
	r.duration = exitedAt.Sub(started)
	p.metrics.Exit(p.cfg.Name, r.result, exitCode(r.exit, r.killReason), r.duration)
	if r.hasTasks {
		p.metrics.Tasks(p.cfg.Name, r.tasks)
	}
	p.logExit(r)

	// A worker that quit voluntarily after N tasks probably left more
	// behind. Hint takes the mutex, so this runs after the unlock; the
	// breaker, the cap and shutdown apply as to any other hint.
	if r.result == "ok" && r.tasks > 0 {
		p.Hint(context.Background(), hint.SourceWorker, r.tasks)
	}
}

// recordExit updates the pool state and the breaker under the lock and
// returns the exit classification and the remaining worker count.
func (p *Pool) recordExit(r exitRecord, exitedAt time.Time) (result string, running int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.running--
	p.lastExit = exitedAt
	p.lastExitCode = r.exit.Code
	if r.hasTasks {
		p.lastTasks = r.tasks
		p.hasLastTasks = true
	}
	result = p.classify(r.exit, r.killReason != "")
	switch result {
	case "ok":
		p.breaker.ok(exitedAt)
	case "failure":
		p.failureLocked("code", r.exit.Code)
	}
	return result, p.running
}

func (p *Pool) logExit(r exitRecord) {
	p.log.Info("exit", r.attrs()...)
	if !r.hasTasks && r.exit.Result != "" {
		p.log.Debug("result_ignored", "pid", r.pid, "line", r.exit.Result)
	}
}

func (r exitRecord) attrs() []any {
	attrs := []any{"pid", r.pid, "code", r.exit.Code, "result", r.result, "running", r.running, "duration", r.duration}
	if r.killReason != "" {
		attrs = append(attrs, "reason", r.killReason)
	}
	if r.exit.Signal != nil {
		attrs = append(attrs, "signal", signalName(r.exit.Signal))
	}
	if r.exit.Err != nil {
		attrs = append(attrs, "err", r.exit.Err)
	}
	if r.hasTasks {
		attrs = append(attrs, "tasks", r.tasks)
	}
	return attrs
}

// wait blocks until pr ends on its own, ttl (nil = never) fires or the pool
// shuts down. In the last two cases it terminates pr with term_signal and
// grace_shutdown and reports the kill reason; "" means pr ended by itself.
func (p *Pool) wait(pr proc.Process, ttl clock.Timer) (proc.Exit, string) {
	var ttlC <-chan time.Time
	if ttl != nil {
		defer ttl.Stop()
		ttlC = ttl.C()
	}
	var reason string
	select {
	case exit := <-pr.Done():
		return exit, ""
	case <-ttlC:
		reason = KillTTL
		p.log.Info("ttl_kill", "pid", pr.PID(), "ttl", p.cfg.TTL,
			"signal", signalName(p.cfg.TermSignal), "grace", p.cfg.GraceShutdown)
	case <-p.stop:
		reason = KillShutdown
		p.log.Info("shutdown_kill", "pid", pr.PID(),
			"signal", signalName(p.cfg.TermSignal), "grace", p.cfg.GraceShutdown)
	}
	p.metrics.Kill(p.cfg.Name, reason)
	return proc.Terminate(pr, p.cfg.TermSignal, p.cfg.GraceShutdown), reason
}

// exitCode is the `code` label of an exit: the kill reason for a kill, the
// signal name for a signalled exit, else the numeric code.
func exitCode(exit proc.Exit, killReason string) string {
	switch {
	case killReason != "":
		return killReason
	case exit.Signal != nil:
		return signalName(exit.Signal)
	default:
		return strconv.Itoa(exit.Code)
	}
}

// signalName is the config spelling of a signal (SIGTERM), the same in
// logs and metric labels.
func signalName(sig os.Signal) string {
	if s, ok := sig.(syscall.Signal); ok {
		return config.Signal(s).String()
	}
	return sig.String()
}

// Shutdown stops accepting hints, terminates every running worker
// concurrently (term_signal, then group kill after grace_shutdown) and waits
// for the reapers and the poke ticker. It returns early with ctx.Err() if ctx
// ends first; the kills still complete in the background. Safe to call more
// than once.
func (p *Pool) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if !p.stopping {
		p.stopping = true
		close(p.stop)
		p.log.Info("shutdown", "running", p.running)
	}
	p.mu.Unlock()

	done := make(chan struct{})
	go func() {
		p.reapers.Wait()
		p.bg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// classify names an exit: ok (code in ok_exit_codes), killed (by doorbell)
// or failure (any other code, external signal, wait error).
func (p *Pool) classify(exit proc.Exit, killed bool) string {
	switch {
	case killed:
		return "killed"
	case exit.Signal == nil && exit.Err == nil && slices.Contains(p.cfg.OkExitCodes, exit.Code):
		return "ok"
	default:
		return "failure"
	}
}

// failureLocked records a failure and logs a trip. Caller holds p.mu.
func (p *Pool) failureLocked(lastKey string, last any) {
	d, tripped := p.breaker.failure(p.clock.Now())
	if !tripped {
		return
	}
	p.metrics.Trip(p.cfg.Name)
	p.log.Error("breaker_trip",
		"failures", p.breaker.threshold, "window", p.breaker.window,
		"cooldown", d, "breaker_level", p.breaker.level, "open_until", p.breaker.openUntil,
		lastKey, last)
}
