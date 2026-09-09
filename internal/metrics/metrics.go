// Package metrics exposes doorbell's prometheus metrics. Counters and the
// histogram are pushed by the pools, the dispatcher and the redis source
// through their small Metrics interfaces; pool gauges are pulled from
// pool.Stats at scrape time so they always agree with the health endpoint.
// Everything is in-memory and starts at zero on every restart.
package metrics

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/pool"
)

// StatsFunc reports every enabled pool's snapshot by pool name.
type StatsFunc func() map[string]pool.Stats

// durationBuckets cover a worker's lifetime from a quick job to an hour.
var durationBuckets = []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600}

// Metrics owns the registry and implements pool.Metrics, app.Metrics and
// redisin.Metrics.
type Metrics struct {
	reg *prometheus.Registry

	spawns      *prometheus.CounterVec
	spawnErrors *prometheus.CounterVec
	exits       *prometheus.CounterVec
	kills       *prometheus.CounterVec
	duration    *prometheus.HistogramVec
	trips       *prometheus.CounterVec
	hints       *prometheus.CounterVec
	drops       *prometheus.CounterVec
	tasks       *prometheus.CounterVec

	redisConnected  prometheus.Gauge
	redisReconnects prometheus.Counter
}

// New builds a registry under cfg.Namespace with build_info set to version.
// Every per-pool series is pre-initialised for pools so zeros are visible
// before the first event; stats feeds the pool gauges (nil reports none).
// The default go and process collectors are registered too.
func New(cfg config.Metrics, version string, pools []string, stats StatsFunc) *Metrics {
	ns := cfg.Namespace
	if stats == nil {
		stats = func() map[string]pool.Stats { return nil }
	}
	m := &Metrics{
		reg: prometheus.NewRegistry(),
		spawns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "spawns_total", Help: "Workers started.",
		}, []string{"pool"}),
		spawnErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "spawn_errors_total", Help: "Spawn attempts that failed to start a process.",
		}, []string{"pool"}),
		exits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "exits_total",
			Help: "Workers reaped, by result (ok|failure|killed) and code (exit code, signal name, or ttl|shutdown when killed).",
		}, []string{"pool", "result", "code"}),
		kills: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "kills_total", Help: "Workers terminated by doorbell, by reason (ttl|shutdown).",
		}, []string{"pool", "reason"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Name: "process_duration_seconds", Help: "Worker lifetime from spawn to reap.",
			Buckets: durationBuckets,
		}, []string{"pool", "result"}),
		trips: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "cooldown_trips_total", Help: "Times the failure breaker opened.",
		}, []string{"pool"}),
		hints: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "hints_total", Help: "Hints that reached a pool, by source (redis|http|poke|worker).",
		}, []string{"source", "pool"}),
		drops: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "dropped_hints_total",
			Help: "Hints dropped, by reason (exit_cooldown|unknown_pool|disabled|bad_payload|shutdown); pool is empty when unknown.",
		}, []string{"source", "pool", "reason"}),
		tasks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Name: "tasks_processed_total", Help: "Tasks workers reported processing on exit.",
		}, []string{"pool"}),
		redisConnected: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Name: "redis_connected", Help: "1 while the redis subscription is confirmed.",
		}),
		redisReconnects: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Name: "redis_reconnects_total", Help: "Reconnect attempts after a lost redis session.",
		}),
	}
	buildInfo := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: ns, Name: "build_info", Help: "Always 1, carries the version label.",
	}, []string{"version"})
	buildInfo.WithLabelValues(version).Set(1)

	m.reg.MustRegister(
		buildInfo, m.spawns, m.spawnErrors, m.exits, m.kills, m.duration, m.trips, m.hints, m.drops, m.tasks,
		m.redisConnected, m.redisReconnects,
		newPoolCollector(ns, stats),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	for _, p := range pools {
		m.init(p)
	}
	return m
}

// init creates the zero-valued series of one pool.
func (m *Metrics) init(p string) {
	m.spawns.WithLabelValues(p)
	m.spawnErrors.WithLabelValues(p)
	m.trips.WithLabelValues(p)
	m.tasks.WithLabelValues(p)
	for _, reason := range []string{pool.KillTTL, pool.KillShutdown} {
		m.kills.WithLabelValues(p, reason)
	}
	for _, result := range []string{"ok", "failure", "killed"} {
		m.duration.WithLabelValues(p, result)
	}
	for _, source := range []string{hint.SourceRedis, hint.SourceHTTP, hint.SourcePoke, hint.SourceWorker} {
		m.hints.WithLabelValues(source, p)
		m.drops.WithLabelValues(source, p, "exit_cooldown")
	}
}

// Registry is the underlying registry, for tests and custom handlers.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the registry in the prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// Spawn implements pool.Metrics.
func (m *Metrics) Spawn(p string) { m.spawns.WithLabelValues(p).Inc() }

// SpawnError implements pool.Metrics.
func (m *Metrics) SpawnError(p string) { m.spawnErrors.WithLabelValues(p).Inc() }

// Exit implements pool.Metrics.
func (m *Metrics) Exit(p, result, code string, d time.Duration) {
	m.exits.WithLabelValues(p, result, code).Inc()
	m.duration.WithLabelValues(p, result).Observe(d.Seconds())
}

// Kill implements pool.Metrics.
func (m *Metrics) Kill(p, reason string) { m.kills.WithLabelValues(p, reason).Inc() }

// Trip implements pool.Metrics.
func (m *Metrics) Trip(p string) { m.trips.WithLabelValues(p).Inc() }

// Hint implements pool.Metrics.
func (m *Metrics) Hint(source, p string) { m.hints.WithLabelValues(source, p).Inc() }

// Drop implements pool.Metrics, app.Metrics and redisin.Metrics.
func (m *Metrics) Drop(source, p, reason string) { m.drops.WithLabelValues(source, p, reason).Inc() }

// Tasks implements pool.Metrics.
func (m *Metrics) Tasks(p string, n int) { m.tasks.WithLabelValues(p).Add(float64(n)) }

// RedisConnected implements redisin.Metrics.
func (m *Metrics) RedisConnected(up bool) {
	if up {
		m.redisConnected.Set(1)
	} else {
		m.redisConnected.Set(0)
	}
}

// RedisReconnect implements redisin.Metrics.
func (m *Metrics) RedisReconnect() { m.redisReconnects.Inc() }
