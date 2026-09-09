package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/pool"
	"doorbell-pm/internal/proc/proctest"
)

func newMetrics(ns string, stats StatsFunc) *Metrics {
	return New(config.Metrics{Namespace: ns}, "1.2.3", []string{"enc"}, stats)
}

// expect compares the named families of m against the text form of want.
func expect(t *testing.T, m *Metrics, want string, names ...string) {
	t.Helper()
	if err := testutil.GatherAndCompare(m.reg, strings.NewReader(want), names...); err != nil {
		t.Fatal(err)
	}
}

func TestBuildInfoAndNamespace(t *testing.T) {
	expect(t, newMetrics("bell", nil), `
# HELP bell_build_info Always 1, carries the version label.
# TYPE bell_build_info gauge
bell_build_info{version="1.2.3"} 1
`, "bell_build_info")
}

func TestSeriesPreInitialised(t *testing.T) {
	expect(t, newMetrics("doorbell", nil), `
# HELP doorbell_spawns_total Workers started.
# TYPE doorbell_spawns_total counter
doorbell_spawns_total{pool="enc"} 0
# HELP doorbell_spawn_errors_total Spawn attempts that failed to start a process.
# TYPE doorbell_spawn_errors_total counter
doorbell_spawn_errors_total{pool="enc"} 0
# HELP doorbell_cooldown_trips_total Times the failure breaker opened.
# TYPE doorbell_cooldown_trips_total counter
doorbell_cooldown_trips_total{pool="enc"} 0
# HELP doorbell_kills_total Workers terminated by doorbell, by reason (ttl|shutdown).
# TYPE doorbell_kills_total counter
doorbell_kills_total{pool="enc",reason="shutdown"} 0
doorbell_kills_total{pool="enc",reason="ttl"} 0
# HELP doorbell_hints_total Hints that reached a pool, by source (redis|http|poke|worker).
# TYPE doorbell_hints_total counter
doorbell_hints_total{pool="enc",source="http"} 0
doorbell_hints_total{pool="enc",source="poke"} 0
doorbell_hints_total{pool="enc",source="redis"} 0
doorbell_hints_total{pool="enc",source="worker"} 0
# HELP doorbell_dropped_hints_total Hints dropped, by reason (exit_cooldown|unknown_pool|disabled|bad_payload|shutdown); pool is empty when unknown.
# TYPE doorbell_dropped_hints_total counter
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="http"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="poke"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="redis"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="worker"} 0
# HELP doorbell_tasks_processed_total Tasks workers reported processing on exit.
# TYPE doorbell_tasks_processed_total counter
doorbell_tasks_processed_total{pool="enc"} 0
`, "doorbell_spawns_total", "doorbell_spawn_errors_total", "doorbell_cooldown_trips_total",
		"doorbell_kills_total", "doorbell_hints_total", "doorbell_dropped_hints_total", "doorbell_tasks_processed_total")

	n := testutil.CollectAndCount(newMetrics("doorbell", nil).duration, "doorbell_process_duration_seconds")
	if n != 3 {
		t.Fatalf("duration series = %d, want 3 (one per result)", n)
	}
}

func TestSpawnAndExits(t *testing.T) {
	m := newMetrics("doorbell", nil)
	m.Spawn("enc")
	m.Spawn("enc")
	m.SpawnError("enc")
	m.Exit("enc", "ok", "0", 200*time.Millisecond)
	m.Exit("enc", "failure", "1", 3*time.Second)
	m.Exit("enc", "failure", "SIGKILL", time.Second)
	m.Exit("enc", "killed", "ttl", 61*time.Second)
	m.Kill("enc", "ttl")
	expect(t, m, `
# HELP doorbell_spawns_total Workers started.
# TYPE doorbell_spawns_total counter
doorbell_spawns_total{pool="enc"} 2
# HELP doorbell_spawn_errors_total Spawn attempts that failed to start a process.
# TYPE doorbell_spawn_errors_total counter
doorbell_spawn_errors_total{pool="enc"} 1
# HELP doorbell_exits_total Workers reaped, by result (ok|failure|killed) and code (exit code, signal name, or ttl|shutdown when killed).
# TYPE doorbell_exits_total counter
doorbell_exits_total{code="0",pool="enc",result="ok"} 1
doorbell_exits_total{code="1",pool="enc",result="failure"} 1
doorbell_exits_total{code="SIGKILL",pool="enc",result="failure"} 1
doorbell_exits_total{code="ttl",pool="enc",result="killed"} 1
# HELP doorbell_kills_total Workers terminated by doorbell, by reason (ttl|shutdown).
# TYPE doorbell_kills_total counter
doorbell_kills_total{pool="enc",reason="shutdown"} 0
doorbell_kills_total{pool="enc",reason="ttl"} 1
`, "doorbell_spawns_total", "doorbell_spawn_errors_total", "doorbell_exits_total", "doorbell_kills_total")

	for _, tc := range []struct {
		result string
		count  uint64
		sum    float64
	}{{"ok", 1, 0.2}, {"failure", 2, 4}, {"killed", 1, 61}} {
		count, sum := histogram(t, m, "enc", tc.result)
		if count != tc.count || sum != tc.sum {
			t.Fatalf("duration[%s] = %d obs, sum %v; want %d, %v", tc.result, count, sum, tc.count, tc.sum)
		}
	}
}

// histogram returns the sample count and sum of process_duration_seconds
// for one pool and result.
func histogram(t *testing.T, m *Metrics, pool, result string) (uint64, float64) {
	t.Helper()
	families, err := m.reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, mf := range families {
		if mf.GetName() != "doorbell_process_duration_seconds" {
			continue
		}
		for _, mt := range mf.GetMetric() {
			labels := map[string]string{}
			for _, l := range mt.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["pool"] == pool && labels["result"] == result {
				return mt.GetHistogram().GetSampleCount(), mt.GetHistogram().GetSampleSum()
			}
		}
	}
	t.Fatalf("no histogram for %s/%s", pool, result)
	return 0, 0
}

func TestHintsDropsAndTrips(t *testing.T) {
	m := newMetrics("doorbell", nil)
	m.Hint("redis", "enc")
	m.Hint("http", "enc")
	m.Hint("http", "enc")
	m.Hint("worker", "enc")
	m.Trip("enc")
	m.Tasks("enc", 3)
	m.Tasks("enc", 2)
	for _, d := range [][3]string{
		{"redis", "enc", "exit_cooldown"}, {"http", "", "unknown_pool"}, {"http", "old", "disabled"},
		{"redis", "", "bad_payload"}, {"poke", "enc", "shutdown"},
	} {
		m.Drop(d[0], d[1], d[2])
	}
	expect(t, m, `
# HELP doorbell_hints_total Hints that reached a pool, by source (redis|http|poke|worker).
# TYPE doorbell_hints_total counter
doorbell_hints_total{pool="enc",source="http"} 2
doorbell_hints_total{pool="enc",source="poke"} 0
doorbell_hints_total{pool="enc",source="redis"} 1
doorbell_hints_total{pool="enc",source="worker"} 1
# HELP doorbell_cooldown_trips_total Times the failure breaker opened.
# TYPE doorbell_cooldown_trips_total counter
doorbell_cooldown_trips_total{pool="enc"} 1
# HELP doorbell_dropped_hints_total Hints dropped, by reason (exit_cooldown|unknown_pool|disabled|bad_payload|shutdown); pool is empty when unknown.
# TYPE doorbell_dropped_hints_total counter
doorbell_dropped_hints_total{pool="",reason="bad_payload",source="redis"} 1
doorbell_dropped_hints_total{pool="",reason="unknown_pool",source="http"} 1
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="http"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="poke"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="redis"} 1
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="worker"} 0
doorbell_dropped_hints_total{pool="enc",reason="shutdown",source="poke"} 1
doorbell_dropped_hints_total{pool="old",reason="disabled",source="http"} 1
# HELP doorbell_tasks_processed_total Tasks workers reported processing on exit.
# TYPE doorbell_tasks_processed_total counter
doorbell_tasks_processed_total{pool="enc"} 5
`, "doorbell_hints_total", "doorbell_cooldown_trips_total", "doorbell_dropped_hints_total", "doorbell_tasks_processed_total")
}

func TestRedis(t *testing.T) {
	m := newMetrics("doorbell", nil)
	expect(t, m, `
# HELP doorbell_redis_connected 1 while the redis subscription is confirmed.
# TYPE doorbell_redis_connected gauge
doorbell_redis_connected 0
# HELP doorbell_redis_reconnects_total Reconnect attempts after a lost redis session.
# TYPE doorbell_redis_reconnects_total counter
doorbell_redis_reconnects_total 0
`, "doorbell_redis_connected", "doorbell_redis_reconnects_total")

	m.RedisConnected(true)
	m.RedisReconnect()
	m.RedisReconnect()
	expect(t, m, `
# HELP doorbell_redis_connected 1 while the redis subscription is confirmed.
# TYPE doorbell_redis_connected gauge
doorbell_redis_connected 1
# HELP doorbell_redis_reconnects_total Reconnect attempts after a lost redis session.
# TYPE doorbell_redis_reconnects_total counter
doorbell_redis_reconnects_total 2
`, "doorbell_redis_connected", "doorbell_redis_reconnects_total")
	m.RedisConnected(false)
	if v := testutil.ToFloat64(m.redisConnected); v != 0 {
		t.Fatalf("redis_connected = %v, want 0", v)
	}
}

func TestPoolGaugesFromStats(t *testing.T) {
	stats := map[string]pool.Stats{
		"enc": {Running: 2, Concurrency: 4, LastHint: time.Unix(100, 500_000_000), LastExit: time.Unix(90, 0),
			BreakerOpenUntil: time.Unix(200, 0), BreakerLevel: 3},
		"rep": {Concurrency: 1},
	}
	m := newMetrics("doorbell", func() map[string]pool.Stats { return stats })
	expect(t, m, `
# HELP doorbell_running Workers alive right now.
# TYPE doorbell_running gauge
doorbell_running{pool="enc"} 2
doorbell_running{pool="rep"} 0
# HELP doorbell_concurrency Configured maximum workers.
# TYPE doorbell_concurrency gauge
doorbell_concurrency{pool="enc"} 4
doorbell_concurrency{pool="rep"} 1
# HELP doorbell_last_hint_timestamp Unix time of the last hint, 0 if none yet.
# TYPE doorbell_last_hint_timestamp gauge
doorbell_last_hint_timestamp{pool="enc"} 100.5
doorbell_last_hint_timestamp{pool="rep"} 0
# HELP doorbell_last_exit_timestamp Unix time of the last reaped exit, 0 if none yet.
# TYPE doorbell_last_exit_timestamp gauge
doorbell_last_exit_timestamp{pool="enc"} 90
doorbell_last_exit_timestamp{pool="rep"} 0
# HELP doorbell_cooldown_active 1 while the failure breaker is open.
# TYPE doorbell_cooldown_active gauge
doorbell_cooldown_active{pool="enc"} 1
doorbell_cooldown_active{pool="rep"} 0
# HELP doorbell_cooldown_level Consecutive breaker trips, resets after a quiet period.
# TYPE doorbell_cooldown_level gauge
doorbell_cooldown_level{pool="enc"} 3
doorbell_cooldown_level{pool="rep"} 0
# HELP doorbell_cooldown_open_until_timestamp Unix time the breaker closes, 0 when closed.
# TYPE doorbell_cooldown_open_until_timestamp gauge
doorbell_cooldown_open_until_timestamp{pool="enc"} 200
doorbell_cooldown_open_until_timestamp{pool="rep"} 0
`, "doorbell_running", "doorbell_concurrency", "doorbell_last_hint_timestamp", "doorbell_last_exit_timestamp",
		"doorbell_cooldown_active", "doorbell_cooldown_level", "doorbell_cooldown_open_until_timestamp")
}

// TestPoolFeedsMetrics drives a real pool with the fake spawner and checks
// the events land with the right labels: spawn, ok exit, failure exit that
// trips, spawn error, cooldown drop, ttl kill.
func TestPoolFeedsMetrics(t *testing.T) {
	spawner := &proctest.FakeSpawner{}
	clk := clock.NewFake(time.Unix(0, 0))
	cfg := config.PoolConfig{
		Name: "enc", Command: []string{"w"}, Concurrency: 2, OkExitCodes: []int{0},
		ExitFailureThreshold: 1, ExitFailureWindow: 10 * time.Second, ExitCooldown: 5 * time.Second,
		ExitCooldownMax: time.Minute, ExitCooldownMultiplier: 2, TTL: 30 * time.Second,
		GraceShutdown: time.Second, TermSignal: syscall.SIGTERM,
	}
	m := newMetrics("doorbell", nil)
	p := pool.New(cfg, spawner, clk, nil, m)
	ctx := context.Background()

	if n := p.Hint(ctx, "redis", 2); n != 2 {
		t.Fatalf("spawned %d, want 2", n)
	}
	clk.Advance(2 * time.Second)
	spawner.Procs()[0].FinishResult(0, "4") // ok with tasks: worker hint refills the slot
	waitProcs(t, spawner, 3)
	spawner.Procs()[1].Finish(1) // trips at threshold 1
	waitRunning(t, p, 1)
	spawner.Procs()[2].FinishResult(1, "2") // failure while open: tasks counted, no respawn
	waitRunning(t, p, 0)
	waitRunning(t, p, 0)
	if n := p.Hint(ctx, "http", 1); n != 0 {
		t.Fatalf("spawned %d during cooldown, want 0", n)
	}
	clk.Advance(5 * time.Second) // t=7: first cooldown over
	spawner.Err = context.Canceled
	p.Hint(ctx, "poke", 1) // spawn error: second trip, 10s cooldown
	spawner.Err = nil
	clk.Advance(10 * time.Second) // t=17: second cooldown over
	if n := p.Hint(ctx, "poke", 1); n != 1 {
		t.Fatalf("spawned %d after cooldown, want 1", n)
	}
	clk.Advance(30 * time.Second) // ttl
	waitRunning(t, p, 0)

	expect(t, m, `
# HELP doorbell_spawns_total Workers started.
# TYPE doorbell_spawns_total counter
doorbell_spawns_total{pool="enc"} 4
# HELP doorbell_spawn_errors_total Spawn attempts that failed to start a process.
# TYPE doorbell_spawn_errors_total counter
doorbell_spawn_errors_total{pool="enc"} 1
# HELP doorbell_exits_total Workers reaped, by result (ok|failure|killed) and code (exit code, signal name, or ttl|shutdown when killed).
# TYPE doorbell_exits_total counter
doorbell_exits_total{code="0",pool="enc",result="ok"} 1
doorbell_exits_total{code="1",pool="enc",result="failure"} 2
doorbell_exits_total{code="ttl",pool="enc",result="killed"} 1
# HELP doorbell_kills_total Workers terminated by doorbell, by reason (ttl|shutdown).
# TYPE doorbell_kills_total counter
doorbell_kills_total{pool="enc",reason="shutdown"} 0
doorbell_kills_total{pool="enc",reason="ttl"} 1
# HELP doorbell_cooldown_trips_total Times the failure breaker opened.
# TYPE doorbell_cooldown_trips_total counter
doorbell_cooldown_trips_total{pool="enc"} 2
# HELP doorbell_hints_total Hints that reached a pool, by source (redis|http|poke|worker).
# TYPE doorbell_hints_total counter
doorbell_hints_total{pool="enc",source="http"} 1
doorbell_hints_total{pool="enc",source="poke"} 2
doorbell_hints_total{pool="enc",source="redis"} 1
doorbell_hints_total{pool="enc",source="worker"} 1
# HELP doorbell_dropped_hints_total Hints dropped, by reason (exit_cooldown|unknown_pool|disabled|bad_payload|shutdown); pool is empty when unknown.
# TYPE doorbell_dropped_hints_total counter
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="http"} 1
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="poke"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="redis"} 0
doorbell_dropped_hints_total{pool="enc",reason="exit_cooldown",source="worker"} 0
# HELP doorbell_tasks_processed_total Tasks workers reported processing on exit.
# TYPE doorbell_tasks_processed_total counter
doorbell_tasks_processed_total{pool="enc"} 6
`, "doorbell_spawns_total", "doorbell_spawn_errors_total", "doorbell_exits_total", "doorbell_kills_total",
		"doorbell_cooldown_trips_total", "doorbell_hints_total", "doorbell_dropped_hints_total",
		"doorbell_tasks_processed_total")

	for _, tc := range []struct {
		result string
		count  uint64
		sum    float64
	}{{"ok", 1, 2}, {"failure", 2, 2}, {"killed", 1, 30}} {
		if count, sum := histogram(t, m, "enc", tc.result); count != tc.count || sum != tc.sum {
			t.Fatalf("duration[%s] = %d obs, sum %v; want %d, %v", tc.result, count, sum, tc.count, tc.sum)
		}
	}
}

func waitProcs(t *testing.T, s *proctest.FakeSpawner, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for len(s.Procs()) != want {
		if time.Now().After(deadline) {
			t.Fatalf("procs = %d, want %d", len(s.Procs()), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitRunning(t *testing.T, p *pool.Pool, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.Running() != want {
		if time.Now().After(deadline) {
			t.Fatalf("running = %d, want %d", p.Running(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestHandlerServesText(t *testing.T) {
	rec := httptest.NewRecorder()
	newMetrics("doorbell", nil).Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{"doorbell_build_info", "go_goroutines", "process_cpu_seconds_total"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body lacks %s:\n%s", want, body)
		}
	}
}
