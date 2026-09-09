//go:build e2e

package e2e

import (
	"net/http"
	"testing"
)

const baseConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  _defaults:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 30s}
    grace_shutdown: 2s
  encoding:
    concurrency: 3
    command: [WORKER_BIN, encoding]
  reports:
    command: [WORKER_BIN, reports]
`

// Scenario 1: a redis publish spawns exactly `concurrency` workers.
func TestRedisHintSpawnsConcurrencyWorkers(t *testing.T) {
	d := startDoorbell(t, baseConfig)

	d.publish("jobs:encoding", "100")
	d.waitFor("3 encoding workers", func() bool { return d.workerLines("started") == 3 })
	d.settle("exactly 3 workers", func() bool { return d.workerLines("started") == 3 })
	d.waitFor("healthz running=3", func() bool { return d.running("encoding") == 3 })
	if n := d.running("reports"); n != 0 {
		t.Fatalf("reports running = %d, want 0", n)
	}
}

// Scenario 2: an http hint spawns one worker in the named pool.
func TestHTTPHintSpawnsOneWorker(t *testing.T) {
	d := startDoorbell(t, baseConfig)

	if code := d.hint(`{"jobs:reports": 1}`); code != http.StatusAccepted {
		t.Fatalf("hint status = %d, want 202", code)
	}
	d.waitFor("1 reports worker", func() bool { return d.workerLines("started") == 1 })
	d.settle("exactly 1 worker", func() bool { return d.workerLines("started") == 1 })
	d.waitFor("healthz running=1", func() bool { return d.running("reports") == 1 })
	if n := d.running("encoding"); n != 0 {
		t.Fatalf("encoding running = %d, want 0", n)
	}

	if code := d.hint(`{"jobs:nope": 1}`); code != http.StatusNotFound {
		t.Fatalf("unknown pool status = %d, want 404", code)
	}
}
