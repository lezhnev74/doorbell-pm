//go:build e2e

package e2e

import "testing"

// Workers sleep 10s in both configs, so an early `term` line or a start
// without a hint is doorbell's doing.
const ttlConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  ttl:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 10s}
    grace_shutdown: 2s
    ttl: 300ms
    command: [WORKER_BIN, ttl]
`

const pokeConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  poke:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 10s}
    grace_shutdown: 2s
    concurrency: 3
    poke: 500ms
    poke_count: 2
    command: [WORKER_BIN, poke]
`

// Scenario 6: a worker outliving ttl is terminated well before its own sleep.
func TestTTLTerminatesLongRunningWorker(t *testing.T) {
	d := startDoorbell(t, ttlConfig)

	d.hint(`{"jobs:ttl": 1}`)
	d.waitFor("ttl worker started", func() bool { return d.workerLines("started") == 1 })
	d.waitFor("ttl worker terminated", func() bool { return d.workerLines("term") == 1 })
	d.waitFor("healthz running=0", func() bool { return d.running("ttl") == 0 })
	if n := d.workerLines("exit"); n != 0 {
		t.Fatalf("worker exit lines = %d, want 0 (killed by ttl, not finished)", n)
	}
}

// Scenario 7: poke starts poke_count workers per tick without any hint and
// never exceeds concurrency.
func TestPokeStartsWorkersWithoutHint(t *testing.T) {
	d := startDoorbell(t, pokeConfig)

	d.waitFor("2 poked workers", func() bool { return d.workerLines("started") == 2 })
	d.waitFor("3rd poked worker", func() bool { return d.workerLines("started") == 3 })
	d.settle("capped at concurrency", func() bool { return d.workerLines("started") == 3 })
	d.waitFor("healthz running=3", func() bool { return d.running("poke") == 3 })
}
