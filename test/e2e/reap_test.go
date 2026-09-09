//go:build e2e

package e2e

import (
	"net/http"
	"testing"
)

// reapConfig runs short-lived workers so exits are observable quickly.
// WORKER_EXIT is left to the pool so scenarios can pick the exit code.
const reapConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  _defaults:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 200ms}
    grace_shutdown: 2s
  ok:
    concurrency: 2
    command: [WORKER_BIN, ok]
  crash:
    exit_failure_threshold: 1
    exit_cooldown: 10s
    env: {WORKER_EXIT: "1"}
    command: [WORKER_BIN, crash]
  tolerant:
    exit_failure_threshold: 1
    exit_cooldown: 10s
    ok_exit_codes: [0, 3]
    env: {WORKER_EXIT: "3"}
    command: [WORKER_BIN, tolerant]
`

// Scenario 3: workers exiting are reaped (running drops to 0) and a new hint
// spawns again.
func TestWorkersAreReapedAndRespawned(t *testing.T) {
	d := startDoorbell(t, reapConfig)

	d.hint(`{"jobs:ok": 5}`)
	d.waitFor("2 ok workers", func() bool { return d.workerLines("started") == 2 })
	d.waitFor("2 ok exits", func() bool { return d.workerLines("exit") == 2 })
	d.waitFor("healthz running=0", func() bool { return d.running("ok") == 0 })

	d.hint(`{"jobs:ok": 1}`)
	d.waitFor("3rd ok worker", func() bool { return d.workerLines("started") == 3 })
	d.settle("exactly 3 workers", func() bool { return d.workerLines("started") == 3 })
	h, err := d.health()
	if err != nil {
		t.Fatal(err)
	}
	if p := h.Pools["ok"]; p.LastExit == nil || p.LastExitCode == nil || *p.LastExitCode != 0 {
		t.Fatalf("ok pool health after exits = %+v, want last_exit set and code 0", p)
	}
}

// Scenario 4: with exit_failure_threshold 1 a non-zero exit trips the breaker
// and the next hint inside exit_cooldown spawns nothing.
func TestFailureExitTripsBreaker(t *testing.T) {
	d := startDoorbell(t, reapConfig)

	d.hint(`{"jobs:crash": 1}`)
	d.waitFor("crash worker started", func() bool { return d.workerLines("started") == 1 })
	d.waitFor("crash worker exited", func() bool { return d.workerLines("exit") == 1 })
	d.waitFor("breaker open", func() bool {
		h, err := d.health()
		return err == nil && h.Pools["crash"].Breaker.OpenUntil != nil && h.Pools["crash"].Breaker.Level == 1
	})

	if code := d.hint(`{"jobs:crash": 1}`); code != http.StatusAccepted {
		t.Fatalf("hint status = %d, want 202", code)
	}
	d.settle("no spawn during cooldown", func() bool { return d.workerLines("started") == 1 })
	if n := d.running("crash"); n != 0 {
		t.Fatalf("crash running = %d, want 0", n)
	}
}

// Scenario 5: an exit code listed in ok_exit_codes is not a failure, so the
// pool keeps spawning.
func TestOkExitCodeDoesNotTrip(t *testing.T) {
	d := startDoorbell(t, reapConfig)

	d.hint(`{"jobs:tolerant": 1}`)
	d.waitFor("tolerant worker exited", func() bool { return d.workerLines("exit") == 1 })
	d.waitFor("healthz running=0", func() bool { return d.running("tolerant") == 0 })

	d.hint(`{"jobs:tolerant": 1}`)
	d.waitFor("2nd tolerant worker", func() bool { return d.workerLines("started") == 2 })
	h, err := d.health()
	if err != nil {
		t.Fatal(err)
	}
	if p := h.Pools["tolerant"]; p.Breaker.OpenUntil != nil || p.Breaker.Level != 0 {
		t.Fatalf("tolerant breaker = %+v, want closed", p.Breaker)
	}
	if p := h.Pools["tolerant"]; p.LastExitCode == nil || *p.LastExitCode != 3 {
		t.Fatalf("tolerant last_exit_code = %v, want 3", p.LastExitCode)
	}
}
