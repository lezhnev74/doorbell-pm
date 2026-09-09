//go:build e2e

package e2e

import (
	"syscall"
	"testing"
	"time"
)

// Workers sleep 10s so any early exit is doorbell's doing. shutdown_timeout
// is larger than every grace so it never masks a stuck pool.
const shutdownConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  _defaults:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 10s}
    grace_shutdown: 2s
  polite:
    concurrency: 2
    command: [WORKER_BIN, polite]
  stubborn:
    grace_shutdown: 300ms
    env: {WORKER_IGNORE_TERM: "1"}
    command: [WORKER_BIN, stubborn]
  sigint:
    term_signal: SIGINT
    command: [WORKER_BIN, sigint]
`

// Scenario 8: SIGTERM to doorbell terminates cooperative workers and doorbell
// exits 0 within grace.
func TestShutdownTerminatesWorkersAndExitsZero(t *testing.T) {
	d := startDoorbell(t, shutdownConfig)

	d.hint(`{"jobs:polite": 2}`)
	d.waitFor("2 polite workers", func() bool { return d.workerLines("started") == 2 })

	start := time.Now()
	if err := d.stop(syscall.SIGTERM, 3*time.Second); err != nil {
		t.Fatalf("doorbell exit: %v\n--- stderr ---\n%s", err, d.stderr.String())
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("shutdown took %s, want under grace_shutdown 2s", took)
	}
	if n := d.workerLines("term"); n != 2 {
		t.Fatalf("worker term lines = %d, want 2", n)
	}
	if n := d.workerLines("exit"); n != 0 {
		t.Fatalf("worker exit lines = %d, want 0", n)
	}
}

// Scenario 9: a worker ignoring SIGTERM is killed after grace_shutdown and
// doorbell still exits cleanly.
func TestShutdownKillsWorkerIgnoringTerm(t *testing.T) {
	d := startDoorbell(t, shutdownConfig)

	d.hint(`{"jobs:stubborn": 1}`)
	d.waitFor("stubborn worker", func() bool { return d.workerLines("started") == 1 })
	pids := d.workerPIDs()
	if len(pids) != 1 || !alive(pids[0]) {
		t.Fatalf("worker pids = %v, want one live pid", pids)
	}

	if err := d.stop(syscall.SIGTERM, 3*time.Second); err != nil {
		t.Fatalf("doorbell exit: %v\n--- stderr ---\n%s", err, d.stderr.String())
	}
	if alive(pids[0]) {
		t.Fatalf("worker %d still alive after doorbell exited", pids[0])
	}
	if n := d.workerLines("term") + d.workerLines("exit"); n != 0 {
		t.Fatalf("worker logged %d term/exit lines, want 0 (it ignores TERM and was killed)", n)
	}
}

// Scenario 10: term_signal SIGINT reaches the worker as INT.
func TestShutdownUsesConfiguredTermSignal(t *testing.T) {
	d := startDoorbell(t, shutdownConfig)

	d.hint(`{"jobs:sigint": 1}`)
	d.waitFor("sigint worker", func() bool { return d.workerLines("started") == 1 })

	if err := d.stop(syscall.SIGTERM, 3*time.Second); err != nil {
		t.Fatalf("doorbell exit: %v\n--- stderr ---\n%s", err, d.stderr.String())
	}
	if n := d.workerLines("int"); n != 1 {
		t.Fatalf("worker int lines = %d, want 1", n)
	}
	if n := d.workerLines("term"); n != 0 {
		t.Fatalf("worker term lines = %d, want 0", n)
	}
}
