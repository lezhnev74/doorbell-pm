//go:build e2e

package e2e

import (
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Every worker prints "1" as its last line and exits 0, so on a pool that
// reads that stream doorbell hints itself for one more worker after each
// exit. concurrency 1 keeps the chain to a single worker at a time.
const resultConfig = `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
shutdown_timeout: 5s
pools:
  _defaults:
    env: {WORKER_LOG: WORKER_LOG_PATH, WORKER_SLEEP: 100ms, WORKER_RESULT: "1"}
    grace_shutdown: 2s
    concurrency: 1
  chain:
    command: [WORKER_BIN, chain]
  silent:
    result_stream: none
    command: [WORKER_BIN, silent]
  onstderr:
    result_stream: stderr
    env: {WORKER_RESULT_STREAM: stderr}
    command: [WORKER_BIN, onstderr]
  mismatch:
    env: {WORKER_RESULT_STREAM: stderr}
    command: [WORKER_BIN, mismatch]
`

// Scenario 11: one hint keeps a pool chaining as long as workers report work.
func TestWorkerResultChainsWorkers(t *testing.T) {
	d := startDoorbell(t, resultConfig)

	d.hint(`{"jobs:chain": 1}`)
	d.waitFor("3 chained starts", func() bool { return d.poolLines("started", "chain") >= 3 })
	d.waitFor("healthz last_tasks=1", func() bool {
		h, err := d.health()
		return err == nil && h.Pools["chain"].LastTasks != nil && *h.Pools["chain"].LastTasks == 1
	})
	if !d.metricsHas(d.baseURL, `doorbell_hints_total{pool="chain",source="worker"}`) {
		t.Fatalf("metrics lack worker hints:\n%s", d.metrics(d.baseURL))
	}
	d.stopChain()
}

// Scenario 12: result_stream none ignores the count, so the pool starts once.
func TestResultStreamNoneDoesNotChain(t *testing.T) {
	d := startDoorbell(t, resultConfig)

	d.hint(`{"jobs:silent": 1}`)
	d.waitFor("silent exit", func() bool { return d.poolExits("silent") == 1 })
	d.settle("exactly 1 silent start", func() bool { return d.poolLines("started", "silent") == 1 })
	h, err := d.health()
	if err != nil {
		t.Fatal(err)
	}
	if p := h.Pools["silent"]; p.LastTasks != nil {
		t.Fatalf("silent last_tasks = %d, want null", *p.LastTasks)
	}
}

// Scenario 13: the count is read from the configured stream only: a worker
// printing on stderr chains a stderr pool and not a stdout one.
func TestResultStreamSelectsChildStream(t *testing.T) {
	d := startDoorbell(t, resultConfig)

	d.hint(`{"jobs:onstderr": 1, "jobs:mismatch": 1}`)
	d.waitFor("3 onstderr starts", func() bool { return d.poolLines("started", "onstderr") >= 3 })
	d.waitFor("mismatch exit", func() bool { return d.poolExits("mismatch") == 1 })
	d.settle("exactly 1 mismatch start", func() bool { return d.poolLines("started", "mismatch") == 1 })
	d.stopChain()
}

// poolLines counts worker log lines with prefix whose args name pool. Only
// `started` carries args, so exits go through poolExits.
func (d *doorbell) poolLines(prefix, pool string) int {
	b, err := os.ReadFile(d.workerLog)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) >= 3 && f[0] == prefix && f[2] == pool {
			n++
		}
	}
	return n
}

// poolExits counts `exit` lines whose pid belongs to a worker started in pool.
func (d *doorbell) poolExits(pool string) int {
	b, err := os.ReadFile(d.workerLog)
	if err != nil {
		return 0
	}
	pids := map[string]bool{}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		switch {
		case len(f) >= 3 && f[0] == "started" && f[2] == pool:
			pids[f[1]] = true
		case len(f) == 2 && f[0] == "exit" && pids[f[1]]:
			n++
		}
	}
	return n
}

// stopChain shuts doorbell down so a chaining pool stops respawning.
func (d *doorbell) stopChain() {
	d.t.Helper()
	if err := d.stop(syscall.SIGTERM, 3*time.Second); err != nil {
		d.t.Fatalf("doorbell exit: %v\n--- stderr ---\n%s", err, d.stderr.String())
	}
}
