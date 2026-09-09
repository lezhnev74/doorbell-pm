//go:build e2e

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// doorbell is one running doorbell subprocess under test.
type doorbell struct {
	t          *testing.T
	cmd        *exec.Cmd
	baseURL    string
	metricsURL string // set when the config used METRICS_ADDR
	workerLog  string
	stderr     *lockedBuffer
	done       chan error
	stopOnce   sync.Once
	stopErr    error
}

// startDoorbell writes cfg (with WORKER_BIN, WORKER_LOG_PATH, REDIS_ADDR,
// HTTP_ADDR and METRICS_ADDR substituted), starts the binary and waits for
// /healthz.
func startDoorbell(t *testing.T, cfg string) *doorbell {
	t.Helper()
	dir := t.TempDir()
	httpAddr, err := freeAddr()
	if err != nil {
		t.Fatal(err)
	}
	metricsAddr, err := freeAddr()
	if err != nil {
		t.Fatal(err)
	}
	workerLog := filepath.Join(dir, "worker.log")
	cfg = strings.NewReplacer(
		"WORKER_BIN", workerBin,
		"WORKER_LOG_PATH", workerLog,
		"REDIS_ADDR", redisAddr,
		"HTTP_ADDR", httpAddr,
		"METRICS_ADDR", metricsAddr,
	).Replace(cfg)
	cfgPath := filepath.Join(dir, "doorbell.yaml")
	if err := os.WriteFile(cfgPath, []byte(cfg), 0o644); err != nil {
		t.Fatal(err)
	}

	d := &doorbell{
		t:          t,
		cmd:        exec.Command(doorbellBin, "-config", cfgPath),
		baseURL:    "http://" + httpAddr,
		metricsURL: "http://" + metricsAddr,
		workerLog:  workerLog,
		stderr:     &lockedBuffer{},
		done:       make(chan error, 1),
	}
	d.cmd.Stderr = d.stderr
	if err := d.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	go func() { d.done <- d.cmd.Wait() }()
	t.Cleanup(func() { _ = d.stop(syscall.SIGKILL, 5*time.Second) })

	d.waitFor("healthz", func() bool {
		_, err := d.health()
		return err == nil
	})
	return d
}

// freeAddr picks an unused loopback host:port.
func freeAddr() (string, error) {
	port, err := freePort()
	if err != nil {
		return "", err
	}
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(port)), nil
}

// stop signals doorbell and waits for it to exit; returns the wait error.
// Safe to call twice: the cleanup after an explicit stop is a no-op.
func (d *doorbell) stop(sig syscall.Signal, timeout time.Duration) error {
	d.stopOnce.Do(func() {
		select {
		case d.stopErr = <-d.done:
			return
		default:
		}
		_ = d.cmd.Process.Signal(sig)
		select {
		case d.stopErr = <-d.done:
		case <-time.After(timeout):
			_ = d.cmd.Process.Kill()
			d.stopErr = fmt.Errorf("doorbell did not exit within %s after %s", timeout, sig)
		}
	})
	return d.stopErr
}

// exited reports whether the process is gone, without blocking.
func (d *doorbell) exited() bool {
	select {
	case err := <-d.done:
		d.done <- err
		return true
	default:
		return false
	}
}

type poolHealth struct {
	Running      int     `json:"running"`
	Concurrency  int     `json:"concurrency"`
	LastHint     *string `json:"last_hint"`
	LastExit     *string `json:"last_exit"`
	LastExitCode *int    `json:"last_exit_code"`
	LastTasks    *int    `json:"last_tasks"`
	Breaker      struct {
		OpenUntil *string `json:"open_until"`
		Level     int     `json:"level"`
	} `json:"breaker"`
}

type healthResponse struct {
	Status string                `json:"status"`
	Pools  map[string]poolHealth `json:"pools"`
}

func (d *doorbell) health() (healthResponse, error) {
	var h healthResponse
	resp, err := http.Get(d.baseURL + "/healthz")
	if err != nil {
		return h, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return h, err
	}
	if resp.StatusCode != http.StatusOK {
		return h, fmt.Errorf("healthz status %d: %s", resp.StatusCode, body)
	}
	return h, json.Unmarshal(body, &h)
}

// running returns the running count of pool from /healthz, or -1 on error.
func (d *doorbell) running(pool string) int {
	h, err := d.health()
	if err != nil {
		return -1
	}
	return h.Pools[pool].Running
}

// hint POSTs body to /hint and returns the status code.
func (d *doorbell) hint(body string) int {
	d.t.Helper()
	resp, err := http.Post(d.baseURL+"/hint", "application/json", strings.NewReader(body))
	if err != nil {
		d.t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

// publish sends a hint through redis once doorbell's pattern subscription is
// visible to the server, so the message cannot be lost to a race with startup.
func (d *doorbell) publish(channel, payload string) {
	d.t.Helper()
	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()
	ctx := context.Background()
	d.waitFor("redis subscription", func() bool {
		n, err := client.PubSubNumPat(ctx).Result()
		return err == nil && n > 0
	})
	if err := client.Publish(ctx, channel, payload).Err(); err != nil {
		d.t.Fatal(err)
	}
}

// workerLines counts worker log lines starting with prefix ("started", "exit", "term", "int").
func (d *doorbell) workerLines(prefix string) int {
	b, err := os.ReadFile(d.workerLog)
	if err != nil {
		return 0
	}
	n := 0
	for _, l := range strings.Split(string(b), "\n") {
		if strings.HasPrefix(l, prefix+" ") {
			n++
		}
	}
	return n
}

// workerPIDs returns the pids of every worker that logged a `started` line.
func (d *doorbell) workerPIDs() []int {
	b, err := os.ReadFile(d.workerLog)
	if err != nil {
		return nil
	}
	var pids []int
	for _, l := range strings.Split(string(b), "\n") {
		f := strings.Fields(l)
		if len(f) < 2 || f[0] != "started" {
			continue
		}
		if pid, err := strconv.Atoi(f[1]); err == nil {
			pids = append(pids, pid)
		}
	}
	return pids
}

// alive reports whether a process with pid still exists.
func alive(pid int) bool {
	return syscall.Kill(pid, 0) == nil
}

// waitFor polls cond every 20ms for up to 10s and fails the test with
// doorbell's stderr when it never holds.
func (d *doorbell) waitFor(what string, cond func() bool) {
	d.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		if d.exited() {
			d.t.Fatalf("doorbell exited while waiting for %s\n--- stderr ---\n%s", what, d.stderr.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	d.t.Fatalf("timed out waiting for %s\n--- stderr ---\n%s", what, d.stderr.String())
}

// settle waits a short fixed time and asserts cond still holds, for "exactly
// N and no more" checks after the positive wait succeeded.
func (d *doorbell) settle(what string, cond func() bool) {
	d.t.Helper()
	time.Sleep(200 * time.Millisecond)
	if !cond() {
		d.t.Fatalf("%s did not hold after settling\n--- stderr ---\n%s", what, d.stderr.String())
	}
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (l *lockedBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *lockedBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}
