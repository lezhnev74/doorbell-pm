package proc

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

const testGrace = 200 * time.Millisecond

// spawnWorker starts the test worker with a 10s sleep and waits for it to
// report started. Extra env is added on top of WORKER_LOG.
func spawnWorker(t *testing.T, env map[string]string) (Process, string) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "worker.log")
	spec := Spec{
		Cmd:        []string{workerBin},
		Env:        map[string]string{"WORKER_LOG": logPath, "WORKER_SLEEP": "10s"},
		InheritEnv: true,
	}
	for k, v := range env {
		spec.Env[k] = v
	}
	p := spawn(t, None, spec)
	waitForLine(t, logPath, "started")
	return p, logPath
}

func TestTerminateCooperativeExitsOnTerm(t *testing.T) {
	p, logPath := spawnWorker(t, nil)
	start := time.Now()
	exit := Terminate(p, nil, 5*time.Second)
	if exit != (Exit{}) {
		t.Fatalf("exit = %+v, want clean exit", exit)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("Terminate took %v, want well under grace", took)
	}
	want := fmt.Sprintf("started %d \nterm %d\n", p.PID(), p.PID())
	if got := readFile(t, logPath); got != want {
		t.Fatalf("worker log = %q, want %q", got, want)
	}
}

func TestTerminateKillsAfterGrace(t *testing.T) {
	p, logPath := spawnWorker(t, map[string]string{"WORKER_IGNORE_TERM": "1"})
	start := time.Now()
	exit := Terminate(p, syscall.SIGTERM, testGrace)
	took := time.Since(start)
	if exit.Signal != syscall.SIGKILL || exit.Code != -1 {
		t.Fatalf("exit = %+v, want killed by SIGKILL", exit)
	}
	if took < testGrace {
		t.Fatalf("killed after %v, before grace %v elapsed", took, testGrace)
	}
	want := fmt.Sprintf("started %d \n", p.PID())
	if got := readFile(t, logPath); got != want {
		t.Fatalf("worker log = %q, want only the start line", got)
	}
}

func TestTerminateWithSigint(t *testing.T) {
	p, logPath := spawnWorker(t, nil)
	if exit := Terminate(p, syscall.SIGINT, 5*time.Second); exit != (Exit{}) {
		t.Fatalf("exit = %+v, want clean exit", exit)
	}
	want := fmt.Sprintf("started %d \nint %d\n", p.PID(), p.PID())
	if got := readFile(t, logPath); got != want {
		t.Fatalf("worker log = %q, want %q", got, want)
	}
}

func TestTerminateAlreadyExited(t *testing.T) {
	p := spawn(t, None, sh("true"))
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := p.Signal(syscall.Signal(0)); errors.Is(err, os.ErrProcessDone) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if exit := Terminate(p, nil, testGrace); exit != (Exit{}) {
		t.Fatalf("exit = %+v, want the recorded clean exit", exit)
	}
}

func TestKillGroupReachesGrandchild(t *testing.T) {
	// The shell ignores TERM and parks on a background sleep whose pid it
	// records first, so only a group-wide kill can end the sleep.
	pidFile := filepath.Join(t.TempDir(), "sleep.pid")
	spec := sh(`trap '' TERM; sleep 30 & echo $! > "$PIDFILE"; wait`)
	spec.Env = map[string]string{"PIDFILE": pidFile}
	p := spawn(t, None, spec)
	waitForLine(t, pidFile, "\n")
	sleepPID, err := strconv.Atoi(strings.TrimSpace(readFile(t, pidFile)))
	if err != nil {
		t.Fatal(err)
	}
	if exit := Terminate(p, nil, testGrace); exit.Signal != syscall.SIGKILL {
		t.Fatalf("exit = %+v, want killed by SIGKILL", exit)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(sleepPID, 0); errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("sleep %d survived the group kill", sleepPID)
}
