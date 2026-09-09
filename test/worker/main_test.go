package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// streams captures what the worker wrote to its stdout and stderr.
type streams struct {
	stdout, stderr bytes.Buffer
}

func startWorker(t *testing.T, env map[string]string, args ...string) (*exec.Cmd, string, *streams) {
	t.Helper()
	logPath := filepath.Join(t.TempDir(), "worker.log")
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestHelperProcess$", "--"}, args...)...)
	cmd.Env = append(os.Environ(), "WORKER_HELPER=1", "WORKER_LOG="+logPath)
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	out := &streams{}
	cmd.Stdout, cmd.Stderr = &out.stdout, &out.stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	return cmd, logPath, out
}

func waitFor(t *testing.T, logPath, prefix string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(readLog(t, logPath), prefix) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("log %q never contained %q", readLog(t, logPath), prefix)
}

func readLog(t *testing.T, logPath string) string {
	t.Helper()
	b, err := os.ReadFile(logPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(b)
}

func exitCode(err error) int {
	var ee *exec.ExitError
	if errors.As(err, &ee) {
		return ee.ExitCode()
	}
	if err != nil {
		return -1
	}
	return 0
}

// TestHelperProcess is re-executed by the tests above as the worker itself.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("WORKER_HELPER") != "1" {
		return
	}
	// Strip everything up to and including "--" so os.Args mirrors a real invocation.
	for i, a := range os.Args {
		if a == "--" {
			os.Args = append(os.Args[:1], os.Args[i+1:]...)
			break
		}
	}
	os.Exit(run())
}

func TestWorkerRunsAndExits(t *testing.T) {
	cmd, logPath, out := startWorker(t, map[string]string{"WORKER_SLEEP": "50ms", "WORKER_EXIT": "3"}, "--queue=encoding", "--drain")
	err := cmd.Wait()
	if got := exitCode(err); got != 3 {
		t.Fatalf("exit code = %d, want 3", got)
	}
	want := "started " + pidOf(cmd) + " --queue=encoding --drain\nexit " + pidOf(cmd) + "\n"
	if got := readLog(t, logPath); got != want {
		t.Fatalf("log = %q, want %q", got, want)
	}
	if out.stdout.Len() != 0 || out.stderr.Len() != 0 {
		t.Fatalf("stdout = %q, stderr = %q, want both empty without WORKER_RESULT", out.stdout.String(), out.stderr.String())
	}
}

func TestWorkerSigterm(t *testing.T) {
	cmd, logPath, _ := startWorker(t, map[string]string{"WORKER_SLEEP": "10s"})
	waitFor(t, logPath, "started")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := readLog(t, logPath); !strings.HasSuffix(got, "term "+pidOf(cmd)+"\n") {
		t.Fatalf("log = %q, want term line", got)
	}
}

func TestWorkerSigint(t *testing.T) {
	cmd, logPath, _ := startWorker(t, map[string]string{"WORKER_SLEEP": "10s"})
	waitFor(t, logPath, "started")
	if err := cmd.Process.Signal(syscall.SIGINT); err != nil {
		t.Fatal(err)
	}
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := readLog(t, logPath); !strings.HasSuffix(got, "int "+pidOf(cmd)+"\n") {
		t.Fatalf("log = %q, want int line", got)
	}
}

func TestWorkerIgnoresTerm(t *testing.T) {
	cmd, logPath, _ := startWorker(t, map[string]string{"WORKER_SLEEP": "300ms", "WORKER_IGNORE_TERM": "1"})
	waitFor(t, logPath, "started")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := readLog(t, logPath); !strings.HasSuffix(got, "exit "+pidOf(cmd)+"\n") {
		t.Fatalf("log = %q, want exit line (term ignored)", got)
	}
}

func TestWorkerPrintsResultOnStdout(t *testing.T) {
	cmd, _, out := startWorker(t, map[string]string{"WORKER_SLEEP": "50ms", "WORKER_RESULT": "5"})
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := out.stdout.String(); got != "5\n" {
		t.Fatalf("stdout = %q, want %q", got, "5\n")
	}
	if got := out.stderr.String(); got != "" {
		t.Fatalf("stderr = %q, want empty", got)
	}
}

func TestWorkerPrintsResultOnStderr(t *testing.T) {
	cmd, _, out := startWorker(t, map[string]string{"WORKER_SLEEP": "50ms", "WORKER_RESULT": "7", "WORKER_RESULT_STREAM": "stderr"})
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := out.stderr.String(); got != "7\n" {
		t.Fatalf("stderr = %q, want %q", got, "7\n")
	}
	if got := out.stdout.String(); got != "" {
		t.Fatalf("stdout = %q, want empty", got)
	}
}

func TestWorkerPrintsResultOnSignal(t *testing.T) {
	cmd, logPath, out := startWorker(t, map[string]string{"WORKER_SLEEP": "10s", "WORKER_RESULT": "1"})
	waitFor(t, logPath, "started")
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if got := exitCode(cmd.Wait()); got != 0 {
		t.Fatalf("exit code = %d, want 0", got)
	}
	if got := out.stdout.String(); got != "1\n" {
		t.Fatalf("stdout = %q, want %q", got, "1\n")
	}
}

func pidOf(cmd *exec.Cmd) string { return strconv.Itoa(cmd.Process.Pid) }
