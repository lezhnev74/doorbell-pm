package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/test/worker/workertest"
)

var workerBin string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "doorbell-proc")
	if err != nil {
		panic(err)
	}
	workerBin, err = workertest.Build(dir)
	if err != nil {
		os.RemoveAll(dir)
		panic(err)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// spawn starts spec through an ExecSpawner reading stream for the result.
func spawn(t *testing.T, stream Stream, spec Spec) Process {
	t.Helper()
	spec.ResultStream = stream
	p, err := NewExecSpawner().Spawn(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

// result runs script through sh and returns the Exit.Result read from stream.
func result(t *testing.T, stream Stream, script string) string {
	t.Helper()
	return waitExit(t, spawn(t, stream, sh(script))).Result
}

func sh(script string) Spec {
	return Spec{Cmd: []string{"sh", "-c", script}, InheritEnv: true}
}

func waitExit(t *testing.T, p Process) Exit {
	t.Helper()
	select {
	case exit, ok := <-p.Done():
		if !ok {
			t.Fatal("Done closed without an Exit")
		}
		if _, ok := <-p.Done(); ok {
			t.Fatal("Done not closed after the Exit")
		}
		return exit
	case <-time.After(10 * time.Second):
		t.Fatal("process did not exit")
	}
	return Exit{}
}

func TestExitCodePropagates(t *testing.T) {
	p := spawn(t, Stdout, sh("exit 3"))
	if p.PID() <= 0 {
		t.Fatalf("pid = %d", p.PID())
	}
	if exit := waitExit(t, p); exit != (Exit{Code: 3}) {
		t.Fatalf("exit = %+v", exit)
	}
}

func TestZeroExit(t *testing.T) {
	p := spawn(t, Stdout, sh("true"))
	if exit := waitExit(t, p); exit != (Exit{}) {
		t.Fatalf("exit = %+v", exit)
	}
}

func TestStartErrorIsReturned(t *testing.T) {
	_, err := NewExecSpawner().Spawn(context.Background(), Spec{Cmd: []string{"/nonexistent/doorbell-worker"}})
	if err == nil {
		t.Fatal("Spawn succeeded for a missing binary")
	}
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want ErrNotExist", err)
	}
	if _, err := NewExecSpawner().Spawn(context.Background(), Spec{}); err == nil {
		t.Fatal("Spawn succeeded for an empty command")
	}
}

func TestCancelledContextDoesNotStart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewExecSpawner().Spawn(ctx, sh("true")); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestResultLastLineWins(t *testing.T) {
	if got := result(t, Stdout, "echo 1; echo 2; echo 3"); got != "3" {
		t.Fatalf("result = %q, want 3", got)
	}
}

func TestResultSkipsBlankLinesAndTrims(t *testing.T) {
	if got := result(t, Stdout, "printf ' 7 \\r\\n\\n   \\n\\r\\n'"); got != "7" {
		t.Fatalf("result = %q, want 7", got)
	}
}

func TestResultPartialLastLineKept(t *testing.T) {
	if got := result(t, Stdout, "echo 1; printf 42"); got != "42" {
		t.Fatalf("result = %q, want 42", got)
	}
}

func TestResultTooLongIsEmpty(t *testing.T) {
	long := strings.Repeat("9", MaxResult+1)
	if got := result(t, Stdout, "echo 5; echo "+long); got != "" {
		t.Fatalf("result = %q, want empty for a %d-byte line", got, MaxResult+1)
	}
	max := strings.Repeat("9", MaxResult)
	if got := result(t, Stdout, "echo "+max); got != max {
		t.Fatalf("result = %q, want the %d-byte line kept", got, MaxResult)
	}
}

func TestResultStreamSelection(t *testing.T) {
	const script = "echo out; echo err >&2"
	if got := result(t, Stdout, script); got != "out" {
		t.Fatalf("stdout result = %q, want out", got)
	}
	if got := result(t, Stderr, script); got != "err" {
		t.Fatalf("stderr result = %q, want err", got)
	}
	if got := result(t, None, script); got != "" {
		t.Fatalf("none result = %q, want empty", got)
	}
	if got := result(t, "", script); got != "" {
		t.Fatalf("zero-value stream result = %q, want empty", got)
	}
}

func TestEnvAndDirApplied(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DOORBELL_PARENT", "inherited")
	spec := sh("echo FOO=$FOO PARENT=$DOORBELL_PARENT")
	spec.Env = map[string]string{"FOO": "bar"}
	if got := waitExit(t, spawn(t, Stdout, spec)).Result; got != "FOO=bar PARENT=inherited" {
		t.Errorf("env not applied: %q", got)
	}
	spec = sh("pwd")
	spec.Dir = dir
	if got := waitExit(t, spawn(t, Stdout, spec)).Result; got != dir {
		t.Errorf("dir not applied: %q, want %q", got, dir)
	}
}

func TestNoInheritEnv(t *testing.T) {
	t.Setenv("DOORBELL_PARENT", "inherited")
	spec := Spec{Cmd: []string{"/bin/sh", "-c", "echo FOO=$FOO PARENT=$DOORBELL_PARENT"}, Env: map[string]string{"FOO": "bar"}}
	if got := waitExit(t, spawn(t, Stdout, spec)).Result; got != "FOO=bar PARENT=" {
		t.Errorf("environment leaked or missing: %q", got)
	}
}

func TestOwnProcessGroup(t *testing.T) {
	p := spawn(t, None, sh("sleep 10"))
	pgid, err := syscall.Getpgid(p.PID())
	if err != nil {
		t.Fatal(err)
	}
	if pgid != p.PID() {
		t.Fatalf("pgid = %d, want %d (own group)", pgid, p.PID())
	}
	if err := p.Signal(syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	if exit := waitExit(t, p); exit.Signal != syscall.SIGKILL || exit.Code != -1 {
		t.Fatalf("exit = %+v, want killed by SIGKILL", exit)
	}
}

func TestSigtermReachesWorker(t *testing.T) {
	logPath := filepath.Join(t.TempDir(), "worker.log")
	spec := Spec{
		Cmd:        []string{workerBin, "--queue=encoding"},
		Env:        map[string]string{"WORKER_LOG": logPath, "WORKER_SLEEP": "10s"},
		InheritEnv: true,
	}
	p := spawn(t, None, spec)
	waitForLine(t, logPath, "started")
	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if exit := waitExit(t, p); exit != (Exit{}) {
		t.Fatalf("exit = %+v, want clean exit on TERM", exit)
	}
	want := fmt.Sprintf("started %d --queue=encoding\nterm %d\n", p.PID(), p.PID())
	if got := readFile(t, logPath); got != want {
		t.Fatalf("worker log = %q, want %q", got, want)
	}
	if err := p.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Signal after exit = %v, want ErrProcessDone", err)
	}
}

func waitForLine(t *testing.T, path, prefix string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(readFile(t, path), prefix) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%q never contained %q", readFile(t, path), prefix)
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(b)
}
