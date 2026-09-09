package proc

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"syscall"
	"time"
)

// waitDelay bounds how long Done is held back after the child exited while a
// grandchild still holds its stdout/stderr open.
const waitDelay = 5 * time.Second

// ExecSpawner starts real processes with os/exec. Each child gets its own
// process group so a later group-wide kill cannot reach the parent.
type ExecSpawner struct{}

// NewExecSpawner returns a spawner for real processes.
func NewExecSpawner() *ExecSpawner { return &ExecSpawner{} }

// Spawn starts spec.Cmd. ctx bounds the start only: the process outlives it
// and is stopped through Process.Signal. The stream named by
// spec.ResultStream is scanned for Exit.Result; the other one, and both when
// nothing is selected, go to /dev/null.
func (s *ExecSpawner) Spawn(ctx context.Context, spec Spec) (Process, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(spec.Cmd) == 0 {
		return nil, errors.New("proc: empty command")
	}
	cmd := exec.Command(spec.Cmd[0], spec.Cmd[1:]...)
	cmd.Dir = spec.Dir
	cmd.Env = buildEnv(spec)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = waitDelay

	p := &execProcess{cmd: cmd, done: make(chan Exit, 1)}
	switch spec.ResultStream {
	case Stdout:
		p.result = &lastLine{}
		cmd.Stdout = p.result
	case Stderr:
		p.result = &lastLine{}
		cmd.Stderr = p.result
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("proc: start %q: %w", spec.Cmd[0], err)
	}
	go p.wait()
	return p, nil
}

// buildEnv returns the child environment: the parent's plus spec.Env when
// inheriting, otherwise spec.Env alone. Keys are sorted so the result is
// deterministic; a later duplicate wins in os/exec, so spec.Env overrides.
func buildEnv(spec Spec) []string {
	keys := make([]string, 0, len(spec.Env))
	for k := range spec.Env {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	env := make([]string, 0, len(keys))
	if spec.InheritEnv {
		env = append(env, os.Environ()...)
	}
	for _, k := range keys {
		env = append(env, k+"="+spec.Env[k])
	}
	return env
}

type execProcess struct {
	cmd    *exec.Cmd
	result *lastLine // nil when no stream is read
	done   chan Exit
}

func (p *execProcess) PID() int { return p.cmd.Process.Pid }

func (p *execProcess) Signal(sig os.Signal) error { return p.cmd.Process.Signal(sig) }

// KillGroup signals the group the child leads. The group outlives the
// leader while any member runs, so this still reaches orphans after the
// child itself exited.
func (p *execProcess) KillGroup() error {
	err := syscall.Kill(-p.PID(), syscall.SIGKILL)
	if errors.Is(err, syscall.ESRCH) {
		return os.ErrProcessDone
	}
	return err
}

func (p *execProcess) Done() <-chan Exit { return p.done }

// wait reaps the child. Wait returns only after the output copy finished or
// waitDelay expired, so the kept line is final by then.
func (p *execProcess) wait() {
	err := p.cmd.Wait()
	exit := exitFrom(p.cmd.ProcessState, err)
	if p.result != nil {
		exit.Result = p.result.Line()
	}
	p.done <- exit
	close(p.done)
}

// exitFrom maps the result of Wait onto Exit. A wait delay expiry still
// carries a valid ProcessState, so it is not reported as an error.
func exitFrom(state *os.ProcessState, err error) Exit {
	if waitFailed(err) {
		return Exit{Code: -1, Err: err}
	}
	if ws, ok := state.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		return Exit{Code: -1, Signal: ws.Signal()}
	}
	return Exit{Code: state.ExitCode()}
}

// waitFailed is true when Wait itself failed, as opposed to reporting a
// non-zero exit or a wait delay expiry.
func waitFailed(err error) bool {
	if err == nil || errors.Is(err, exec.ErrWaitDelay) {
		return false
	}
	var ee *exec.ExitError
	return !errors.As(err, &ee)
}
