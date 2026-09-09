// Package proctest provides an in-memory proc.Spawner for unit tests.
package proctest

import (
	"context"
	"os"
	"sync"
	"syscall"

	"doorbell-pm/internal/proc"
)

// FakeSpawner records every Spawn call and hands out FakeProcess values the
// test finishes by hand. Zero value is ready to use.
type FakeSpawner struct {
	mu    sync.Mutex
	next  int
	procs []*FakeProcess

	// Err, when set, is returned by Spawn instead of starting a process.
	Err error
}

// Spawn records spec and returns a running FakeProcess with a fresh pid.
func (s *FakeSpawner) Spawn(_ context.Context, spec proc.Spec) (proc.Process, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	s.next++
	p := &FakeProcess{pid: s.next, spec: spec, done: make(chan proc.Exit, 1)}
	s.procs = append(s.procs, p)
	return p, nil
}

// Procs returns every process spawned so far, in order.
func (s *FakeSpawner) Procs() []*FakeProcess {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*FakeProcess(nil), s.procs...)
}

// Specs returns the Spec of every Spawn call, in order.
func (s *FakeSpawner) Specs() []proc.Spec {
	s.mu.Lock()
	defer s.mu.Unlock()
	specs := make([]proc.Spec, len(s.procs))
	for i, p := range s.procs {
		specs[i] = p.spec
	}
	return specs
}

// Running returns the spawned processes that have not finished.
func (s *FakeSpawner) Running() []*FakeProcess {
	var running []*FakeProcess
	for _, p := range s.Procs() {
		if !p.Finished() {
			running = append(running, p)
		}
	}
	return running
}

// FakeProcess is a process that ends only when the test says so.
type FakeProcess struct {
	pid  int
	spec proc.Spec
	done chan proc.Exit
	once sync.Once

	mu       sync.Mutex
	finished bool
	signals  []os.Signal
}

// PID is a counter unique within the spawner.
func (p *FakeProcess) PID() int { return p.pid }

// Spec is what the process was spawned with.
func (p *FakeProcess) Spec() proc.Spec { return p.spec }

// Signal records sig. It fails with os.ErrProcessDone once finished, like a
// real process would.
func (p *FakeProcess) Signal(sig os.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.finished {
		return os.ErrProcessDone
	}
	p.signals = append(p.signals, sig)
	return nil
}

// KillGroup records SIGKILL and ends the process as killed by it, since a
// real process cannot survive one.
func (p *FakeProcess) KillGroup() error {
	if err := p.Signal(syscall.SIGKILL); err != nil {
		return err
	}
	p.Kill(syscall.SIGKILL)
	return nil
}

// Signals returns every signal delivered before the process finished.
func (p *FakeProcess) Signals() []os.Signal {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]os.Signal(nil), p.signals...)
}

// Done yields the Exit set by Finish or Kill, then closes.
func (p *FakeProcess) Done() <-chan proc.Exit { return p.done }

// Finished reports whether Finish or Kill has been called.
func (p *FakeProcess) Finished() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.finished
}

// Finish ends the process with an exit code and no result line. Later calls
// are no-ops.
func (p *FakeProcess) Finish(code int) { p.end(proc.Exit{Code: code}) }

// FinishResult ends the process with an exit code and line as the result the
// real spawner would have scanned. Later calls are no-ops.
func (p *FakeProcess) FinishResult(code int, line string) {
	p.end(proc.Exit{Code: code, Result: line})
}

// Kill ends the process as if sig terminated it. Later calls are no-ops.
func (p *FakeProcess) Kill(sig os.Signal) { p.end(proc.Exit{Code: -1, Signal: sig}) }

func (p *FakeProcess) end(exit proc.Exit) {
	p.once.Do(func() {
		p.mu.Lock()
		p.finished = true
		p.mu.Unlock()
		p.done <- exit
		close(p.done)
	})
}
