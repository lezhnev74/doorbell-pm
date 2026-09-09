package proctest

import (
	"context"
	"errors"
	"os"
	"syscall"
	"testing"

	"doorbell-pm/internal/proc"
)

func TestSpawnRecordsSpecsAndPIDs(t *testing.T) {
	var s FakeSpawner
	ctx := context.Background()
	a, err := s.Spawn(ctx, proc.Spec{Cmd: []string{"a"}})
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Spawn(ctx, proc.Spec{Cmd: []string{"b"}, Dir: "/tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if a.PID() == b.PID() {
		t.Fatalf("pids collide: %d", a.PID())
	}
	specs := s.Specs()
	if len(specs) != 2 || specs[0].Cmd[0] != "a" || specs[1].Dir != "/tmp" {
		t.Fatalf("specs = %+v", specs)
	}
	if got := s.Procs(); len(got) != 2 || got[0] != a || got[1] != b {
		t.Fatalf("procs = %v", got)
	}
}

func TestSpawnErr(t *testing.T) {
	want := errors.New("boom")
	s := FakeSpawner{Err: want}
	p, err := s.Spawn(context.Background(), proc.Spec{})
	if !errors.Is(err, want) || p != nil {
		t.Fatalf("Spawn = %v, %v", p, err)
	}
	if len(s.Procs()) != 0 {
		t.Fatal("failed spawn was recorded")
	}
}

func TestFinishDeliversExitThenCloses(t *testing.T) {
	var s FakeSpawner
	p := spawn(t, &s)

	select {
	case <-p.Done():
		t.Fatal("Done fired before Finish")
	default:
	}

	p.Finish(3)
	p.Finish(4) // ignored: a process ends once

	exit, ok := <-p.Done()
	if !ok || exit != (proc.Exit{Code: 3}) {
		t.Fatalf("exit = %+v, ok = %v", exit, ok)
	}
	if _, ok := <-p.Done(); ok {
		t.Fatal("Done not closed after delivering the exit")
	}
	if !p.Finished() {
		t.Fatal("Finished() = false after Finish")
	}
}

func TestFinishResultCarriesLine(t *testing.T) {
	var s FakeSpawner
	p := spawn(t, &s)
	p.FinishResult(0, "3")
	if exit := <-p.Done(); exit != (proc.Exit{Code: 0, Result: "3"}) {
		t.Fatalf("exit = %+v, want code 0 result 3", exit)
	}
}

func TestKillSetsSignal(t *testing.T) {
	var s FakeSpawner
	p := spawn(t, &s)
	p.Kill(syscall.SIGKILL)
	exit := <-p.Done()
	if exit.Signal != syscall.SIGKILL || exit.Code != -1 {
		t.Fatalf("exit = %+v", exit)
	}
}

func TestSignalRecordedUntilFinished(t *testing.T) {
	var s FakeSpawner
	p := spawn(t, &s)
	if err := p.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if got := p.Signals(); len(got) != 1 || got[0] != syscall.SIGTERM {
		t.Fatalf("signals = %v", got)
	}
	p.Finish(0)
	if err := p.Signal(syscall.SIGTERM); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("Signal after finish = %v, want ErrProcessDone", err)
	}
	if got := p.Signals(); len(got) != 1 {
		t.Fatalf("signal after finish was recorded: %v", got)
	}
}

func TestRunning(t *testing.T) {
	var s FakeSpawner
	a := spawn(t, &s)
	b := spawn(t, &s)
	a.Finish(0)
	if got := s.Running(); len(got) != 1 || got[0] != b {
		t.Fatalf("running = %v", got)
	}
}

func spawn(t *testing.T, s *FakeSpawner) *FakeProcess {
	t.Helper()
	p, err := s.Spawn(context.Background(), proc.Spec{Cmd: []string{"w"}})
	if err != nil {
		t.Fatal(err)
	}
	return p.(*FakeProcess)
}

func TestKillGroupRecordsAndEnds(t *testing.T) {
	var s FakeSpawner
	p := spawn(t, &s)
	if err := p.KillGroup(); err != nil {
		t.Fatal(err)
	}
	if exit := <-p.Done(); exit.Signal != syscall.SIGKILL {
		t.Fatalf("exit = %+v", exit)
	}
	if got := p.Signals(); len(got) != 1 || got[0] != syscall.SIGKILL {
		t.Fatalf("signals = %v", got)
	}
	if err := p.KillGroup(); !errors.Is(err, os.ErrProcessDone) {
		t.Fatalf("KillGroup after end = %v", err)
	}
}
