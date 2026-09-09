package pool

import (
	"context"
	"os"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/proc"
	"doorbell-pm/internal/proc/proctest"
)

func TestParseTasks(t *testing.T) {
	tests := []struct {
		line string
		n    int
		ok   bool
	}{
		{"0", 0, true},
		{"7", 7, true},
		{"250", 250, true},
		{"", 0, false},
		{"-1", 0, false},
		{"1.5", 0, false},
		{"7 tasks", 0, false},
		{"done", 0, false},
		{strings.Repeat("9", proc.MaxResult+1), 0, false},
	}
	for _, tc := range tests {
		n, ok := parseTasks(tc.line)
		if n != tc.n || ok != tc.ok {
			t.Errorf("parseTasks(%q) = %d, %v; want %d, %v", tc.line, n, ok, tc.n, tc.ok)
		}
	}
}

// recMetrics records Tasks, Hint and Drop calls; everything else is a no-op.
type recMetrics struct {
	nopMetrics
	mu    sync.Mutex
	tasks []int
	hints []string
	drops []string // source/reason
}

func (m *recMetrics) Tasks(_ string, n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tasks = append(m.tasks, n)
}

func (m *recMetrics) Hint(source, _ string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hints = append(m.hints, source)
}

func (m *recMetrics) Drop(source, _, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.drops = append(m.drops, source+"/"+reason)
}

func (m *recMetrics) snapshot() (tasks []int, hints, drops []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]int(nil), m.tasks...), append([]string(nil), m.hints...), append([]string(nil), m.drops...)
}

// newResultPool is newLoggedSink with a recording metrics sink.
func newResultPool(t *testing.T, concurrency int, mod func(*config.PoolConfig)) (*Pool, *proctest.FakeSpawner, *clock.FakeClock, *logSink, *recMetrics) {
	t.Helper()
	p, s, clk, sink := newLoggedSink(t, concurrency, mod)
	m := &recMetrics{}
	p.metrics = m
	return p, s, clk, sink, m
}

// waitProcs blocks until s has spawned want processes in total.
func waitProcs(t *testing.T, s *proctest.FakeSpawner, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(s.Procs()) == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("spawned %d procs, want %d", len(s.Procs()), want)
}

func TestSpecCarriesResultStream(t *testing.T) {
	p, s, _ := newBreakerPool(t, 1, func(c *config.PoolConfig) { c.ResultStream = "stderr" })
	p.Hint(ctx, "test", 1)
	if got := s.Specs()[0].ResultStream; got != proc.Stderr {
		t.Fatalf("spec.ResultStream = %q, want stderr", got)
	}
}

func TestOkExitWithTasksRespawns(t *testing.T) {
	p, s, _, sink, m := newResultPool(t, 4, nil)
	p.Hint(ctx, hint.SourceHTTP, 2)
	pr := s.Procs()[0]
	pr.FinishResult(0, "5")

	// Two slots were free and the exit freed a third: 5 wanted, 3 spawn.
	waitProcs(t, s, 5)
	waitRunning(t, p, 4)
	assertKeys(t, sink.one(t, "exit"), map[string]any{
		"pid": float64(pr.PID()), "result": "ok", "tasks": float64(5),
	})
	for _, r := range sink.find(t, "spawn")[2:] {
		assertKeys(t, r, map[string]any{"source": "worker"})
	}
	tasks, hints, _ := m.snapshot()
	if len(tasks) != 1 || tasks[0] != 5 {
		t.Fatalf("tasks metric = %v, want [5]", tasks)
	}
	if len(hints) != 2 || hints[1] != "worker" {
		t.Fatalf("hint sources = %v, want [http worker]", hints)
	}
}

func TestOkExitWithZeroTasksDoesNotRespawn(t *testing.T) {
	p, s, _, sink, m := newResultPool(t, 2, nil)
	p.Hint(ctx, hint.SourceHTTP, 1)
	s.Procs()[0].FinishResult(0, "0")
	waitRunning(t, p, 0)

	assertKeys(t, sink.one(t, "exit"), map[string]any{"result": "ok", "tasks": float64(0)})
	if got := len(s.Procs()); got != 1 {
		t.Fatalf("spawned %d procs, want 1", got)
	}
	if tasks, _, _ := m.snapshot(); len(tasks) != 1 || tasks[0] != 0 {
		t.Fatalf("tasks metric = %v, want [0]", tasks)
	}
}

func TestFailureWithTasksRecordsButDoesNotRespawn(t *testing.T) {
	p, s, _, sink, m := newResultPool(t, 2, nil)
	p.Hint(ctx, hint.SourceHTTP, 1)
	s.Procs()[0].FinishResult(2, "5")
	waitRunning(t, p, 0)

	assertKeys(t, sink.one(t, "exit"), map[string]any{"result": "failure", "tasks": float64(5)})
	if got := len(s.Procs()); got != 1 {
		t.Fatalf("spawned %d procs, want 1", got)
	}
	tasks, hints, _ := m.snapshot()
	if len(tasks) != 1 || tasks[0] != 5 {
		t.Fatalf("tasks metric = %v, want [5]", tasks)
	}
	if len(hints) != 1 {
		t.Fatalf("hint sources = %v, want only the http one", hints)
	}
}

func TestKilledWithTasksDoesNotRespawn(t *testing.T) {
	p, s, clk, sink, m := newResultPool(t, 2, func(c *config.PoolConfig) { c.TTL = time.Minute })
	p.Hint(ctx, hint.SourceHTTP, 1)
	pr := s.Procs()[0]
	clk.Advance(time.Minute)
	waitSignal(t, pr, os.Signal(syscall.SIGTERM))
	pr.FinishResult(0, "4")
	waitRunning(t, p, 0)

	assertKeys(t, sink.one(t, "exit"), map[string]any{"result": "killed", "reason": "ttl", "tasks": float64(4)})
	if got := len(s.Procs()); got != 1 {
		t.Fatalf("spawned %d procs, want 1", got)
	}
	if tasks, hints, _ := m.snapshot(); len(tasks) != 1 || len(hints) != 1 {
		t.Fatalf("tasks = %v hints = %v, want one each", tasks, hints)
	}
}

func TestGarbageResultIsIgnored(t *testing.T) {
	p, s, _, sink, m := newResultPool(t, 2, nil)
	p.Hint(ctx, hint.SourceHTTP, 1)
	pr := s.Procs()[0]
	pr.FinishResult(0, "processed 3")
	waitRunning(t, p, 0)

	exit := sink.one(t, "exit")
	if _, ok := exit["tasks"]; ok {
		t.Fatalf("exit line carries tasks for a garbage result: %v", exit)
	}
	assertKeys(t, sink.one(t, "result_ignored"), map[string]any{
		"level": "DEBUG", "pool": "enc", "pid": float64(pr.PID()), "line": "processed 3",
	})
	if got := len(s.Procs()); got != 1 {
		t.Fatalf("spawned %d procs, want 1", got)
	}
	if tasks, _, _ := m.snapshot(); len(tasks) != 0 {
		t.Fatalf("tasks metric = %v, want none", tasks)
	}
	if st := p.Stats(); st.HasLastTasks {
		t.Fatalf("stats = %+v, want no last tasks", st)
	}
}

func TestEmptyResultLogsNothing(t *testing.T) {
	p, s, _, sink, _ := newResultPool(t, 1, nil)
	p.Hint(ctx, hint.SourceHTTP, 1)
	s.Procs()[0].Finish(0)
	waitRunning(t, p, 0)
	sink.one(t, "exit")
	if got := sink.find(t, "result_ignored"); len(got) != 0 {
		t.Fatalf("result_ignored logged for an empty result: %v", got)
	}
}

func TestBreakerDropsWorkerHint(t *testing.T) {
	p, s, _, sink, m := newResultPool(t, 2, func(c *config.PoolConfig) { c.ExitFailureThreshold = 1 })
	p.Hint(ctx, hint.SourceHTTP, 2)
	procs := s.Procs()
	procs[0].Finish(1) // trips the breaker
	waitRunning(t, p, 1)
	procs[1].FinishResult(0, "2")
	waitRunning(t, p, 0)

	assertKeys(t, sink.one(t, "drop"), map[string]any{
		"reason": "exit_cooldown", "source": "worker", "count": float64(2),
	})
	if got := len(s.Procs()); got != 2 {
		t.Fatalf("spawned %d procs, want 2", got)
	}
	if _, _, drops := m.snapshot(); len(drops) != 1 || drops[0] != "worker/exit_cooldown" {
		t.Fatalf("drops = %v, want [worker/exit_cooldown]", drops)
	}
}

func TestShutdownDropsWorkerHint(t *testing.T) {
	p, s, _, sink, _ := newResultPool(t, 2, nil)
	p.Hint(ctx, hint.SourceHTTP, 1)
	pr := s.Procs()[0]
	go func() {
		waitSignal(t, pr, os.Signal(syscall.SIGTERM))
		pr.FinishResult(0, "3")
	}()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(s.Procs()); got != 1 {
		t.Fatalf("spawned %d procs, want 1", got)
	}
	if got := sink.find(t, "drop"); len(got) != 0 {
		t.Fatalf("a killed worker hinted the pool: %v", got)
	}
}

// A --once style worker that always returns 1 keeps exactly one worker alive
// across several reaps, never two.
func TestOnceChainKeepsOneWorker(t *testing.T) {
	p, s, _, _, _ := newResultPool(t, 1, nil)
	p.Hint(ctx, hint.SourceHTTP, 5)
	for i := range 4 {
		waitProcs(t, s, i+1)
		if got := p.Running(); got != 1 {
			t.Fatalf("round %d: running = %d, want 1", i, got)
		}
		s.Procs()[i].FinishResult(0, "1")
	}
	waitProcs(t, s, 5)
	waitRunning(t, p, 1)
	s.Procs()[4].FinishResult(0, "0")
	waitRunning(t, p, 0)
	time.Sleep(10 * time.Millisecond)
	if got := len(s.Procs()); got != 5 {
		t.Fatalf("spawned %d procs, want 5", got)
	}
}

func TestStatsExposeLastTasks(t *testing.T) {
	p, s := newPool(t, 2)
	if st := p.Stats(); st.HasLastTasks || st.LastTasks != 0 {
		t.Fatalf("fresh stats = %+v", st)
	}
	p.Hint(context.Background(), "test", 2)
	procs := s.Procs()
	procs[0].FinishResult(1, "6")
	waitRunning(t, p, 1)
	if st := p.Stats(); !st.HasLastTasks || st.LastTasks != 6 {
		t.Fatalf("after failure with count: %+v, want last tasks 6", st)
	}
	procs[1].Finish(0)
	waitRunning(t, p, 0)
	if st := p.Stats(); !st.HasLastTasks || st.LastTasks != 6 {
		t.Fatalf("after exit without count: %+v, want last tasks kept at 6", st)
	}
}
