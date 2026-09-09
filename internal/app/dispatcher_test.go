package app

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
)

// fakeHinter records the counts it was hinted with and answers with reply.
type fakeHinter struct {
	mu     sync.Mutex
	counts []int
	reply  int
}

func (f *fakeHinter) Hint(_ context.Context, _ string, n int) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.counts = append(f.counts, n)
	return f.reply
}

func (f *fakeHinter) got() []int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.counts...)
}

func newDispatcher(t *testing.T) (*Dispatcher, *fakeHinter, *fakeHinter, *bytes.Buffer) {
	t.Helper()
	enc := &fakeHinter{reply: 2}
	rep := &fakeHinter{reply: 1}
	cfgs := []config.PoolConfig{
		{Name: "encoding", Channel: "encoding", Enabled: true},
		{Name: "reports", Channel: "rep-queue", Enabled: true},
		{Name: "legacy", Channel: "legacy", Enabled: false},
	}
	pools := map[string]Hinter{"encoding": enc, "reports": rep}
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return NewDispatcher(cfgs, pools, log, nil), enc, rep, &buf
}

func TestDispatchKnownChannel(t *testing.T) {
	d, enc, rep, _ := newDispatcher(t)
	if got := d.Dispatch(context.Background(), hint.Hint{Pool: "encoding", Count: 100}); got != 2 {
		t.Fatalf("spawned %d, want 2", got)
	}
	if got := enc.got(); len(got) != 1 || got[0] != 100 {
		t.Fatalf("encoding hinted with %v, want [100]", got)
	}
	if len(rep.got()) != 0 {
		t.Fatal("reports pool was hinted")
	}
}

func TestDispatchCustomChannel(t *testing.T) {
	d, enc, rep, _ := newDispatcher(t)
	if got := d.Dispatch(context.Background(), hint.Hint{Pool: "rep-queue", Count: 3}); got != 1 {
		t.Fatalf("spawned %d, want 1", got)
	}
	if got := rep.got(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("reports hinted with %v, want [3]", got)
	}
	if len(enc.got()) != 0 {
		t.Fatal("encoding pool was hinted")
	}
	// The pool name is not a channel when a custom one is set.
	if got := d.Dispatch(context.Background(), hint.Hint{Pool: "reports", Count: 3}); got != 0 {
		t.Fatalf("pool name routed, spawned %d", got)
	}
}

func TestDispatchDrops(t *testing.T) {
	tests := []struct {
		name, channel, reason string
	}{
		{"unknown", "nope", "unknown_pool"},
		{"disabled", "legacy", "disabled"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d, enc, rep, buf := newDispatcher(t)
			if got := d.Dispatch(context.Background(), hint.Hint{Pool: tc.channel, Count: 5}); got != 0 {
				t.Fatalf("spawned %d, want 0", got)
			}
			if len(enc.got())+len(rep.got()) != 0 {
				t.Fatal("a pool was hinted")
			}
			out := buf.String()
			if !strings.Contains(out, "msg=drop") || !strings.Contains(out, "reason="+tc.reason) {
				t.Fatalf("log missing drop with reason %s:\n%s", tc.reason, out)
			}
		})
	}
}

func TestRunDispatchesUntilClosed(t *testing.T) {
	d, enc, _, _ := newDispatcher(t)
	in := make(chan hint.Hint)
	done := make(chan struct{})
	go func() {
		d.Run(context.Background(), in)
		close(done)
	}()
	in <- hint.Hint{Pool: "encoding", Count: 1}
	in <- hint.Hint{Pool: "encoding", Count: 2}
	close(in)
	<-done
	if got := enc.got(); len(got) != 2 || got[0] != 1 || got[1] != 2 {
		t.Fatalf("encoding hinted with %v, want [1 2]", got)
	}
}

func TestRunStopsOnContext(t *testing.T) {
	d, _, _, _ := newDispatcher(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.Run(ctx, make(chan hint.Hint))
		close(done)
	}()
	cancel()
	<-done
}
