package clock

import (
	"testing"
	"time"
)

var t0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func recv(t *testing.T, ch <-chan time.Time) time.Time {
	t.Helper()
	select {
	case v := <-ch:
		return v
	default:
		t.Fatal("channel did not fire")
		return time.Time{}
	}
}

func assertQuiet(t *testing.T, ch <-chan time.Time) {
	t.Helper()
	select {
	case v := <-ch:
		t.Fatalf("unexpected fire at %v", v)
	default:
	}
}

func TestNowFrozenUntilAdvance(t *testing.T) {
	c := NewFake(t0)
	if c.Now() != t0 {
		t.Fatalf("Now = %v", c.Now())
	}
	c.Advance(time.Second)
	if got := c.Now(); got != t0.Add(time.Second) {
		t.Fatalf("Now = %v", got)
	}
}

func TestAfterFiresOnceAtDeadline(t *testing.T) {
	c := NewFake(t0)
	ch := c.After(2 * time.Second)

	c.Advance(time.Second)
	assertQuiet(t, ch)

	c.Advance(time.Second)
	if got := recv(t, ch); got != t0.Add(2*time.Second) {
		t.Fatalf("fired with %v", got)
	}
	if c.Pending() != 0 {
		t.Fatal("timer still armed after firing")
	}
	c.Advance(time.Hour)
	assertQuiet(t, ch)
}

func TestTimersFireInDeadlineOrderWithinOneAdvance(t *testing.T) {
	c := NewFake(t0)
	late := c.After(3 * time.Second)
	early := c.After(time.Second)
	mid := c.After(2 * time.Second)

	c.Advance(10 * time.Second)

	if got := recv(t, early); got != t0.Add(time.Second) {
		t.Fatalf("early fired at %v", got)
	}
	if got := recv(t, mid); got != t0.Add(2*time.Second) {
		t.Fatalf("mid fired at %v", got)
	}
	if got := recv(t, late); got != t0.Add(3*time.Second) {
		t.Fatalf("late fired at %v", got)
	}
	if got := c.Now(); got != t0.Add(10*time.Second) {
		t.Fatalf("Now = %v", got)
	}
}

func TestTickerRepeatsAndStops(t *testing.T) {
	c := NewFake(t0)
	tk := c.NewTicker(time.Second)

	c.Advance(time.Second)
	if got := recv(t, tk.C()); got != t0.Add(time.Second) {
		t.Fatalf("tick 1 = %v", got)
	}
	c.Advance(time.Second)
	if got := recv(t, tk.C()); got != t0.Add(2*time.Second) {
		t.Fatalf("tick 2 = %v", got)
	}

	// Undrained ticks are dropped, not queued.
	c.Advance(5 * time.Second)
	if got := recv(t, tk.C()); got != t0.Add(3*time.Second) {
		t.Fatalf("tick 3 = %v", got)
	}
	assertQuiet(t, tk.C())

	tk.Stop()
	if c.Pending() != 0 {
		t.Fatal("ticker still armed after Stop")
	}
	c.Advance(time.Minute)
	assertQuiet(t, tk.C())
}

func TestTimerStopDisarms(t *testing.T) {
	c := NewFake(t0)
	tm := c.NewTimer(time.Second)
	if !tm.Stop() {
		t.Fatal("Stop on armed timer returned false")
	}
	if c.Pending() != 0 {
		t.Fatal("timer still armed after Stop")
	}
	c.Advance(time.Minute)
	assertQuiet(t, tm.C())
	if tm.Stop() {
		t.Fatal("second Stop returned true")
	}
}

func TestTimerFiresThenStopReportsFalse(t *testing.T) {
	c := NewFake(t0)
	tm := c.NewTimer(time.Second)
	c.Advance(time.Second)
	if got := recv(t, tm.C()); got != t0.Add(time.Second) {
		t.Fatalf("fired with %v", got)
	}
	if tm.Stop() {
		t.Fatal("Stop after firing returned true")
	}
}

func TestZeroAfterFiresOnNextAdvance(t *testing.T) {
	c := NewFake(t0)
	ch := c.After(0)
	assertQuiet(t, ch)
	c.Advance(0)
	recv(t, ch)
}

func TestNewTickerRejectsNonPositive(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	NewFake(t0).NewTicker(0)
}

func TestRealClockDelivers(t *testing.T) {
	var c Clock = Real{}
	before := c.Now()
	select {
	case got := <-c.After(time.Millisecond):
		if got.Before(before) {
			t.Fatalf("After delivered %v before %v", got, before)
		}
	case <-time.After(time.Second):
		t.Fatal("After never fired")
	}
	tk := c.NewTicker(time.Millisecond)
	defer tk.Stop()
	select {
	case <-tk.C():
	case <-time.After(time.Second):
		t.Fatal("ticker never fired")
	}
	tm := c.NewTimer(time.Millisecond)
	select {
	case <-tm.C():
	case <-time.After(time.Second):
		t.Fatal("timer never fired")
	}
	if c.NewTimer(time.Hour).Stop() != true {
		t.Fatal("Stop on armed real timer returned false")
	}
}
