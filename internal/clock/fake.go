package clock

import (
	"sort"
	"sync"
	"time"
)

// FakeClock only moves when Advance is called. Timers due within an Advance
// fire in deadline order, each observing Now equal to its own deadline.
type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []*waiter
}

type waiter struct {
	at     time.Time
	period time.Duration // 0 for After
	ch     chan time.Time
}

// NewFake returns a clock frozen at start.
func NewFake(start time.Time) *FakeClock {
	return &FakeClock{now: start}
}

// Now returns the frozen time.
func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// After fires once when Advance moves the clock past now+d.
// A non-positive d fires on the next Advance, like a zero timer.
func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	return c.add(d, 0).ch
}

// NewTimer is After that can be disarmed with Stop.
func (c *FakeClock) NewTimer(d time.Duration) Timer {
	return &fakeTimer{c: c, w: c.add(d, 0)}
}

// NewTicker fires every d during Advance until Stop. d must be positive,
// as with time.NewTicker.
func (c *FakeClock) NewTicker(d time.Duration) Ticker {
	if d <= 0 {
		panic("clock: non-positive interval for NewTicker")
	}
	return &fakeTicker{c: c, w: c.add(d, d)}
}

// Advance moves the clock forward by d, firing every due timer in order.
func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	target := c.now.Add(d)
	for {
		w := c.earliest()
		if w == nil || w.at.After(target) {
			break
		}
		c.now = w.at
		select {
		case w.ch <- w.at:
		default: // receiver has not drained the previous tick; drop like time.Ticker
		}
		if w.period > 0 {
			w.at = w.at.Add(w.period)
		} else {
			c.remove(w)
		}
	}
	c.now = target
}

// Pending reports how many timers and tickers are armed.
func (c *FakeClock) Pending() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.waiters)
}

func (c *FakeClock) add(d, period time.Duration) *waiter {
	c.mu.Lock()
	defer c.mu.Unlock()
	w := &waiter{at: c.now.Add(d), period: period, ch: make(chan time.Time, 1)}
	c.waiters = append(c.waiters, w)
	return w
}

// earliest picks the due-first waiter; ties keep insertion order.
func (c *FakeClock) earliest() *waiter {
	if len(c.waiters) == 0 {
		return nil
	}
	sort.SliceStable(c.waiters, func(i, j int) bool { return c.waiters[i].at.Before(c.waiters[j].at) })
	return c.waiters[0]
}

// remove disarms w and reports whether it was still armed.
func (c *FakeClock) remove(w *waiter) bool {
	for i, x := range c.waiters {
		if x == w {
			c.waiters = append(c.waiters[:i], c.waiters[i+1:]...)
			return true
		}
	}
	return false
}

type fakeTimer struct {
	c *FakeClock
	w *waiter
}

func (t *fakeTimer) C() <-chan time.Time { return t.w.ch }

func (t *fakeTimer) Stop() bool {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	return t.c.remove(t.w)
}

type fakeTicker struct {
	c *FakeClock
	w *waiter
}

func (t *fakeTicker) C() <-chan time.Time { return t.w.ch }

func (t *fakeTicker) Stop() {
	t.c.mu.Lock()
	defer t.c.mu.Unlock()
	t.c.remove(t.w)
}
