// Package clock abstracts time so pool logic can be tested without sleeping.
package clock

import "time"

// Clock is the subset of package time the pool logic depends on.
type Clock interface {
	Now() time.Time
	// After delivers the current time once d has elapsed.
	After(d time.Duration) <-chan time.Time
	// NewTimer is After with a Stop, for deadlines that may be cancelled.
	NewTimer(d time.Duration) Timer
	// NewTicker delivers the current time every d until Stop.
	NewTicker(d time.Duration) Ticker
}

// Timer mirrors *time.Timer behind an interface.
type Timer interface {
	C() <-chan time.Time
	// Stop disarms the timer and reports whether it was still armed.
	Stop() bool
}

// Ticker mirrors *time.Ticker behind an interface.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

// Real is the wall clock.
type Real struct{}

// Now returns time.Now.
func (Real) Now() time.Time { return time.Now() }

// After returns time.After.
func (Real) After(d time.Duration) <-chan time.Time { return time.After(d) }

// NewTimer wraps time.NewTimer.
func (Real) NewTimer(d time.Duration) Timer { return realTimer{time.NewTimer(d)} }

// NewTicker wraps time.NewTicker.
func (Real) NewTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

type realTimer struct{ t *time.Timer }

func (t realTimer) C() <-chan time.Time { return t.t.C }
func (t realTimer) Stop() bool          { return t.t.Stop() }

type realTicker struct{ t *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.t.C }
func (t realTicker) Stop()               { t.t.Stop() }
