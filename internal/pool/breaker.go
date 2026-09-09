package pool

import (
	"math"
	"time"

	"doorbell-pm/internal/config"
)

// breaker rate-limits process churn. It keeps the timestamps of the last
// threshold failures; when all of them fall inside window the pool is paused
// for a cooldown that grows by multiplier per consecutive trip, capped at
// maxCooldown. It never touches running workers. Not safe for concurrent use;
// the pool mutex guards it.
type breaker struct {
	threshold   int
	window      time.Duration
	cooldown    time.Duration
	maxCooldown time.Duration
	multiplier  float64

	failures  []time.Time
	openUntil time.Time
	level     int
}

func newBreaker(cfg config.PoolConfig) *breaker {
	return &breaker{
		threshold:   cfg.ExitFailureThreshold,
		window:      cfg.ExitFailureWindow,
		cooldown:    cfg.ExitCooldown,
		maxCooldown: cfg.ExitCooldownMax,
		multiplier:  cfg.ExitCooldownMultiplier,
	}
}

// enabled is false when the config turns the breaker off.
func (b *breaker) enabled() bool { return b.threshold > 0 && b.cooldown > 0 }

// open reports whether spawning is paused at now.
func (b *breaker) open(now time.Time) bool {
	b.decay(now)
	return now.Before(b.openUntil)
}

// failure records one failed exit at now. It returns the cooldown applied
// and true when this failure tripped the breaker.
func (b *breaker) failure(now time.Time) (time.Duration, bool) {
	if !b.enabled() {
		return 0, false
	}
	b.decay(now)
	b.failures = append(b.failures, now)
	if len(b.failures) > b.threshold {
		b.failures = b.failures[len(b.failures)-b.threshold:]
	}
	if now.Before(b.openUntil) || len(b.failures) < b.threshold || now.Sub(b.failures[0]) > b.window {
		return 0, false
	}
	d := b.nextCooldown()
	b.openUntil = now.Add(d)
	b.level++
	b.failures = b.failures[:0]
	return d, true
}

// ok records a successful exit at now. The escalation level resets when the
// breaker is closed and no failure is inside the window.
func (b *breaker) ok(now time.Time) {
	b.decay(now)
	if now.Before(b.openUntil) {
		return
	}
	for _, f := range b.failures {
		if now.Sub(f) <= b.window {
			return
		}
	}
	b.level = 0
}

// decay resets the level once the breaker has stayed closed for maxCooldown.
func (b *breaker) decay(now time.Time) {
	if b.level > 0 && !now.Before(b.openUntil.Add(b.maxCooldown)) {
		b.level = 0
	}
}

func (b *breaker) nextCooldown() time.Duration {
	d := float64(b.cooldown) * math.Pow(b.multiplier, float64(b.level))
	if d > float64(b.maxCooldown) {
		return b.maxCooldown
	}
	return time.Duration(d)
}
