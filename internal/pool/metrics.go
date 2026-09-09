package pool

import "time"

// Metrics receives the pool's churn events. The real implementation lives in
// internal/metrics; nil in New means a no-op.
type Metrics interface {
	// Spawn counts one started worker.
	Spawn(pool string)
	// SpawnError counts one failed spawn.
	SpawnError(pool string)
	// Exit counts one reaped worker. result is ok|failure|killed; code is the
	// exit code, the signal name, or the kill reason (ttl|shutdown).
	Exit(pool, result, code string, d time.Duration)
	// Kill counts one worker terminated by doorbell (reason ttl|shutdown).
	Kill(pool, reason string)
	// Trip counts one breaker trip.
	Trip(pool string)
	// Hint counts one hint that reached the pool.
	Hint(source, pool string)
	// Drop counts one hint the pool refused (reason exit_cooldown|shutdown).
	Drop(source, pool, reason string)
	// Tasks adds the task count a worker reported on exit.
	Tasks(pool string, n int)
}

type nopMetrics struct{}

func (nopMetrics) Spawn(string)                               {}
func (nopMetrics) SpawnError(string)                          {}
func (nopMetrics) Exit(string, string, string, time.Duration) {}
func (nopMetrics) Kill(string, string)                        {}
func (nopMetrics) Trip(string)                                {}
func (nopMetrics) Hint(string, string)                        {}
func (nopMetrics) Drop(string, string, string)                {}
func (nopMetrics) Tasks(string, int)                          {}
