// Package hint defines the message that asks a pool for workers and the
// interface every producer of such messages (redis, http) implements.
package hint

import "context"

// Source names, used as the `source` label on hint metrics.
const (
	SourceRedis = "redis"
	SourceHTTP  = "http"
	SourcePoke  = "poke"
	// SourceWorker is a pool hinting itself with the task count a worker
	// printed before exiting.
	SourceWorker = "worker"
)

// Hint asks the pool listening on channel Pool for up to Count workers.
// Pool is the channel suffix as published (redis prefix already stripped),
// which is the pool's `channel` setting and defaults to the pool name.
// Source is the producer's name (SourceRedis, SourceHTTP).
type Hint struct {
	Source string
	Pool   string
	Count  int
}

// Source produces hints until ctx ends. Run blocks and returns nil on a
// clean stop or the error that made the source give up. It must never
// close out; the owner does.
type Source interface {
	Run(ctx context.Context, out chan<- Hint) error
}
