// Package httpapi serves the hint, health and metrics endpoints on the
// listener configured under `http`. The hint endpoint is a hint.Source:
// requests turn into hints on the channel handed to Run.
package httpapi

import (
	"context"
	"log/slog"
	"net/http"

	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
	"doorbell-pm/internal/pool"
)

// StatsFunc reports every enabled pool's snapshot by pool name for the
// health endpoint.
type StatsFunc func() map[string]pool.Stats

// Server owns the listener and the mux. Build it only when http is enabled;
// a disabled listener has nothing mounted at all.
type Server struct {
	listener
	cfg      config.HTTP
	prefix   string
	channels map[string]struct{}
	stats    StatsFunc
	metrics  http.Handler
	log      *slog.Logger
}

// New prepares a server for cfg. prefix is the redis channel prefix that hint
// keys carry (`jobs:encoding`); channels are the routable channel names, so
// anything else is answered with 404. stats feeds the health endpoint; nil
// reports no pools. metrics is mounted at cfg.MetricsPath; nil mounts
// nothing there. A nil log means slog.Default().
func New(cfg config.HTTP, prefix string, channels []string, stats StatsFunc, metrics http.Handler, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	if stats == nil {
		stats = func() map[string]pool.Stats { return nil }
	}
	known := make(map[string]struct{}, len(channels))
	for _, c := range channels {
		known[c] = struct{}{}
	}
	log = log.With("source", hint.SourceHTTP)
	return &Server{
		listener: listener{cfg: cfg, log: log},
		cfg:      cfg,
		prefix:   prefix,
		channels: known,
		stats:    stats,
		metrics:  metrics,
		log:      log,
	}
}

// Handler returns the mux with every endpoint mounted. Hints go to out.
// Exposed so tests and the app can serve it through httptest or their own
// listener.
func (s *Server) Handler(out chan<- hint.Hint) http.Handler {
	mux := http.NewServeMux()
	mux.Handle("POST "+s.cfg.HintPath, s.hintHandler(out))
	mux.HandleFunc("GET "+s.cfg.HealthPath, s.healthHandler)
	if s.metrics != nil {
		mux.Handle("GET "+s.cfg.MetricsPath, s.metrics)
	}
	return mux
}

// Run listens on cfg.Addr and serves until ctx ends, then drains in-flight
// requests for up to cfg.ShutdownTimeout. It returns nil after a clean stop
// and the listen or serve error otherwise.
func (s *Server) Run(ctx context.Context, out chan<- hint.Hint) error {
	return s.serve(ctx, s.cfg.Addr, s.Handler(out))
}
