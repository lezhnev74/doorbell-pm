package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"

	"doorbell-pm/internal/config"
)

// listener is the listen, serve and drain cycle shared by the main server
// and the standalone metrics server. cfg supplies the timeouts.
type listener struct {
	cfg config.HTTP
	log *slog.Logger

	mu   sync.Mutex
	addr string
}

// Addr is the bound listener address, empty until serve has listened.
// Useful when the configured address ends in :0.
func (l *listener) Addr() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.addr
}

// serve listens on addr and serves h until ctx ends, then drains in-flight
// requests for up to cfg.ShutdownTimeout. It returns nil after a clean stop
// and the listen or serve error otherwise.
func (l *listener) serve(ctx context.Context, addr string, h http.Handler) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.addr = ln.Addr().String()
	l.mu.Unlock()

	srv := &http.Server{
		Handler:      h,
		ReadTimeout:  l.cfg.ReadTimeout.Std(),
		WriteTimeout: l.cfg.WriteTimeout.Std(),
		BaseContext:  func(net.Listener) context.Context { return ctx },
	}
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	l.log.Info("http listening", "addr", l.addr)

	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return l.shutdown(srv, errc)
	}
}

// shutdown drains srv for up to cfg.ShutdownTimeout, closing it outright
// when that runs out, and reports how Serve ended.
func (l *listener) shutdown(srv *http.Server, errc <-chan error) error {
	stopCtx, cancel := context.WithTimeout(context.Background(), l.cfg.ShutdownTimeout.Std())
	defer cancel()
	if err := srv.Shutdown(stopCtx); err != nil {
		l.log.Warn("http shutdown", "err", err)
		_ = srv.Close()
	}
	if err := <-errc; !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// MetricsServer serves the metrics handler alone on `metrics.addr`, for
// setups that keep the scrape endpoint off the hint listener.
type MetricsServer struct {
	listener
	addr    string
	path    string
	handler http.Handler
}

// NewMetricsServer serves h at cfg.MetricsPath on addr, with cfg's
// timeouts. A nil log means slog.Default().
func NewMetricsServer(cfg config.HTTP, addr string, h http.Handler, log *slog.Logger) *MetricsServer {
	if log == nil {
		log = slog.Default()
	}
	return &MetricsServer{
		listener: listener{cfg: cfg, log: log.With("listener", "metrics")},
		addr:     addr,
		path:     cfg.MetricsPath,
		handler:  h,
	}
}

// Run serves until ctx ends. See listener.serve for the return value.
func (m *MetricsServer) Run(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.Handle("GET "+m.path, m.handler)
	return m.serve(ctx, m.addr, mux)
}
