// Package redisin turns redis pub/sub messages into hints. It subscribes to
// `<channel_prefix>*`, maps the channel suffix to the pool channel and the
// payload to the count, and keeps resubscribing with backoff while redis is
// away.
package redisin

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"

	"doorbell-pm/internal/clock"
	"doorbell-pm/internal/config"
	"doorbell-pm/internal/hint"
)

// Metrics receives the source's connection events and dropped messages.
// nil in New means a no-op.
type Metrics interface {
	// RedisConnected reports the subscription state: true once the server
	// confirmed it, false when the session ends.
	RedisConnected(up bool)
	// RedisReconnect counts one reconnect attempt after a lost session.
	RedisReconnect()
	// Drop counts one unparsable message (reason bad_payload, pool "").
	Drop(source, pool, reason string)
}

type nopMetrics struct{}

func (nopMetrics) RedisConnected(bool)         {}
func (nopMetrics) RedisReconnect()             {}
func (nopMetrics) Drop(string, string, string) {}

// Source is a hint.Source fed by redis pub/sub.
type Source struct {
	cfg     config.Redis
	clk     clock.Clock
	log     *slog.Logger
	metrics Metrics
}

// New prepares a source for cfg. A nil clk means the wall clock; a nil log
// means slog.Default(); a nil m means no metrics.
func New(cfg config.Redis, clk clock.Clock, log *slog.Logger, m Metrics) *Source {
	if clk == nil {
		clk = clock.Real{}
	}
	if log == nil {
		log = slog.Default()
	}
	if m == nil {
		m = nopMetrics{}
	}
	return &Source{cfg: cfg, clk: clk, log: log.With("source", hint.SourceRedis), metrics: m}
}

// Run subscribes and forwards hints to out until ctx ends. A lost connection
// is retried after reconnect_min, doubling up to reconnect_max; the backoff
// resets once a subscription is confirmed again. It returns nil after ctx is
// done and never gives up on its own.
func (s *Source) Run(ctx context.Context, out chan<- hint.Hint) error {
	backoff := s.cfg.ReconnectMin.Std()
	for {
		subscribed, err := s.session(ctx, out)
		if ctx.Err() != nil {
			return nil
		}
		if subscribed {
			backoff = s.cfg.ReconnectMin.Std()
		}
		s.log.Warn("redis connection lost, reconnecting", "err", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-s.clk.After(backoff):
		}
		backoff = min(backoff*2, s.cfg.ReconnectMax.Std())
		s.metrics.RedisReconnect()
	}
}

// session runs one connection: subscribe, forward messages, return whether
// the subscription was ever confirmed and the error that ended it. ctx
// cancellation unblocks the read by closing the subscription.
func (s *Source) session(ctx context.Context, out chan<- hint.Hint) (subscribed bool, err error) {
	client := redis.NewClient(s.options())
	defer client.Close()

	ps := client.PSubscribe(ctx, pattern(s.cfg.ChannelPrefix))
	defer ps.Close()
	defer s.metrics.RedisConnected(false)

	done := make(chan struct{})
	defer close(done)
	go closeOnCancel(ctx, ps, done)

	for {
		msg, err := ps.Receive(ctx)
		if err != nil {
			return subscribed, err
		}
		switch m := msg.(type) {
		case *redis.Subscription:
			subscribed = true
			s.metrics.RedisConnected(true)
			s.log.Info("redis subscribed", "addr", s.cfg.Addr, "pattern", m.Channel)
		case *redis.Message:
			s.forward(ctx, out, m)
		}
	}
}

// closeOnCancel closes ps when ctx ends before done is closed; Receive has
// no other way to be unblocked.
func closeOnCancel(ctx context.Context, ps *redis.PubSub, done <-chan struct{}) {
	select {
	case <-ctx.Done():
		_ = ps.Close()
	case <-done:
	}
}

// forward parses one message and hands it to out. Bad payloads are dropped
// with a log line and never abort the session.
func (s *Source) forward(ctx context.Context, out chan<- hint.Hint, m *redis.Message) {
	channel, ok := strings.CutPrefix(m.Channel, s.cfg.ChannelPrefix)
	if !ok {
		s.log.Warn("drop", "reason", "bad_payload", "channel", m.Channel, "err", "channel without prefix")
		s.metrics.Drop(hint.SourceRedis, "", "bad_payload")
		return
	}
	count, err := parseCount(m.Payload)
	if err != nil {
		s.log.Warn("drop", "reason", "bad_payload", "channel", channel, "payload", m.Payload, "err", err)
		s.metrics.Drop(hint.SourceRedis, "", "bad_payload")
		return
	}
	select {
	case out <- hint.Hint{Source: hint.SourceRedis, Pool: channel, Count: count}:
	case <-ctx.Done():
	}
}

func parseCount(payload string) (int, error) {
	n, err := strconv.Atoi(strings.TrimSpace(payload))
	if err != nil {
		return 0, errors.New("payload is not an integer")
	}
	if n < 0 {
		return 0, errors.New("count must be >= 0")
	}
	return n, nil
}

func (s *Source) options() *redis.Options {
	opt := &redis.Options{
		Addr:        s.cfg.Addr,
		Username:    s.cfg.Username,
		Password:    s.cfg.Password,
		DB:          s.cfg.DB,
		DialTimeout: s.cfg.DialTimeout.Std(),
		MaxRetries:  -1, // Run owns the retry and backoff policy
	}
	if s.cfg.TLS {
		opt.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	return opt
}

// pattern builds the PSUBSCRIBE glob for prefix, escaping glob
// metacharacters so the prefix matches literally.
func pattern(prefix string) string {
	var b strings.Builder
	for _, r := range prefix {
		if strings.ContainsRune(`*?[]\`, r) {
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	b.WriteByte('*')
	return b.String()
}
