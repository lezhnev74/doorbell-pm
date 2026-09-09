package main

import (
	"io"
	"log/slog"

	"doorbell-pm/internal/config"
)

// unixTimestamps is the log.timestamp_format value that prints seconds since
// the epoch instead of a Go layout.
const unixTimestamps = "unix"

// newLogger builds the slog root from the log block: text or json handler,
// level, and the time key rendered with timestamp_format.
func newLogger(cfg config.Log, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:       cfg.Level,
		ReplaceAttr: timestampFormatter(cfg.TimestampFormat),
	}
	var h slog.Handler
	if cfg.Format == "json" {
		h = slog.NewJSONHandler(w, opts)
	} else {
		h = slog.NewTextHandler(w, opts)
	}
	return slog.New(h)
}

// timestampFormatter rewrites the built-in time attribute at the record's
// top level; nested groups are left alone.
func timestampFormatter(layout string) func([]string, slog.Attr) slog.Attr {
	return func(groups []string, a slog.Attr) slog.Attr {
		if len(groups) > 0 || a.Key != slog.TimeKey || a.Value.Kind() != slog.KindTime {
			return a
		}
		t := a.Value.Time()
		if layout == unixTimestamps {
			return slog.Int64(slog.TimeKey, t.Unix())
		}
		return slog.String(slog.TimeKey, t.Format(layout))
	}
}
