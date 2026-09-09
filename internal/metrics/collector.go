package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// poolCollector turns pool.Stats into gauges at scrape time.
type poolCollector struct {
	stats StatsFunc

	running     *prometheus.Desc
	concurrency *prometheus.Desc
	lastHint    *prometheus.Desc
	lastExit    *prometheus.Desc
	active      *prometheus.Desc
	level       *prometheus.Desc
	openUntil   *prometheus.Desc
}

func newPoolCollector(ns string, stats StatsFunc) *poolCollector {
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc(prometheus.BuildFQName(ns, "", name), help, []string{"pool"}, nil)
	}
	return &poolCollector{
		stats:       stats,
		running:     desc("running", "Workers alive right now."),
		concurrency: desc("concurrency", "Configured maximum workers."),
		lastHint:    desc("last_hint_timestamp", "Unix time of the last hint, 0 if none yet."),
		lastExit:    desc("last_exit_timestamp", "Unix time of the last reaped exit, 0 if none yet."),
		active:      desc("cooldown_active", "1 while the failure breaker is open."),
		level:       desc("cooldown_level", "Consecutive breaker trips, resets after a quiet period."),
		openUntil:   desc("cooldown_open_until_timestamp", "Unix time the breaker closes, 0 when closed."),
	}
}

func (c *poolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{c.running, c.concurrency, c.lastHint, c.lastExit, c.active, c.level, c.openUntil} {
		ch <- d
	}
}

func (c *poolCollector) Collect(ch chan<- prometheus.Metric) {
	for name, st := range c.stats() {
		gauge := func(d *prometheus.Desc, v float64) {
			ch <- prometheus.MustNewConstMetric(d, prometheus.GaugeValue, v, name)
		}
		gauge(c.running, float64(st.Running))
		gauge(c.concurrency, float64(st.Concurrency))
		gauge(c.lastHint, unix(st.LastHint))
		gauge(c.lastExit, unix(st.LastExit))
		gauge(c.active, boolean(!st.BreakerOpenUntil.IsZero()))
		gauge(c.level, float64(st.BreakerLevel))
		gauge(c.openUntil, unix(st.BreakerOpenUntil))
	}
}

// unix is t as fractional unix seconds, 0 for the zero time.
func unix(t time.Time) float64 {
	if t.IsZero() {
		return 0
	}
	return float64(t.UnixNano()) / 1e9
}

func boolean(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
