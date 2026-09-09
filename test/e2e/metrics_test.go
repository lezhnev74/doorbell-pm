//go:build e2e

package e2e

import (
	"net/http"
	"strings"
	"testing"
)

// Scenario 7: /metrics on the main listener reflects the pool gauges and
// the breaker.
func TestMetricsReflectPoolState(t *testing.T) {
	d := startDoorbell(t, reapConfig)

	d.hint(`{"jobs:ok": 5}`)
	d.waitFor("running gauge 2", func() bool {
		return d.metricsHas(d.baseURL, `doorbell_running{pool="ok"} 2`)
	})
	d.waitFor("2 ok exits counted", func() bool {
		return d.metricsHas(d.baseURL, `doorbell_exits_total{code="0",pool="ok",result="ok"} 2`)
	})

	d.hint(`{"jobs:crash": 1}`)
	d.waitFor("breaker trip", func() bool {
		return d.metricsHas(d.baseURL, `doorbell_cooldown_active{pool="crash"} 1`)
	})
	d.hint(`{"jobs:crash": 1}`)
	d.waitFor("cooldown drop", func() bool {
		return d.metricsHas(d.baseURL, `doorbell_dropped_hints_total{pool="crash",reason="exit_cooldown",source="http"} 1`)
	})
	for _, want := range []string{
		`doorbell_spawns_total{pool="ok"} 2`,
		`doorbell_hints_total{pool="ok",source="http"} 1`,
		`doorbell_cooldown_trips_total{pool="crash"} 1`,
		`doorbell_exits_total{code="1",pool="crash",result="failure"} 1`,
		`doorbell_redis_connected 1`,
		`doorbell_build_info{version=`,
	} {
		if !d.metricsHas(d.baseURL, want) {
			t.Fatalf("metrics lack %q:\n%s", want, d.metrics(d.baseURL))
		}
	}
}

// Scenario 8: with metrics.addr set the scrape endpoint moves to its own
// listener and the main one answers 404.
func TestMetricsOnSeparateListener(t *testing.T) {
	d := startDoorbell(t, `
log: {level: debug}
http: {addr: "HTTP_ADDR"}
redis: {addr: "REDIS_ADDR"}
metrics: {addr: "METRICS_ADDR"}
pools:
  encoding:
    command: [WORKER_BIN, encoding]
`)
	d.waitFor("metrics listener", func() bool {
		return d.metricsHas(d.metricsURL, `doorbell_running{pool="encoding"} 0`)
	})
	resp, err := http.Get(d.baseURL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("main listener /metrics status = %d, want 404", resp.StatusCode)
	}
}

// metrics returns the /metrics body served at base, or "" on error.
func (d *doorbell) metrics(base string) string {
	resp, err := http.Get(base + "/metrics")
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	var b strings.Builder
	buf := make([]byte, 32<<10)
	for {
		n, err := resp.Body.Read(buf)
		b.Write(buf[:n])
		if err != nil {
			return b.String()
		}
	}
}

func (d *doorbell) metricsHas(base, line string) bool {
	return strings.Contains(d.metrics(base), line)
}
