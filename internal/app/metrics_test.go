package app

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// get returns the status and body of url.
func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestMetricsOnMainListener(t *testing.T) {
	mr := miniredis.RunT(t)
	cfg, _ := testConfig(t, `
http: {addr: "127.0.0.1:0", metrics_path: /stats}
redis: {addr: "`+mr.Addr()+`"}
metrics: {namespace: bell}
pools:
  encoding: {concurrency: 3, command: [WORKER_BIN, encoding]}
  legacy: {enabled: false, command: [WORKER_BIN, legacy]}
`)
	a, buf, _ := runApp(t, cfg)
	waitFor(t, "http listener", func() bool { return a.HTTPAddr() != "" })
	waitFor(t, "redis subscription", func() bool { return strings.Contains(buf.String(), "redis subscribed") })

	code, body := get(t, "http://"+a.HTTPAddr()+"/stats")
	if code != http.StatusOK {
		t.Fatalf("status %d", code)
	}
	for _, want := range []string{
		`bell_build_info{version="test"} 1`,
		`bell_concurrency{pool="encoding"} 3`,
		`bell_running{pool="encoding"} 0`,
		`bell_spawns_total{pool="encoding"} 0`,
		`bell_redis_connected 1`,
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics lack %q:\n%s", want, body)
		}
	}
	if strings.Contains(body, `pool="legacy"`) {
		t.Fatalf("disabled pool exported:\n%s", body)
	}
	if code, _ := get(t, "http://"+a.HTTPAddr()+"/metrics"); code != http.StatusNotFound {
		t.Fatalf("default path status %d, want 404", code)
	}
}

func TestMetricsOnOwnListener(t *testing.T) {
	cfg, _ := testConfig(t, `
http: {addr: "127.0.0.1:0"}
redis: {enabled: false}
metrics: {addr: "127.0.0.1:0"}
pools:
  encoding: {command: [WORKER_BIN, encoding]}
`)
	a, _, _ := runApp(t, cfg)
	waitFor(t, "listeners", func() bool { return a.HTTPAddr() != "" && a.MetricsAddr() != "" })

	code, body := get(t, "http://"+a.MetricsAddr()+"/metrics")
	if code != http.StatusOK || !strings.Contains(body, `doorbell_running{pool="encoding"} 0`) {
		t.Fatalf("metrics listener: status %d\n%s", code, body)
	}
	if code, _ := get(t, "http://"+a.HTTPAddr()+"/metrics"); code != http.StatusNotFound {
		t.Fatalf("main listener status %d, want 404", code)
	}
	if code, _ := get(t, "http://"+a.MetricsAddr()+"/healthz"); code != http.StatusNotFound {
		t.Fatalf("healthz on metrics listener status %d, want 404", code)
	}
}

func TestMetricsDisabled(t *testing.T) {
	cfg, _ := testConfig(t, `
http: {addr: "127.0.0.1:0"}
redis: {enabled: false}
metrics: {enabled: false, addr: "127.0.0.1:0"}
pools:
  encoding: {command: [WORKER_BIN, encoding]}
`)
	a, _, _ := runApp(t, cfg)
	waitFor(t, "http listener", func() bool { return a.HTTPAddr() != "" })
	if code, _ := get(t, "http://"+a.HTTPAddr()+"/metrics"); code != http.StatusNotFound {
		t.Fatalf("status %d, want 404", code)
	}
	if a.MetricsAddr() != "" {
		t.Fatalf("metrics listener started at %s", a.MetricsAddr())
	}
}
