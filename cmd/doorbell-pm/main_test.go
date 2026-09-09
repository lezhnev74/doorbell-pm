package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"doorbell-pm/internal/config"
)

func TestRunVersion(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-version"}, &out, &errOut); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if got, want := out.String(), "doorbell-pm dev\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestRunUnknownFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run([]string{"-bogus"}, &out, &errOut); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
}

func TestRunRequiresConfig(t *testing.T) {
	var out, errOut bytes.Buffer
	if code := run(nil, &out, &errOut); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if !strings.Contains(errOut.String(), "-config is required") {
		t.Fatalf("stderr = %q", errOut.String())
	}
}

func TestRunBadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("pools: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"-config", path, "-check"}, &out, &errOut); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "at least one enabled pool") {
		t.Fatalf("stderr = %q", errOut.String())
	}
}

func TestRunCheckPrintsResolvedConfig(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "hunter2")
	var out, errOut bytes.Buffer
	if code := run([]string{"-config", "../../test/testdata/config/full.yaml", "-check"}, &out, &errOut); code != 0 {
		t.Fatalf("exit code = %d, want 0\n%s", code, errOut.String())
	}
	got := out.String()
	for _, want := range []string{
		`password: "***"`, "shutdown_timeout: 1m0s", "  encoding:", "ttl: 10m0s",
		"term_signal: SIGTERM", "PHP_MEMORY_LIMIT: 512M", "exit_cooldown_max: 5m0s",
		"result_stream: stdout",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "hunter2") {
		t.Fatalf("secret leaked:\n%s", got)
	}
}

func TestNewLoggerJSONUnix(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(config.Log{Format: "json", Level: slog.LevelWarn, TimestampFormat: "unix"}, &buf)
	log.Info("hidden")
	log.Warn("shown", "pool", "p1")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("not one json record: %v\n%s", err, buf.String())
	}
	if rec["msg"] != "shown" || rec["pool"] != "p1" {
		t.Fatalf("record = %v", rec)
	}
	if _, ok := rec["time"].(float64); !ok {
		t.Fatalf("time = %v (%T), want unix number", rec["time"], rec["time"])
	}
}

func TestNewLoggerTextLayout(t *testing.T) {
	var buf bytes.Buffer
	log := newLogger(config.Log{Format: "text", Level: slog.LevelInfo, TimestampFormat: "2006"}, &buf)
	log.Info("hello")
	if !strings.HasPrefix(buf.String(), "time=20") || strings.Contains(buf.String(), "T") {
		t.Fatalf("log = %q, want a bare year timestamp", buf.String())
	}
}
