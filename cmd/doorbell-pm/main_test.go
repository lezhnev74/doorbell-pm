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
	if code := run([]string{"version"}, &out, &errOut); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	if got, want := out.String(), "doorbell-pm dev\n"; got != want {
		t.Fatalf("stdout = %q, want %q", got, want)
	}
}

func TestRunUsageErrors(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string // substring of stderr
	}{
		{"no args", nil, "COMMANDS:"},
		{"unknown flag", []string{"--bogus"}, "flag provided but not defined: -bogus"},
		{"unknown verb", []string{"bogus"}, `unknown command "bogus"`},
		{"run without config", []string{"run"}, "--config is required"},
		{"check unknown flag", []string{"check", "--bogus"}, "flag provided but not defined: -bogus"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			if code := run(tc.args, &out, &errOut); code != 2 {
				t.Fatalf("exit code = %d, want 2\n%s", code, errOut.String())
			}
			if !strings.Contains(errOut.String(), tc.want) {
				t.Fatalf("stderr = %q, want %q", errOut.String(), tc.want)
			}
			if strings.Count(errOut.String(), "doorbell-pm: ") > 1 {
				t.Fatalf("diagnostic printed twice:\n%s", errOut.String())
			}
			if out.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", out.String())
			}
		})
	}
}

func TestRunHelpGoesToStdout(t *testing.T) {
	for _, args := range [][]string{{"--help"}, {"run", "--help"}, {"check", "-h"}} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("%v: exit code = %d, want 0", args, code)
		}
		if !strings.Contains(out.String(), "USAGE:") || errOut.Len() != 0 {
			t.Fatalf("%v: stdout = %q, stderr = %q", args, out.String(), errOut.String())
		}
	}
}

func TestRunBadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("pools: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	if code := run([]string{"check", "--config", path}, &out, &errOut); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if !strings.Contains(errOut.String(), "doorbell-pm: ") || !strings.Contains(errOut.String(), "at least one enabled pool") {
		t.Fatalf("stderr = %q", errOut.String())
	}
	if out.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing printed before the error", out.String())
	}
}

func TestRunConfigFlagForms(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "hunter2")
	for _, args := range [][]string{
		{"check", "--config=../../test/testdata/config/full.yaml"},
		{"check", "-c", "../../test/testdata/config/full.yaml"},
	} {
		var out, errOut bytes.Buffer
		if code := run(args, &out, &errOut); code != 0 {
			t.Fatalf("%v: exit code = %d, want 0\n%s", args, code, errOut.String())
		}
		if !strings.Contains(out.String(), "pools:") {
			t.Fatalf("%v: stdout = %q", args, out.String())
		}
	}
}

func TestRunCheckPrintsResolvedConfig(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "hunter2")
	var out, errOut bytes.Buffer
	if code := run([]string{"check", "--config", "../../test/testdata/config/full.yaml"}, &out, &errOut); code != 0 {
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
