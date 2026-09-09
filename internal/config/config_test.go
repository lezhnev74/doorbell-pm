package config

import (
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const testdata = "../../test/testdata/config"

func writeConfig(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "doorbell.yaml")
	if err := os.WriteFile(path, []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadSample(t *testing.T) {
	cfg, err := Load(filepath.Join(testdata, "sample.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Redis.ChannelPrefix != "jobs:" {
		t.Fatalf("redis = %+v", cfg.Redis)
	}
	if cfg.Defaults.GraceShutdown == nil || cfg.Defaults.GraceShutdown.Std() != 30*time.Second {
		t.Fatalf("_defaults.grace_shutdown = %v", cfg.Defaults.GraceShutdown)
	}
	if _, leaked := cfg.Pools[DefaultsKey]; leaked || len(cfg.Pools) != 3 {
		t.Fatalf("pools = %d, want 3", len(cfg.Pools))
	}
	enc := cfg.Pools["encoding"]
	if enc.Concurrency == nil || *enc.Concurrency != 4 {
		t.Fatalf("encoding.concurrency = %v", enc.Concurrency)
	}
	if got := strings.Join(enc.Command, " "); got != "php worker.php --queue=encoding --drain --max-jobs=100" {
		t.Fatalf("encoding.command = %q", got)
	}
}

func TestLoadFull(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "s3cret")
	cfg, err := Load(filepath.Join(testdata, "full.yaml"))
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Log.Format != "text" || cfg.Log.Level != slog.LevelInfo ||
		cfg.Log.TimestampFormat != "2006-01-02T15:04:05.000Z07:00" {
		t.Fatalf("log = %+v", cfg.Log)
	}
	if cfg.HTTP.Enabled == nil || !*cfg.HTTP.Enabled || cfg.HTTP.Addr != "127.0.0.1:8080" ||
		cfg.HTTP.HintPath != "/hint" || cfg.HTTP.HealthPath != "/healthz" || cfg.HTTP.MetricsPath != "/metrics" ||
		cfg.HTTP.ReadTimeout.Std() != 5*time.Second || cfg.HTTP.WriteTimeout.Std() != 5*time.Second ||
		cfg.HTTP.ShutdownTimeout.Std() != 5*time.Second {
		t.Fatalf("http = %+v", cfg.HTTP)
	}
	if cfg.Redis.Enabled == nil || !*cfg.Redis.Enabled || cfg.Redis.Password != "s3cret" || cfg.Redis.DB != 0 ||
		cfg.Redis.TLS || cfg.Redis.DialTimeout.Std() != 5*time.Second ||
		cfg.Redis.ReconnectMin.Std() != 500*time.Millisecond || cfg.Redis.ReconnectMax.Std() != 30*time.Second {
		t.Fatalf("redis = %+v", cfg.Redis)
	}
	if cfg.Metrics.Enabled == nil || !*cfg.Metrics.Enabled || cfg.Metrics.Namespace != "doorbell" || cfg.Metrics.Addr != "" {
		t.Fatalf("metrics = %+v", cfg.Metrics)
	}
	if cfg.ShutdownTimeout.Std() != time.Minute {
		t.Fatalf("shutdown_timeout = %v", cfg.ShutdownTimeout)
	}

	d := cfg.Defaults
	for name, got := range map[string]bool{
		"concurrency":              d.Concurrency != nil && *d.Concurrency == 1,
		"ok_exit_codes":            len(d.OkExitCodes) == 1 && d.OkExitCodes[0] == 0,
		"exit_failure_threshold":   d.ExitFailureThreshold != nil && *d.ExitFailureThreshold == 3,
		"exit_failure_window":      d.ExitFailureWindow != nil && d.ExitFailureWindow.Std() == 10*time.Second,
		"exit_cooldown":            d.ExitCooldown != nil && d.ExitCooldown.Std() == 5*time.Second,
		"exit_cooldown_max":        d.ExitCooldownMax != nil && d.ExitCooldownMax.Std() == 5*time.Minute,
		"exit_cooldown_multiplier": d.ExitCooldownMultiplier != nil && *d.ExitCooldownMultiplier == 2,
		"ttl":                      d.TTL != nil && d.TTL.Std() == 0,
		"poke":                     d.Poke != nil && d.Poke.Std() == 0,
		"poke_count":               d.PokeCount != nil && *d.PokeCount == 1,
		"grace_shutdown":           d.GraceShutdown != nil && d.GraceShutdown.Std() == 30*time.Second,
		"term_signal":              d.TermSignal != nil && d.TermSignal.Std() == syscall.SIGTERM,
		"inherit_env":              d.InheritEnv != nil && *d.InheritEnv,
		"env":                      d.Env != nil && len(d.Env) == 0,
		"dir":                      d.Dir != nil && *d.Dir == "",
		"result_stream":            d.ResultStream != nil && *d.ResultStream == "stdout",
	} {
		if !got {
			t.Errorf("_defaults.%s not parsed as expected: %+v", name, d)
		}
	}

	enc, ok := cfg.Pools["encoding"]
	if !ok {
		t.Fatalf("pools = %v", cfg.Pools)
	}
	if enc.Enabled == nil || !*enc.Enabled || enc.Channel != "encoding" ||
		enc.Concurrency == nil || *enc.Concurrency != 4 ||
		enc.TTL == nil || enc.TTL.Std() != 10*time.Minute ||
		enc.Env["PHP_MEMORY_LIMIT"] != "512M" ||
		strings.Join(enc.Command, " ") != "php worker.php --queue=encoding --drain" {
		t.Fatalf("encoding = %+v", enc)
	}
	if enc.Poke != nil || enc.TermSignal != nil {
		t.Fatalf("encoding should not inherit defaults at parse time: %+v", enc)
	}
}

func TestLoadUnknownKey(t *testing.T) {
	for name, src := range map[string]string{
		"top":  "bogus: 1\npools: {}\n",
		"old":  "defaults:\n  ttl: 1s\npools: {}\n",
		"http": "http:\n  bogus: 1\n",
		"pool": "pools:\n  a:\n    command: [x]\n    spawn_cooldown: 1s\n",
	} {
		if _, err := Load(writeConfig(t, src)); err == nil {
			t.Errorf("%s: unknown key accepted", name)
		} else if !strings.Contains(err.Error(), "bogus") && !strings.Contains(err.Error(), "spawn_cooldown") && !strings.Contains(err.Error(), "defaults") {
			t.Errorf("%s: error does not name the key: %v", name, err)
		}
	}
}

func TestParsePoolNames(t *testing.T) {
	for _, src := range []string{
		"pools:\n  _defaults:\n    command: [x]\n",
		"pools:\n  _defaults:\n    channel: x\n",
		"pools:\n  _other:\n    command: [x]\n",
		"pools:\n  \"bad name\":\n    command: [x]\n",
		"pools:\n  \"\":\n    command: [x]\n",
	} {
		if _, err := Parse(src); err == nil {
			t.Errorf("accepted:\n%s", src)
		}
	}
	cfg, err := Parse("pools:\n  a.b-c_1:\n    command: [x]\n")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := cfg.Pools["a.b-c_1"]; !ok {
		t.Fatalf("pools = %v", cfg.Pools)
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load(filepath.Join(t.TempDir(), "nope.yaml")); !os.IsNotExist(err) && err == nil {
		t.Fatal("missing file accepted")
	}
}

func TestLoadEnvExpansion(t *testing.T) {
	t.Setenv("DB_ADDR", "redis.internal:6380")
	t.Setenv("DB_PASS", "pw")
	cfg, err := Load(writeConfig(t, "redis:\n  addr: \"${DB_ADDR}\"\n  password: $DB_PASS\npools: {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Redis.Addr != "redis.internal:6380" || cfg.Redis.Password != "pw" {
		t.Fatalf("redis = %+v", cfg.Redis)
	}
}

func TestLoadBadDuration(t *testing.T) {
	if _, err := Load(writeConfig(t, "shutdown_timeout: soon\n")); err == nil {
		t.Fatal("bad duration accepted")
	}
	if _, err := Load(writeConfig(t, "pools:\n  _defaults:\n    ttl: 5\n")); err == nil {
		t.Fatal("unitless non-zero duration accepted")
	}
}

func TestSignalParse(t *testing.T) {
	for in, want := range map[string]syscall.Signal{
		"SIGTERM": syscall.SIGTERM, "TERM": syscall.SIGTERM, "sigint": syscall.SIGINT,
		"SIGHUP": syscall.SIGHUP, "SIGQUIT": syscall.SIGQUIT, "SIGKILL": syscall.SIGKILL,
		"SIGUSR1": syscall.SIGUSR1, "SIGUSR2": syscall.SIGUSR2,
	} {
		cfg, err := Parse("pools:\n  _defaults:\n    term_signal: " + in + "\n")
		if err != nil {
			t.Errorf("%s: %v", in, err)
			continue
		}
		if cfg.Defaults.TermSignal == nil || cfg.Defaults.TermSignal.Std() != want {
			t.Errorf("%s = %v, want %v", in, cfg.Defaults.TermSignal, want)
		}
	}
	if _, err := Parse("pools:\n  _defaults:\n    term_signal: SIGFOO\n"); err == nil || !strings.Contains(err.Error(), "SIGFOO") {
		t.Fatalf("unknown signal: err = %v", err)
	}
	if got := Signal(syscall.SIGTERM).String(); got != "SIGTERM" {
		t.Fatalf("String() = %q", got)
	}
}
