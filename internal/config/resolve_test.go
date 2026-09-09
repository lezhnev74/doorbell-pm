package config

import (
	"fmt"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

const minimal = "pools:\n  a:\n    command: [x]\n"

func finalize(t *testing.T, src string) Config {
	t.Helper()
	cfg, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Finalize(); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestFinalizeTestdata(t *testing.T) {
	t.Setenv("REDIS_PASSWORD", "x")
	for _, name := range []string{"sample.yaml", "full.yaml"} {
		cfg, err := Load(filepath.Join(testdata, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := cfg.Finalize(); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestBuiltinDefaults(t *testing.T) {
	cfg := finalize(t, minimal)

	if cfg.Log.Format != "text" || cfg.Log.TimestampFormat != DefaultLogTimestampFormat {
		t.Errorf("log = %+v", cfg.Log)
	}
	if !cfg.HTTP.IsEnabled() || cfg.HTTP.Addr != "127.0.0.1:8080" || cfg.HTTP.HintPath != "/hint" ||
		cfg.HTTP.HealthPath != "/healthz" || cfg.HTTP.MetricsPath != "/metrics" ||
		cfg.HTTP.ReadTimeout.Std() != 5*time.Second || cfg.HTTP.WriteTimeout.Std() != 5*time.Second ||
		cfg.HTTP.ShutdownTimeout.Std() != 5*time.Second {
		t.Errorf("http = %+v", cfg.HTTP)
	}
	if !cfg.Redis.IsEnabled() || cfg.Redis.Addr != "127.0.0.1:6379" || cfg.Redis.ChannelPrefix != "jobs:" ||
		cfg.Redis.DialTimeout.Std() != 5*time.Second || cfg.Redis.ReconnectMin.Std() != 500*time.Millisecond ||
		cfg.Redis.ReconnectMax.Std() != 30*time.Second {
		t.Errorf("redis = %+v", cfg.Redis)
	}
	if !cfg.Metrics.IsEnabled() || cfg.Metrics.Namespace != "doorbell" || cfg.Metrics.Addr != "" {
		t.Errorf("metrics = %+v", cfg.Metrics)
	}
	if cfg.ShutdownTimeout.Std() != time.Minute {
		t.Errorf("shutdown_timeout = %s", cfg.ShutdownTimeout)
	}

	p := cfg.Pool("a")
	want := builtinPool
	want.Name, want.Channel, want.Command, want.Env = "a", "a", []string{"x"}, map[string]string{}
	if !samePool(p, want) {
		t.Errorf("pool a:\n got %+v\nwant %+v", p, want)
	}
	pools := cfg.ResolvedPools()
	if len(pools) != 1 || pools[0].Name != "a" {
		t.Errorf("ResolvedPools = %+v", pools)
	}
}

func TestExplicitFalseKeepsDisabled(t *testing.T) {
	cfg := finalize(t, "http:\n  enabled: false\n"+minimal)
	if cfg.HTTP.IsEnabled() || !cfg.Redis.IsEnabled() {
		t.Fatalf("http=%v redis=%v", cfg.HTTP.IsEnabled(), cfg.Redis.IsEnabled())
	}
}

// Every pools._defaults key must be overridable per pool, and a pool value
// must win over _defaults, which must win over the built-in.
func TestPoolOverridesDefaults(t *testing.T) {
	cases := []struct {
		key               string
		def, pool         string // yaml values
		get               func(PoolConfig) any
		wantDef, wantPool any
	}{
		{"enabled", "true", "false", func(p PoolConfig) any { return p.Enabled }, true, false},
		{"concurrency", "3", "7", func(p PoolConfig) any { return p.Concurrency }, 3, 7},
		{"ok_exit_codes", "[0, 1]", "[0, 2, 3]", func(p PoolConfig) any { return len(p.OkExitCodes) }, 2, 3},
		{"exit_failure_threshold", "5", "9", func(p PoolConfig) any { return p.ExitFailureThreshold }, 5, 9},
		{"exit_failure_window", "1s", "2s", func(p PoolConfig) any { return p.ExitFailureWindow }, time.Second, 2 * time.Second},
		{"exit_cooldown", "1s", "2s", func(p PoolConfig) any { return p.ExitCooldown }, time.Second, 2 * time.Second},
		{"exit_cooldown_max", "6m", "7m", func(p PoolConfig) any { return p.ExitCooldownMax }, 6 * time.Minute, 7 * time.Minute},
		{"exit_cooldown_multiplier", "3", "1.5", func(p PoolConfig) any { return p.ExitCooldownMultiplier }, 3.0, 1.5},
		{"ttl", "1m", "2m", func(p PoolConfig) any { return p.TTL }, time.Minute, 2 * time.Minute},
		{"poke", "1m", "2m", func(p PoolConfig) any { return p.Poke }, time.Minute, 2 * time.Minute},
		{"poke_count", "3", "5", func(p PoolConfig) any { return p.PokeCount }, 3, 5},
		{"grace_shutdown", "1s", "2s", func(p PoolConfig) any { return p.GraceShutdown }, time.Second, 2 * time.Second},
		{"term_signal", "SIGINT", "SIGHUP", func(p PoolConfig) any { return p.TermSignal }, syscall.SIGINT, syscall.SIGHUP},
		{"inherit_env", "false", "true", func(p PoolConfig) any { return p.InheritEnv }, false, true},
		{"env", "{A: d}", "{A: p}", func(p PoolConfig) any { return p.Env["A"] }, "d", "p"},
		{"dir", "/d", "/p", func(p PoolConfig) any { return p.Dir }, "/d", "/p"},
		{"result_stream", "stderr", "none", func(p PoolConfig) any { return p.ResultStream }, "stderr", "none"},
	}
	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			withDef := "pools:\n  _defaults:\n    " + tc.key + ": " + tc.def + "\n"
			if tc.key != "concurrency" {
				withDef += "    concurrency: 10\n" // room for poke_count values
			}
			withDef += "  a:\n    command: [x]\n  b:\n    command: [x]\n"
			cfg := finalize(t, withDef)
			if got := tc.get(cfg.Pool("a")); got != tc.wantDef {
				t.Errorf("defaults only: got %v, want %v", got, tc.wantDef)
			}
			cfg = finalize(t, withDef+"    "+tc.key+": "+tc.pool+"\n")
			if got := tc.get(cfg.Pool("b")); got != tc.wantPool {
				t.Errorf("pool override: got %v, want %v", got, tc.wantPool)
			}
			if got := tc.get(cfg.Pool("a")); got != tc.wantDef {
				t.Errorf("sibling pool leaked override: got %v, want %v", got, tc.wantDef)
			}
		})
	}
}

func TestExplicitZeroOverridesDefault(t *testing.T) {
	cfg := finalize(t, "pools:\n  _defaults:\n    ttl: 10m\n    poke: 1m\n    exit_cooldown: 5s\n"+
		"  a:\n    ttl: 0\n    poke: 0\n    exit_cooldown: 0\n    command: [x]\n  b:\n    command: [x]\n")
	a, b := cfg.Pool("a"), cfg.Pool("b")
	if a.TTL != 0 || a.Poke != 0 || a.ExitCooldown != 0 {
		t.Errorf("explicit zero lost: %+v", a)
	}
	if b.TTL != 10*time.Minute || b.Poke != time.Minute || b.ExitCooldown != 5*time.Second {
		t.Errorf("defaults lost: %+v", b)
	}
}

func TestEnvMergesLayers(t *testing.T) {
	cfg := finalize(t, "pools:\n  _defaults:\n    env: {A: 1, B: 2}\n  a:\n    env: {B: 3, C: 4}\n    command: [x]\n")
	env := cfg.Pool("a").Env
	if env["A"] != "1" || env["B"] != "3" || env["C"] != "4" || len(env) != 3 {
		t.Fatalf("env = %v", env)
	}
}

func TestChannelDefaultsToName(t *testing.T) {
	cfg := finalize(t, "pools:\n  a:\n    command: [x]\n  b:\n    channel: other\n    command: [x]\n")
	if cfg.Pool("a").Channel != "a" || cfg.Pool("b").Channel != "other" {
		t.Fatalf("channels: %q %q", cfg.Pool("a").Channel, cfg.Pool("b").Channel)
	}
}

func TestValidateRules(t *testing.T) {
	pool := func(body string) string { return "pools:\n  a:\n    command: [x]\n" + body }
	cases := []struct{ name, src, want string }{
		{"no pools", "pools: {}\n", "pools: at least one enabled pool"},
		{"all disabled", "pools:\n  a:\n    enabled: false\n    command: [x]\n", "at least one enabled pool"},
		{"no command", "pools:\n  a:\n    concurrency: 1\n", "pools.a.command: must not be empty"},
		{"empty command", "pools:\n  a:\n    command: []\n", "pools.a.command"},
		{"concurrency 0", pool("    concurrency: 0\n"), "pools.a.concurrency: must be >= 1"},
		{"poke_count > concurrency", pool("    concurrency: 2\n    poke_count: 3\n"), "pools.a.poke_count"},
		{"ok_exit_codes empty", pool("    ok_exit_codes: []\n"), "pools.a.ok_exit_codes: must not be empty"},
		{"ok_exit_codes range", pool("    ok_exit_codes: [0, 256]\n"), "pools.a.ok_exit_codes: codes must be 0..255"},
		{"exit_failure_threshold negative", pool("    exit_failure_threshold: -1\n"), "pools.a.exit_failure_threshold"},
		{"exit_failure_window 0", pool("    exit_failure_window: 0\n"), "pools.a.exit_failure_window: must be > 0"},
		{"exit_cooldown_multiplier < 1", pool("    exit_cooldown_multiplier: 0.5\n"), "pools.a.exit_cooldown_multiplier"},
		{"exit_cooldown_max < exit_cooldown", pool("    exit_cooldown: 10s\n    exit_cooldown_max: 5s\n"), "pools.a.exit_cooldown_max"},
		{"ttl negative", pool("    ttl: -1s\n"), "pools.a.ttl"},
		{"grace negative", pool("    grace_shutdown: -1s\n"), "pools.a.grace_shutdown"},
		{"duplicate channel", "pools:\n  a:\n    command: [x]\n  b:\n    channel: a\n    command: [x]\n", "pools.b.channel: \"a\" is already used by pools.a"},
		{"rule in _defaults", "pools:\n  _defaults:\n    concurrency: 0\n  a:\n    command: [x]\n", "pools.a.concurrency"},
		{"log.format", "log:\n  format: xml\n" + pool(""), "log.format"},
		{"log.child_output removed", "log:\n  child_output: discard\n" + pool(""), "log.child_output: removed, worker output is no longer forwarded; use pools._defaults.result_stream to pick the stream doorbell reads the task count from"},
		{"result_stream", pool("    result_stream: tee\n"), "pools.a.result_stream: must be stdout, stderr or none"},
		{"no source", "http:\n  enabled: false\nredis:\n  enabled: false\n" + pool(""), "at least one hint source"},
		{"http path no slash", "http:\n  hint_path: hint\n" + pool(""), "http.hint_path: must start with /"},
		{"http paths clash", "http:\n  hint_path: /x\n  health_path: /x\n" + pool(""), "http.health_path: \"/x\" is already used by http.hint_path"},
		{"http timeout negative", "http:\n  read_timeout: -1s\n" + pool(""), "http.read_timeout"},
		{"redis db negative", "redis:\n  db: -1\n" + pool(""), "redis.db"},
		{"redis reconnect order", "redis:\n  reconnect_min: 10s\n  reconnect_max: 1s\n" + pool(""), "redis.reconnect_max"},
		{"shutdown_timeout negative", "shutdown_timeout: -1s\n" + pool(""), "shutdown_timeout"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Parse(tc.src)
			if err != nil {
				t.Fatal(err)
			}
			err = cfg.Finalize()
			if err == nil {
				t.Fatalf("accepted; want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %q, want it to contain %q", err, tc.want)
			}
		})
	}
}

func TestValidateDisabledSourceSkipsItsRules(t *testing.T) {
	// http paths are irrelevant when http is off; redis addr irrelevant when redis is off.
	finalize(t, "http:\n  enabled: false\n  hint_path: nope\n"+minimal)
	finalize(t, "redis:\n  enabled: false\n  reconnect_min: 0\n"+minimal)
}

func TestValidateReportsAllErrors(t *testing.T) {
	cfg, err := Parse("log:\n  format: xml\npools:\n  a:\n    concurrency: 0\n    ttl: -1s\n")
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.Finalize()
	if err == nil {
		t.Fatal("accepted")
	}
	for _, want := range []string{"log.format", "pools.a.command", "pools.a.concurrency", "pools.a.ttl"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %q in:\n%v", want, err)
		}
	}
}

func TestPoolUnknownPanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no panic")
		}
	}()
	finalize(t, minimal).Pool("nope")
}

func samePool(a, b PoolConfig) bool {
	return fmt.Sprintf("%+v", a) == fmt.Sprintf("%+v", b)
}
