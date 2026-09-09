package config

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"syscall"
	"time"
)

// PoolConfig is one pool with every knob resolved through the three layers:
// built-in default -> pools._defaults -> pool block. No pointers: this is
// what the rest of the program works with.
type PoolConfig struct {
	Name                   string
	Enabled                bool
	Channel                string
	Command                []string
	Concurrency            int
	OkExitCodes            []int
	ExitFailureThreshold   int
	ExitFailureWindow      time.Duration
	ExitCooldown           time.Duration
	ExitCooldownMax        time.Duration
	ExitCooldownMultiplier float64
	TTL                    time.Duration
	Poke                   time.Duration
	PokeCount              int
	GraceShutdown          time.Duration
	TermSignal             syscall.Signal
	InheritEnv             bool
	Env                    map[string]string
	Dir                    string
	ResultStream           string
}

// builtinPool is the bottom layer of pool settings.
var builtinPool = PoolConfig{
	Enabled:                true,
	Concurrency:            1,
	OkExitCodes:            []int{0},
	ExitFailureThreshold:   3,
	ExitFailureWindow:      10 * time.Second,
	ExitCooldown:           5 * time.Second,
	ExitCooldownMax:        5 * time.Minute,
	ExitCooldownMultiplier: 2,
	PokeCount:              1,
	GraceShutdown:          30 * time.Second,
	TermSignal:             syscall.SIGTERM,
	InheritEnv:             true,
	ResultStream:           "stdout",
}

// Built-in defaults for the top-level blocks.
const (
	DefaultLogFormat          = "text"
	DefaultLogTimestampFormat = "2006-01-02T15:04:05.000Z07:00"
	DefaultHTTPAddr           = "127.0.0.1:8080"
	DefaultHintPath           = "/hint"
	DefaultHealthPath         = "/healthz"
	DefaultMetricsPath        = "/metrics"
	DefaultHTTPTimeout        = 5 * time.Second
	DefaultRedisAddr          = "127.0.0.1:6379"
	DefaultRedisChannelPrefix = "jobs:"
	DefaultRedisDialTimeout   = 5 * time.Second
	DefaultRedisReconnectMin  = 500 * time.Millisecond
	DefaultRedisReconnectMax  = 30 * time.Second
	DefaultMetricsNamespace   = "doorbell"
	DefaultShutdownTimeout    = 60 * time.Second
)

// IsEnabled reports whether the http listener is on (default true).
func (h HTTP) IsEnabled() bool { return h.Enabled == nil || *h.Enabled }

// IsEnabled reports whether the redis source is on (default true).
func (r Redis) IsEnabled() bool { return r.Enabled == nil || *r.Enabled }

// IsEnabled reports whether the metrics exporter is on (default true).
func (m Metrics) IsEnabled() bool { return m.Enabled == nil || *m.Enabled }

// Finalize fills built-in defaults into the top-level blocks and validates
// the whole document. Call it once after Load; it returns every violation
// found, joined, so a broken file can be fixed in one pass.
func (c *Config) Finalize() error {
	c.applyDefaults()
	return c.validate()
}

// applyDefaults fills zero-valued top-level keys. Pool-level defaults are
// applied by Pool()/ResolvedPools() so an explicit zero in a pool block wins.
func (c *Config) applyDefaults() {
	setStr(&c.Log.Format, DefaultLogFormat)
	setStr(&c.Log.TimestampFormat, DefaultLogTimestampFormat)

	setBool(&c.HTTP.Enabled, true)
	setStr(&c.HTTP.Addr, DefaultHTTPAddr)
	setStr(&c.HTTP.HintPath, DefaultHintPath)
	setStr(&c.HTTP.HealthPath, DefaultHealthPath)
	setStr(&c.HTTP.MetricsPath, DefaultMetricsPath)
	setDur(&c.HTTP.ReadTimeout, DefaultHTTPTimeout)
	setDur(&c.HTTP.WriteTimeout, DefaultHTTPTimeout)
	setDur(&c.HTTP.ShutdownTimeout, DefaultHTTPTimeout)

	setBool(&c.Redis.Enabled, true)
	setStr(&c.Redis.Addr, DefaultRedisAddr)
	setStr(&c.Redis.ChannelPrefix, DefaultRedisChannelPrefix)
	setDur(&c.Redis.DialTimeout, DefaultRedisDialTimeout)
	setDur(&c.Redis.ReconnectMin, DefaultRedisReconnectMin)
	setDur(&c.Redis.ReconnectMax, DefaultRedisReconnectMax)

	setBool(&c.Metrics.Enabled, true)
	setStr(&c.Metrics.Namespace, DefaultMetricsNamespace)

	setDur(&c.ShutdownTimeout, DefaultShutdownTimeout)
}

func setStr(p *string, def string) {
	if *p == "" {
		*p = def
	}
}

func setDur(p *Duration, def time.Duration) {
	if *p == 0 {
		*p = Duration(def)
	}
}

func setBool(p **bool, def bool) {
	if *p == nil {
		v := def
		*p = &v
	}
}

// Pool resolves pools.<name> through built-in -> pools._defaults -> pool.
// It panics on an unknown name; check c.Pools first.
func (c Config) Pool(name string) PoolConfig {
	p, ok := c.Pools[name]
	if !ok {
		panic("config: unknown pool " + name)
	}
	d := c.Defaults
	r := builtinPool
	r.Name = name
	r.Channel = p.Channel
	if r.Channel == "" {
		r.Channel = name
	}
	r.Command = append([]string(nil), p.Command...)
	r.Enabled = pick(r.Enabled, d.Enabled, p.Enabled)
	r.Concurrency = pick(r.Concurrency, d.Concurrency, p.Concurrency)
	r.OkExitCodes = pickSlice(r.OkExitCodes, d.OkExitCodes, p.OkExitCodes)
	r.ExitFailureThreshold = pick(r.ExitFailureThreshold, d.ExitFailureThreshold, p.ExitFailureThreshold)
	r.ExitFailureWindow = pickDur(r.ExitFailureWindow, d.ExitFailureWindow, p.ExitFailureWindow)
	r.ExitCooldown = pickDur(r.ExitCooldown, d.ExitCooldown, p.ExitCooldown)
	r.ExitCooldownMax = pickDur(r.ExitCooldownMax, d.ExitCooldownMax, p.ExitCooldownMax)
	r.ExitCooldownMultiplier = pick(r.ExitCooldownMultiplier, d.ExitCooldownMultiplier, p.ExitCooldownMultiplier)
	r.TTL = pickDur(r.TTL, d.TTL, p.TTL)
	r.Poke = pickDur(r.Poke, d.Poke, p.Poke)
	r.PokeCount = pick(r.PokeCount, d.PokeCount, p.PokeCount)
	r.GraceShutdown = pickDur(r.GraceShutdown, d.GraceShutdown, p.GraceShutdown)
	if s := pick(Signal(r.TermSignal), d.TermSignal, p.TermSignal); s != 0 {
		r.TermSignal = s.Std()
	}
	r.InheritEnv = pick(r.InheritEnv, d.InheritEnv, p.InheritEnv)
	r.Env = mergeEnv(d.Env, p.Env)
	r.Dir = pick(r.Dir, d.Dir, p.Dir)
	r.ResultStream = pick(r.ResultStream, d.ResultStream, p.ResultStream)
	return r
}

// ResolvedPools resolves every pool, sorted by name.
func (c Config) ResolvedPools() []PoolConfig {
	names := make([]string, 0, len(c.Pools))
	for n := range c.Pools {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]PoolConfig, 0, len(names))
	for _, n := range names {
		out = append(out, c.Pool(n))
	}
	return out
}

// pick returns the last non-nil layer, or base.
func pick[T any](base T, layers ...*T) T {
	for _, l := range layers {
		if l != nil {
			base = *l
		}
	}
	return base
}

func pickDur(base time.Duration, layers ...*Duration) time.Duration {
	for _, l := range layers {
		if l != nil {
			base = l.Std()
		}
	}
	return base
}

// pickSlice returns a copy of the last non-nil layer, or base. An explicit
// empty list ([]) counts as set.
func pickSlice[T any](base []T, layers ...[]T) []T {
	for _, l := range layers {
		if l != nil {
			base = l
		}
	}
	return append([]T{}, base...)
}

// mergeEnv layers env maps: later keys override earlier ones.
func mergeEnv(layers ...map[string]string) map[string]string {
	out := map[string]string{}
	for _, l := range layers {
		for k, v := range l {
			out[k] = v
		}
	}
	return out
}

// badFn records one validation error under a config key.
type badFn func(key, format string, args ...any)

// validate checks the document after applyDefaults.
func (c Config) validate() error {
	var errs []error
	bad := func(key, format string, args ...any) {
		errs = append(errs, fmt.Errorf("%s: %s", key, fmt.Sprintf(format, args...)))
	}
	c.validateLog(bad)
	c.validateSources(bad)
	if c.Metrics.IsEnabled() && c.Metrics.Namespace == "" {
		bad("metrics.namespace", "must not be empty")
	}
	if c.ShutdownTimeout <= 0 {
		bad("shutdown_timeout", "must be > 0, got %s", c.ShutdownTimeout)
	}
	c.validatePools(bad)
	return errors.Join(errs...)
}

func (c Config) validateLog(bad badFn) {
	switch c.Log.Format {
	case "text", "json":
	default:
		bad("log.format", "must be text or json, got %q", c.Log.Format)
	}
	if c.Log.ChildOutput != "" {
		bad("log.child_output", "removed, worker output is no longer forwarded; "+
			"use pools._defaults.result_stream to pick the stream doorbell reads the task count from")
	}
	if c.Log.TimestampFormat == "" {
		bad("log.timestamp_format", "must not be empty")
	}
}

func (c Config) validateSources(bad badFn) {
	if !c.HTTP.IsEnabled() && !c.Redis.IsEnabled() {
		bad("http.enabled/redis.enabled", "at least one hint source must be enabled")
	}
	if c.HTTP.IsEnabled() {
		c.validateHTTP(bad)
	}
	if c.Redis.IsEnabled() {
		c.validateRedis(bad)
	}
}

func (c Config) validateHTTP(bad badFn) {
	if c.HTTP.Addr == "" {
		bad("http.addr", "required when http is enabled")
	}
	c.validateHTTPPaths(bad)
	for key, d := range map[string]Duration{
		"http.read_timeout":     c.HTTP.ReadTimeout,
		"http.write_timeout":    c.HTTP.WriteTimeout,
		"http.shutdown_timeout": c.HTTP.ShutdownTimeout,
	} {
		if d < 0 {
			bad(key, "must be >= 0, got %s", d)
		}
	}
}

func (c Config) validateHTTPPaths(bad badFn) {
	paths := map[string]string{
		"http.hint_path":    c.HTTP.HintPath,
		"http.health_path":  c.HTTP.HealthPath,
		"http.metrics_path": c.HTTP.MetricsPath,
	}
	seen := map[string]string{}
	for _, key := range []string{"http.hint_path", "http.health_path", "http.metrics_path"} {
		p := paths[key]
		if !strings.HasPrefix(p, "/") {
			bad(key, "must start with /, got %q", p)
		}
		if other, dup := seen[p]; dup {
			bad(key, "%q is already used by %s", p, other)
		}
		seen[p] = key
	}
}

func (c Config) validateRedis(bad badFn) {
	if c.Redis.Addr == "" {
		bad("redis.addr", "required when redis is enabled")
	}
	if c.Redis.DB < 0 {
		bad("redis.db", "must be >= 0, got %d", c.Redis.DB)
	}
	if c.Redis.DialTimeout < 0 {
		bad("redis.dial_timeout", "must be >= 0, got %s", c.Redis.DialTimeout)
	}
	c.validateRedisBackoff(bad)
}

func (c Config) validateRedisBackoff(bad badFn) {
	if c.Redis.ReconnectMin <= 0 {
		bad("redis.reconnect_min", "must be > 0, got %s", c.Redis.ReconnectMin)
	}
	if c.Redis.ReconnectMax < c.Redis.ReconnectMin {
		bad("redis.reconnect_max", "must be >= reconnect_min (%s), got %s", c.Redis.ReconnectMin, c.Redis.ReconnectMax)
	}
}

func (c Config) validatePools(bad badFn) {
	channels := map[string]string{}
	for _, p := range c.ResolvedPools() {
		key := "pools." + p.Name
		if owner, dup := channels[p.Channel]; dup {
			bad(key+".channel", "%q is already used by %s", p.Channel, owner)
		}
		channels[p.Channel] = key
		for _, e := range p.validate() {
			bad(key+"."+e.key, "%s", e.msg)
		}
	}
	if c.enabledPools() == 0 {
		bad("pools", "at least one enabled pool is required")
	}
}

func (c Config) enabledPools() int {
	n := 0
	for _, p := range c.ResolvedPools() {
		if p.Enabled {
			n++
		}
	}
	return n
}

type poolErr struct{ key, msg string }

// validate checks one resolved pool. Keys are relative to the pool block.
func (p PoolConfig) validate() []poolErr {
	var errs []poolErr
	bad := func(key, format string, args ...any) {
		errs = append(errs, poolErr{key, fmt.Sprintf(format, args...)})
	}
	p.validateSpawn(bad)
	p.validateExitCodes(bad)
	p.validateBreaker(bad)
	p.validateCooldown(bad)
	p.validateProcess(bad)
	return errs
}

func (p PoolConfig) validateSpawn(bad badFn) {
	if len(p.Command) == 0 {
		bad("command", "must not be empty")
	}
	if p.Concurrency < 1 {
		bad("concurrency", "must be >= 1, got %d", p.Concurrency)
	}
	if p.PokeCount < 0 || p.PokeCount > p.Concurrency {
		bad("poke_count", "must be 0..concurrency (%d), got %d", p.Concurrency, p.PokeCount)
	}
}

func (p PoolConfig) validateExitCodes(bad badFn) {
	if len(p.OkExitCodes) == 0 {
		bad("ok_exit_codes", "must not be empty")
	}
	for _, code := range p.OkExitCodes {
		if code < 0 || code > 255 {
			bad("ok_exit_codes", "codes must be 0..255, got %d", code)
		}
	}
}

func (p PoolConfig) validateBreaker(bad badFn) {
	if p.ExitFailureThreshold < 0 {
		bad("exit_failure_threshold", "must be >= 0, got %d", p.ExitFailureThreshold)
	}
	if p.ExitFailureWindow <= 0 {
		bad("exit_failure_window", "must be > 0, got %s", p.ExitFailureWindow)
	}
}

func (p PoolConfig) validateCooldown(bad badFn) {
	if p.ExitCooldown < 0 {
		bad("exit_cooldown", "must be >= 0, got %s", p.ExitCooldown)
	}
	if p.ExitCooldownMax < p.ExitCooldown {
		bad("exit_cooldown_max", "must be >= exit_cooldown (%s), got %s", p.ExitCooldown, p.ExitCooldownMax)
	}
	if p.ExitCooldownMultiplier < 1 {
		bad("exit_cooldown_multiplier", "must be >= 1, got %g", p.ExitCooldownMultiplier)
	}
}

func (p PoolConfig) validateProcess(bad badFn) {
	for key, d := range map[string]time.Duration{"ttl": p.TTL, "poke": p.Poke, "grace_shutdown": p.GraceShutdown} {
		if d < 0 {
			bad(key, "must be >= 0, got %s", d)
		}
	}
	if p.TermSignal == 0 {
		bad("term_signal", "must be a known signal")
	}
	switch p.ResultStream {
	case "stdout", "stderr", "none":
	default:
		bad("result_stream", "must be stdout, stderr or none, got %q", p.ResultStream)
	}
}
