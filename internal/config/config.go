// Package config loads and describes the doorbell yaml configuration.
//
// Load reads a file, expands ${ENV_VAR} references, and decodes it strictly:
// unknown keys are an error. Pool-level knobs are pointers so that an explicit
// zero (ttl: 0 = forever) is distinguishable from "not set"; resolving the
// three layers (built-in -> pools._defaults -> pool) into flat values is a
// separate step.
//
// The pools._defaults entry is reserved: it is not a pool but the shared
// settings every pool starts from. Names starting with "_" are reserved for
// such meta entries and are never valid pool names.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"regexp"

	"github.com/goccy/go-yaml"
)

// Config is the whole yaml document.
type Config struct {
	Log             Log             `yaml:"log"`
	HTTP            HTTP            `yaml:"http"`
	Redis           Redis           `yaml:"redis"`
	Metrics         Metrics         `yaml:"metrics"`
	ShutdownTimeout Duration        `yaml:"shutdown_timeout"`
	Pools           map[string]Pool `yaml:"pools"`
	Defaults        PoolSettings    `yaml:"-"` // pools._defaults, split out by Parse
}

// DefaultsKey is the reserved pools entry holding shared pool settings.
const DefaultsKey = "_defaults"

// poolName is what a pool may be called: it becomes a redis channel suffix
// and a metrics label. Leading "_" is reserved for meta entries.
var poolName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]*$`)

// Log configures slog output.
type Log struct {
	Format          string     `yaml:"format"`           // text | json
	Level           slog.Level `yaml:"level"`            // debug | info | warn | error
	TimestampFormat string     `yaml:"timestamp_format"` // Go layout or "unix"
	// ChildOutput is a tombstone: the key is still parsed so strict decoding
	// accepts old files, and validate rejects it with a migration hint.
	ChildOutput string `yaml:"child_output,omitempty"`
}

// HTTP configures the hint / health / metrics listener.
type HTTP struct {
	Enabled         *bool    `yaml:"enabled"`
	Addr            string   `yaml:"addr"`
	HintPath        string   `yaml:"hint_path"`
	HealthPath      string   `yaml:"health_path"`
	MetricsPath     string   `yaml:"metrics_path"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

// Redis configures the pub/sub hint source.
type Redis struct {
	Enabled       *bool    `yaml:"enabled"`
	Addr          string   `yaml:"addr"`
	Username      string   `yaml:"username"`
	Password      string   `yaml:"password"`
	DB            int      `yaml:"db"`
	TLS           bool     `yaml:"tls"`
	ChannelPrefix string   `yaml:"channel_prefix"`
	DialTimeout   Duration `yaml:"dial_timeout"`
	ReconnectMin  Duration `yaml:"reconnect_min"`
	ReconnectMax  Duration `yaml:"reconnect_max"`
}

// Metrics configures the prometheus exporter.
type Metrics struct {
	Enabled   *bool  `yaml:"enabled"`
	Namespace string `yaml:"namespace"`
	Addr      string `yaml:"addr"` // "" = serve on the main http listener
}

// PoolSettings holds every per-pool knob. It is used both for the defaults:
// block and, inlined, for each pool. Nil means "not set at this layer".
type PoolSettings struct {
	Enabled                *bool             `yaml:"enabled"`
	Concurrency            *int              `yaml:"concurrency"`
	OkExitCodes            []int             `yaml:"ok_exit_codes"`
	ExitFailureThreshold   *int              `yaml:"exit_failure_threshold"`
	ExitFailureWindow      *Duration         `yaml:"exit_failure_window"`
	ExitCooldown           *Duration         `yaml:"exit_cooldown"`
	ExitCooldownMax        *Duration         `yaml:"exit_cooldown_max"`
	ExitCooldownMultiplier *float64          `yaml:"exit_cooldown_multiplier"`
	TTL                    *Duration         `yaml:"ttl"`
	Poke                   *Duration         `yaml:"poke"`
	PokeCount              *int              `yaml:"poke_count"`
	GraceShutdown          *Duration         `yaml:"grace_shutdown"`
	TermSignal             *Signal           `yaml:"term_signal"`
	InheritEnv             *bool             `yaml:"inherit_env"`
	Env                    map[string]string `yaml:"env"`
	Dir                    *string           `yaml:"dir"`
	ResultStream           *string           `yaml:"result_stream"` // stdout | stderr | none
}

// Pool is one pools.<name> block.
type Pool struct {
	PoolSettings `yaml:",inline"`
	Channel      string   `yaml:"channel"` // hint channel suffix; default = pool name
	Command      []string `yaml:"command"`
}

// Load reads path, expands ${ENV_VAR} references and parses it strictly.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	cfg, err := Parse(os.Expand(string(raw), os.Getenv))
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

// Parse decodes yaml source. Unknown keys are an error.
func Parse(src string) (Config, error) {
	var cfg Config
	if err := yaml.UnmarshalWithOptions([]byte(src), &cfg, yaml.Strict()); err != nil {
		return Config{}, err
	}
	if err := cfg.splitDefaults(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// splitDefaults moves pools._defaults into Defaults and rejects names that
// are reserved or unusable as channel suffixes.
func (c *Config) splitDefaults() error {
	if d, ok := c.Pools[DefaultsKey]; ok {
		if len(d.Command) > 0 || d.Channel != "" {
			return fmt.Errorf("pools.%s: command and channel are per-pool keys", DefaultsKey)
		}
		c.Defaults = d.PoolSettings
		delete(c.Pools, DefaultsKey)
	}
	for name := range c.Pools {
		if !poolName.MatchString(name) {
			return fmt.Errorf("pools.%s: invalid pool name (want %s; names starting with _ are reserved)", name, poolName)
		}
	}
	return nil
}
