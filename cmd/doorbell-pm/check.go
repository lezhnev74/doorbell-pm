package main

import (
	"fmt"
	"io"
	"sort"

	"github.com/goccy/go-yaml"

	"doorbell-pm/internal/config"
)

// resolved is the -check output: every top-level block with defaults
// applied and every pool flattened through the three layers, in yaml so it
// can be diffed against the input file.
type resolved struct {
	Log             config.Log      `yaml:"log"`
	HTTP            config.HTTP     `yaml:"http"`
	Redis           config.Redis    `yaml:"redis"`
	Metrics         config.Metrics  `yaml:"metrics"`
	ShutdownTimeout config.Duration `yaml:"shutdown_timeout"`
	Pools           yaml.MapSlice   `yaml:"pools"`
}

// resolvedPool mirrors config.PoolConfig with yaml-friendly duration and
// signal types.
type resolvedPool struct {
	Enabled                bool              `yaml:"enabled"`
	Channel                string            `yaml:"channel"`
	Command                []string          `yaml:"command"`
	Concurrency            int               `yaml:"concurrency"`
	OkExitCodes            []int             `yaml:"ok_exit_codes"`
	ExitFailureThreshold   int               `yaml:"exit_failure_threshold"`
	ExitFailureWindow      config.Duration   `yaml:"exit_failure_window"`
	ExitCooldown           config.Duration   `yaml:"exit_cooldown"`
	ExitCooldownMax        config.Duration   `yaml:"exit_cooldown_max"`
	ExitCooldownMultiplier float64           `yaml:"exit_cooldown_multiplier"`
	TTL                    config.Duration   `yaml:"ttl"`
	Poke                   config.Duration   `yaml:"poke"`
	PokeCount              int               `yaml:"poke_count"`
	GraceShutdown          config.Duration   `yaml:"grace_shutdown"`
	TermSignal             config.Signal     `yaml:"term_signal"`
	InheritEnv             bool              `yaml:"inherit_env"`
	Env                    map[string]string `yaml:"env"`
	Dir                    string            `yaml:"dir"`
	ResultStream           string            `yaml:"result_stream"`
}

// printResolved writes cfg as yaml with secrets masked.
func printResolved(w io.Writer, cfg config.Config) error {
	r := resolved{
		Log:             cfg.Log,
		HTTP:            cfg.HTTP,
		Redis:           cfg.Redis,
		Metrics:         cfg.Metrics,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}
	if r.Redis.Password != "" {
		r.Redis.Password = "***"
	}
	for _, p := range cfg.ResolvedPools() {
		r.Pools = append(r.Pools, yaml.MapItem{Key: p.Name, Value: toResolvedPool(p)})
	}
	out, err := yaml.Marshal(r)
	if err != nil {
		return fmt.Errorf("render config: %w", err)
	}
	_, err = w.Write(out)
	return err
}

func toResolvedPool(p config.PoolConfig) resolvedPool {
	env := p.Env
	if env == nil {
		env = map[string]string{}
	}
	codes := append([]int(nil), p.OkExitCodes...)
	sort.Ints(codes)
	return resolvedPool{
		Enabled:                p.Enabled,
		Channel:                p.Channel,
		Command:                p.Command,
		Concurrency:            p.Concurrency,
		OkExitCodes:            codes,
		ExitFailureThreshold:   p.ExitFailureThreshold,
		ExitFailureWindow:      config.Duration(p.ExitFailureWindow),
		ExitCooldown:           config.Duration(p.ExitCooldown),
		ExitCooldownMax:        config.Duration(p.ExitCooldownMax),
		ExitCooldownMultiplier: p.ExitCooldownMultiplier,
		TTL:                    config.Duration(p.TTL),
		Poke:                   config.Duration(p.Poke),
		PokeCount:              p.PokeCount,
		GraceShutdown:          config.Duration(p.GraceShutdown),
		TermSignal:             config.Signal(p.TermSignal),
		InheritEnv:             p.InheritEnv,
		Env:                    env,
		Dir:                    p.Dir,
		ResultStream:           p.ResultStream,
	}
}
