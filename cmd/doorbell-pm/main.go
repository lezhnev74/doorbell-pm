// Command doorbell is a deliberately dumb process spawner: it listens for
// work notifications (doorbells) and spawns worker processes.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"doorbell-pm/internal/app"
	"doorbell-pm/internal/config"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the process: exit code 0 on a clean stop, 1 on a
// config or runtime error, 2 on bad flags.
func run(args []string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("doorbell-pm", flag.ContinueOnError)
	fs.SetOutput(stderr)
	path := fs.String("config", "", "path to the yaml config (required)")
	showVersion := fs.Bool("version", false, "print version and exit")
	check := fs.Bool("check", false, "load and validate the config, print it resolved, exit")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *showVersion {
		fmt.Fprintf(stdout, "doorbell-pm %s\n", version)
		return 0
	}
	if *path == "" {
		fmt.Fprintln(stderr, "doorbell-pm: -config is required")
		fs.Usage()
		return 2
	}

	cfg, err := config.Load(*path)
	if err == nil {
		err = cfg.Finalize()
	}
	if err != nil {
		fmt.Fprintf(stderr, "doorbell-pm: %v\n", err)
		return 1
	}
	if *check {
		if err := printResolved(stdout, cfg); err != nil {
			fmt.Fprintf(stderr, "doorbell-pm: %v\n", err)
			return 1
		}
		return 0
	}

	log := newLogger(cfg.Log, stderr)
	log.Info("doorbell starting", "version", version, "config", *path)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.New(cfg, version, log).Run(ctx); err != nil {
		log.Error("doorbell stopped with error", "err", err)
		return 1
	}
	log.Info("doorbell stopped")
	return 0
}
