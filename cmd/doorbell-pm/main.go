// Command doorbell is a deliberately dumb process spawner: it listens for
// work notifications (doorbells) and spawns worker processes.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/urfave/cli/v3"

	"doorbell-pm/internal/app"
	"doorbell-pm/internal/config"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run is main without the process: exit code 0 on a clean stop, 1 on a
// config or runtime error, 2 on bad flags or usage.
//
// Actions return a plain error for code 1 (printed here as
// "doorbell-pm: <err>") or a cli.ExitCoder with an empty message when the
// diagnostic was already written (usage errors, logged runtime errors).
func run(args []string, stdout, stderr io.Writer) int {
	err := newCommand(stdout, stderr).Run(context.Background(), append([]string{"doorbell-pm"}, args...))
	if err == nil {
		return 0
	}
	code := 1
	var ec cli.ExitCoder
	if errors.As(err, &ec) {
		code = ec.ExitCode()
	}
	if msg := err.Error(); msg != "" {
		fmt.Fprintf(stderr, "doorbell-pm: %s\n", msg)
	}
	return code
}

// newCommand builds the verb tree: run, check, version.
func newCommand(stdout, stderr io.Writer) *cli.Command {
	configFlag := func() *cli.StringFlag {
		return &cli.StringFlag{Name: "config", Aliases: []string{"c"}, Usage: "path to the yaml config (required)"}
	}
	root := &cli.Command{
		Name:            "doorbell-pm",
		Usage:           "spawn worker processes on redis doorbells",
		Writer:          stdout,
		ErrWriter:       stderr,
		HideVersion:     true,
		HideHelpCommand: true,
		// Keep os.Exit out of the library: run maps errors to exit codes.
		ExitErrHandler: func(context.Context, *cli.Command, error) {},
		OnUsageError:   onUsageError,
		Action: func(_ context.Context, cmd *cli.Command) error {
			if cmd.NArg() > 0 {
				return usageError(cmd, fmt.Errorf("unknown command %q", cmd.Args().First()))
			}
			return usageError(cmd, nil)
		},
		Commands: []*cli.Command{
			{
				Name:         "run",
				Usage:        "start the process manager",
				Flags:        []cli.Flag{configFlag()},
				OnUsageError: onUsageError,
				Action: func(_ context.Context, cmd *cli.Command) error {
					cfg, err := loadConfig(cmd)
					if err != nil {
						return err
					}
					return runApp(cfg, cmd.String("config"), stderr)
				},
			},
			{
				Name:         "check",
				Usage:        "load and validate the config, print it resolved, exit",
				Flags:        []cli.Flag{configFlag()},
				OnUsageError: onUsageError,
				Action: func(_ context.Context, cmd *cli.Command) error {
					cfg, err := loadConfig(cmd)
					if err != nil {
						return err
					}
					return printResolved(stdout, cfg)
				},
			},
			{
				Name:         "version",
				Usage:        "print version and exit",
				OnUsageError: onUsageError,
				Action: func(context.Context, *cli.Command) error {
					_, err := fmt.Fprintf(stdout, "doorbell-pm %s\n", version)
					return err
				},
			},
		},
	}
	return root
}

// onUsageError adapts usageError to the library's OnUsageError hook.
func onUsageError(_ context.Context, cmd *cli.Command, err error, _ bool) error {
	return usageError(cmd, err)
}

// usageError reports a bad invocation: "doorbell-pm: <err>" (when err is
// non-nil) plus the command's help on stderr, exit code 2.
func usageError(cmd *cli.Command, err error) error {
	stderr := cmd.Root().ErrWriter
	if err != nil {
		fmt.Fprintf(stderr, "doorbell-pm: %v\n", err)
	}
	tmpl := cli.CommandHelpTemplate
	if cmd.Root() == cmd {
		tmpl = cli.RootCommandHelpTemplate
	}
	cli.DefaultPrintHelp(stderr, tmpl, cmd)
	return cli.Exit("", 2)
}

// loadConfig reads and finalizes the config named by --config, or reports a
// usage error when the flag is missing.
func loadConfig(cmd *cli.Command) (config.Config, error) {
	path := cmd.String("config")
	if path == "" {
		return config.Config{}, usageError(cmd, errors.New("--config is required"))
	}
	cfg, err := config.Load(path)
	if err == nil {
		err = cfg.Finalize()
	}
	return cfg, err
}

// runApp runs the daemon until SIGINT/SIGTERM. Runtime failures go through
// the structured logger, so the returned error carries no message.
func runApp(cfg config.Config, path string, stderr io.Writer) error {
	log := newLogger(cfg.Log, stderr)
	log.Info("doorbell starting", "version", version, "pid", os.Getpid(), "config", path)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := app.New(cfg, version, log).Run(ctx); err != nil {
		log.Error("doorbell stopped with error", "err", err)
		return cli.Exit("", 1)
	}
	log.Info("doorbell stopped")
	return nil
}
