package config

import (
	"fmt"
	"strings"
	"syscall"
	"time"
)

// Duration is a time.Duration that decodes from "30s"-style strings and
// from a bare 0.
type Duration time.Duration

// UnmarshalText implements encoding.TextUnmarshaler.
func (d *Duration) UnmarshalText(b []byte) error {
	v, err := time.ParseDuration(string(b))
	if err != nil {
		return fmt.Errorf("duration %q: %w", b, err)
	}
	*d = Duration(v)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (d Duration) MarshalText() ([]byte, error) { return []byte(d.String()), nil }

// Std returns the value as a time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// Signal is a unix signal that decodes from its name (SIGTERM, TERM, term).
type Signal syscall.Signal

var signalNames = map[string]syscall.Signal{
	"HUP":  syscall.SIGHUP,
	"INT":  syscall.SIGINT,
	"QUIT": syscall.SIGQUIT,
	"KILL": syscall.SIGKILL,
	"USR1": syscall.SIGUSR1,
	"USR2": syscall.SIGUSR2,
	"TERM": syscall.SIGTERM,
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (s *Signal) UnmarshalText(b []byte) error {
	name := strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(string(b))), "SIG")
	sig, ok := signalNames[name]
	if !ok {
		return fmt.Errorf("unknown signal %q", b)
	}
	*s = Signal(sig)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (s Signal) MarshalText() ([]byte, error) { return []byte(s.String()), nil }

// Std returns the value as a syscall.Signal.
func (s Signal) Std() syscall.Signal { return syscall.Signal(s) }

func (s Signal) String() string {
	for name, sig := range signalNames {
		if sig == syscall.Signal(s) {
			return "SIG" + name
		}
	}
	return fmt.Sprintf("signal(%d)", int(s))
}
