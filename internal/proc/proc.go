// Package proc abstracts child process control.
//
// Pool logic depends only on Spawner and Process; the os/exec implementation
// and the test fake (proctest) are adapters.
package proc

import (
	"context"
	"os"
)

// Stream names a child output stream. Values match the result_stream config
// key; the zero value reads nothing, like None.
type Stream string

const (
	Stdout Stream = "stdout"
	Stderr Stream = "stderr"
	None   Stream = "none"
)

// Spec describes what to start. Env is added on top of the parent environment
// when InheritEnv is set, otherwise it is the whole environment. ResultStream
// is the stream whose last line lands in Exit.Result; both streams otherwise
// go to /dev/null.
type Spec struct {
	Cmd          []string
	Dir          string
	Env          map[string]string
	InheritEnv   bool
	ResultStream Stream
}

// Exit is how a process ended. Signal is set when the process was killed by a
// signal, Err when waiting on it failed for a reason other than its exit
// status. Result is the last non-empty line the child wrote to the
// Spec.ResultStream, trimmed; empty when there was none, the stream is not
// read, or that line exceeded MaxResult bytes.
type Exit struct {
	Code   int
	Signal os.Signal
	Err    error
	Result string
}

// Process is a started child.
type Process interface {
	PID() int
	// Signal delivers sig to the process itself.
	Signal(os.Signal) error
	// KillGroup sends SIGKILL to the whole process group, so children the
	// process forked die with it.
	KillGroup() error
	// Done yields exactly one Exit and is then closed.
	Done() <-chan Exit
}

// Spawner starts processes.
type Spawner interface {
	Spawn(context.Context, Spec) (Process, error)
}
