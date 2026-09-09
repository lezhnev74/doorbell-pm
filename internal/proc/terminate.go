package proc

import (
	"errors"
	"os"
	"syscall"
	"time"
)

// Terminate stops p: it sends sig (SIGTERM when nil), waits up to grace for
// the process to exit, then kills its whole process group. It returns the
// Exit read from p.Done, so the caller must not read Done itself.
func Terminate(p Process, sig os.Signal, grace time.Duration) Exit {
	if sig == nil {
		sig = syscall.SIGTERM
	}
	if err := p.Signal(sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
		return kill(p)
	}
	select {
	case exit := <-p.Done():
		return exit
	case <-time.After(grace):
		return kill(p)
	}
}

// kill ends the group and waits for the leader to be reaped. Done is
// guaranteed to fire: SIGKILL cannot be caught or ignored.
func kill(p Process) Exit {
	_ = p.KillGroup()
	return <-p.Done()
}
