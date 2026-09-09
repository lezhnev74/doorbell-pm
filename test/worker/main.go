// Command worker is the child process used by the doorbell test suite.
//
// It appends observable lines to the file named by WORKER_LOG:
//
//	started <pid> <args...>   on start
//	exit <pid>                after WORKER_SLEEP elapsed (exits with WORKER_EXIT)
//	term <pid>                on SIGTERM (exits 0)
//	int <pid>                 on SIGINT (exits 0)
//
// WORKER_IGNORE_TERM=1 makes it ignore SIGTERM and SIGINT.
//
// WORKER_RESULT, when set, is printed as the final line right before the
// process exits (on every path above) on the stream named by
// WORKER_RESULT_STREAM: stdout (default) or stderr. Doorbell reads it as the
// number of tasks the worker processed.
package main

import (
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	os.Exit(run())
}

func run() int {
	pid := os.Getpid()
	sleep, _ := time.ParseDuration(os.Getenv("WORKER_SLEEP"))
	exitCode, _ := strconv.Atoi(os.Getenv("WORKER_EXIT"))

	logLine("started %d %s", pid, strings.Join(os.Args[1:], " "))

	sigs := make(chan os.Signal, 1)
	if os.Getenv("WORKER_IGNORE_TERM") == "1" {
		signal.Ignore(syscall.SIGTERM, syscall.SIGINT)
	} else {
		signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	}

	defer printResult()

	select {
	case <-time.After(sleep):
		logLine("exit %d", pid)
		return exitCode
	case sig := <-sigs:
		switch sig {
		case syscall.SIGINT:
			logLine("int %d", pid)
		default:
			logLine("term %d", pid)
		}
		return 0
	}
}

// printResult writes WORKER_RESULT as the last line on WORKER_RESULT_STREAM;
// it is a no-op when WORKER_RESULT is unset.
func printResult() {
	result, ok := os.LookupEnv("WORKER_RESULT")
	if !ok {
		return
	}
	w := os.Stdout
	if os.Getenv("WORKER_RESULT_STREAM") == "stderr" {
		w = os.Stderr
	}
	fmt.Fprintln(w, result)
}

// logLine appends one line to WORKER_LOG; it is a no-op when the variable is unset.
func logLine(format string, args ...any) {
	path := os.Getenv("WORKER_LOG")
	if path == "" {
		return
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		return
	}
	defer f.Close()
	fmt.Fprintf(f, format+"\n", args...)
}
