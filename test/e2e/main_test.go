//go:build e2e

// Package e2e runs black-box scenarios against the built doorbell binary,
// a real redis and the test worker binary. Run with `make e2e`.
package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"doorbell-pm/test/worker/workertest"
)

var (
	doorbellBin string
	workerBin   string
	redisAddr   string
)

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	dir, err := os.MkdirTemp("", "doorbell-e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer os.RemoveAll(dir)

	doorbellBin = filepath.Join(dir, "doorbell")
	build := exec.Command("go", "build", "-o", doorbellBin, "doorbell-pm/cmd/doorbell-pm")
	if out, err := build.CombinedOutput(); err != nil {
		fmt.Fprintf(os.Stderr, "build doorbell: %v\n%s", err, out)
		return 1
	}
	if workerBin, err = workertest.Build(dir); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}

	stopRedis, err := startRedis(dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start redis:", err)
		return 1
	}
	defer stopRedis()

	return m.Run()
}
