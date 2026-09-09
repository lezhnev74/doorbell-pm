// Package workertest builds the test worker binary for other test packages.
package workertest

import (
	"fmt"
	"os/exec"
	"path/filepath"
)

// Build compiles doorbell/test/worker into dir and returns the binary path.
// Run it once from TestMain; it takes about a second.
func Build(dir string) (string, error) {
	bin := filepath.Join(dir, "worker")
	cmd := exec.Command("go", "build", "-o", bin, "doorbell-pm/test/worker")
	if out, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build worker: %w\n%s", err, out)
	}
	return bin, nil
}
