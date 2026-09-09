package pool

import (
	"strconv"

	"doorbell-pm/internal/proc"
)

// parseTasks reads the task count a worker printed as its result line: a
// non-negative decimal integer of at most proc.MaxResult bytes. Anything
// else, including an empty line, is not a count.
func parseTasks(line string) (int, bool) {
	if line == "" || len(line) > proc.MaxResult {
		return 0, false
	}
	n, err := strconv.Atoi(line)
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
