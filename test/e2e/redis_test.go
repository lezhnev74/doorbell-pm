//go:build e2e

package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// startRedis starts a real redis for the whole run and sets redisAddr.
// It prefers a local `redis-server` and falls back to the docker CLI, so the
// suite needs no Go dependency on either.
func startRedis(dir string) (stop func(), err error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	redisAddr = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	var cmd *exec.Cmd
	var kill func()
	switch {
	case lookPath("redis-server"):
		cmd = exec.Command("redis-server", "--port", strconv.Itoa(port), "--bind", "127.0.0.1",
			"--save", "", "--appendonly", "no", "--dir", dir)
		kill = func() { _ = cmd.Process.Kill() }
	case lookPath("docker"):
		// Killing the docker client leaves the container running, so stop it by name.
		name := fmt.Sprintf("doorbell-e2e-%d-%d", os.Getpid(), port)
		cmd = exec.Command("docker", "run", "--rm", "--name", name, "-p", redisAddr+":6379", "redis:7-alpine")
		kill = func() { _ = exec.Command("docker", "rm", "-f", name).Run() }
	default:
		return nil, errors.New("neither redis-server nor docker found in PATH")
	}
	cmd.Stdout, cmd.Stderr = nil, os.Stderr
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	stop = func() {
		kill()
		_ = cmd.Wait()
	}

	client := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for {
		if err := client.Ping(ctx).Err(); err == nil {
			return stop, nil
		}
		select {
		case <-ctx.Done():
			stop()
			return nil, fmt.Errorf("redis at %s did not answer PING in time", redisAddr)
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func lookPath(name string) bool {
	_, err := exec.LookPath(name)
	return err == nil
}

// freePort asks the kernel for an unused tcp port on loopback.
func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
