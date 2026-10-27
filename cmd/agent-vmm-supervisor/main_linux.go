//go:build linux

// The supervisor is the sandbox init. User processes are containerd exec processes.
package main

import (
	"os"
	"os/signal"
	"syscall"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, syscall.SIGINT, syscall.SIGCHLD)
	for event := range stop {
		if event != syscall.SIGCHLD {
			return
		}
		for {
			pid, err := syscall.Wait4(-1, nil, syscall.WNOHANG, nil)
			if err == syscall.EINTR {
				continue
			}
			if err != nil || pid == 0 {
				break
			}
		}
	}
}
