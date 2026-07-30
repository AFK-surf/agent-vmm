//go:build linux

package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guest/bootstrap"
	"github.com/AFK-surf/agent-vmm/internal/guest/control"
	"github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/AFK-surf/agent-vmm/internal/guest/vsock"
	"google.golang.org/grpc"
)

const vmmdPort = 4050

func main() {
	prepared, err := bootstrap.Prepare()
	if err != nil {
		log.Fatal(err)
	}
	networkManager, err := network.NewManager()
	if err != nil {
		log.Fatal(err)
	}
	defer networkManager.Close()
	containerd, err := startContainerd()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		_ = containerd.Process.Signal(syscall.SIGTERM)
		_ = containerd.Wait()
	}()
	runtime, err := control.NewRuntime(prepared.StateDevice, networkManager)
	if err != nil {
		log.Fatal(err)
	}
	defer runtime.Close()
	bootID, err := os.ReadFile("/proc/sys/kernel/random/uuid")
	if err != nil {
		log.Fatal(err)
	}
	listener, err := vsock.Listen(vmmdPort)
	if err != nil {
		log.Fatal(err)
	}
	defer listener.Close()
	grpcServer := control.NewGRPCServer()
	service := control.NewServer(runtime, control.HealthState{
		BootID: strings.TrimSpace(string(bootID)), ManifestDigest: prepared.ManifestDigest, RootReadOnly: prepared.RootReadOnly,
		Versions: map[string]string{
			"containerd": version("/usr/bin/containerd", "--version"),
			"runsc":      version("/usr/bin/runsc", "--version"),
			"shim":       version("/usr/bin/containerd-shim-runsc-v1", "-v"),
		},
	}, func(ctx context.Context) error {
		if err := networkManager.Close(); err != nil {
			return err
		}
		if err := runtime.Close(); err != nil {
			return err
		}
		if err := containerd.Process.Signal(syscall.SIGTERM); err != nil {
			return err
		}
		wait := make(chan error, 1)
		go func() { wait <- containerd.Wait() }()
		select {
		case err := <-wait:
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
		return bootstrap.UnmountState()
	})
	vmmdv1.RegisterVMMDServiceServer(grpcServer, service)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer shutdownCancel()
		_ = service.Shutdown(shutdownCtx)
		grpcServer.GracefulStop()
	}()
	if err := grpcServer.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		log.Fatal(err)
	}
}

func startContainerd() (*exec.Cmd, error) {
	if err := os.MkdirAll("/run/containerd", 0o755); err != nil {
		return nil, err
	}
	command := exec.Command("/usr/bin/containerd", "--config", "/etc/containerd/config.toml")
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		return nil, err
	}
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		if info, err := os.Stat("/run/containerd/containerd.sock"); err == nil && info.Mode()&os.ModeSocket != 0 {
			return command, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	_ = command.Process.Kill()
	_ = command.Wait()
	return nil, errors.New("containerd startup timeout")
}

func version(name string, argument string) string {
	output, err := exec.Command(name, argument).CombinedOutput()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}
