//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guest/bootstrap"
	"github.com/AFK-surf/agent-vmm/internal/guest/control"
	"github.com/AFK-surf/agent-vmm/internal/guest/egress"
	"github.com/AFK-surf/agent-vmm/internal/guest/network"
	"github.com/AFK-surf/agent-vmm/internal/guest/vsock"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
)

const (
	vmmdPort              = 4050
	containerdLauncherArg = "__agent_vmm_containerd"
)

func main() {
	if len(os.Args) == 2 && os.Args[1] == containerdLauncherArg {
		log.Fatal(execContainerdWithoutQuotaBypass())
	}
	log.Printf("guest startup stage=bootstrap")
	prepared, err := bootstrap.Prepare()
	if err != nil {
		log.Fatalf("guest startup stage=bootstrap error=%.4096s", err.Error())
	}
	log.Printf("guest startup stage=oci_storage")
	startup, err := control.PrepareOCIStorage(prepared.StateDevice)
	if err != nil {
		log.Fatalf("guest startup stage=oci_storage error=%.4096s", err.Error())
	}
	bootID, err := os.ReadFile("/proc/sys/kernel/random/uuid")
	if err != nil {
		log.Fatal(err)
	}
	pump, err := egress.New(strings.TrimSpace(string(bootID)))
	if err != nil {
		log.Fatal(err)
	}
	pumpContext, stopPump := context.WithCancel(context.Background())
	defer stopPump()
	defer pump.Close()
	go func() {
		if err := pump.Run(pumpContext); err != nil {
			log.Printf("egress pump stopped: %v", err)
		}
	}()
	networkManager, err := network.NewManager()
	if err != nil {
		log.Fatal(err)
	}
	defer networkManager.Close()
	log.Printf("guest startup stage=containerd")
	containerd, err := startContainerd()
	if err != nil {
		log.Fatalf("guest startup stage=containerd error=%.4096s", err.Error())
	}
	defer func() {
		_ = containerd.Process.Signal(syscall.SIGTERM)
		_ = containerd.Wait()
	}()
	log.Printf("guest startup stage=recovery")
	runtime, err := control.NewRuntime(prepared.StateDevice, networkManager, startup)
	if err != nil {
		log.Fatalf("guest startup stage=recovery error=%.4096s", err.Error())
	}
	defer runtime.Close()
	log.Printf("guest startup stage=listener")
	listener, err := vsock.Listen(vmmdPort)
	if err != nil {
		log.Fatalf("guest startup stage=listener error=%.4096s", err.Error())
	}
	defer listener.Close()
	grpcServer := control.NewGRPCServer()
	service := control.NewServer(runtime, control.HealthState{
		BootID: strings.TrimSpace(string(bootID)), RootReadOnly: prepared.RootReadOnly,
		Versions: map[string]string{
			"containerd": version("/usr/bin/containerd", "--version"),
			"runsc":      version("/usr/bin/runsc", "--version"),
			"shim":       version("/usr/bin/containerd-shim-runsc-v1", "-v"),
		},
		EgressHealth: pump.Ready,
		EgressStatus: pump.Status,
	}, func(ctx context.Context) error {
		stopPump()
		if err := pump.Close(); err != nil {
			return err
		}
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
	executable, err := os.Executable()
	if err != nil {
		return nil, err
	}
	command := exec.Command(executable, containerdLauncherArg)
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

func execContainerdWithoutQuotaBypass() error {
	// Capabilities are per-thread. Keep this launcher on one OS thread through
	// exec so ordinary trusted containerd/shim writes remain subject to project
	// hard limits. vmmd retains CAP_SYS_ADMIN as the quota/project-id authority.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	// Give containerd the minimum OOM score before removing CAP_SYS_RESOURCE.
	// The runsc shim inherits -1000 and raises itself to its built-in -999, which
	// is permitted without restoring the capability that can override project
	// quotas.
	if err := setContainerdOOMScore("/proc/self/oom_score_adj"); err != nil {
		return fmt.Errorf("set containerd OOM score: %w", err)
	}
	if err := unix.Prctl(unix.PR_CAPBSET_DROP, unix.CAP_SYS_RESOURCE, 0, 0, 0); err != nil {
		return err
	}
	return unix.Exec("/usr/bin/containerd", []string{"containerd", "--config", "/etc/containerd/config.toml"}, os.Environ())
}

func setContainerdOOMScore(path string) error {
	return os.WriteFile(path, []byte("-1000"), 0o600)
}

func version(name string, argument string) string {
	output, err := exec.Command(name, argument).CombinedOutput()
	if err != nil {
		return "unavailable"
	}
	return strings.TrimSpace(string(output))
}
