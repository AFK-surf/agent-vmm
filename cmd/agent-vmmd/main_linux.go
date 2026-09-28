//go:build linux

package main

import (
	"context"
	"encoding/json"
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
	startup := guestStartupReference()
	reportGuestStartup(startup, "bootstrap", "started", nil)
	prepared, err := bootstrap.Prepare()
	if err != nil {
		reportGuestStartup(startup, "bootstrap", "failed", err)
		os.Exit(1)
	}
	reportGuestStartup(startup, "bootstrap", "succeeded", nil)
	reportGuestStartup(startup, "oci_storage", "started", nil)
	if err := control.PrepareOCIStorage(prepared.StateDevice, prepared.ControlDevice); err != nil {
		reportGuestStartup(startup, "oci_storage", "failed", err)
		os.Exit(1)
	}
	reportGuestStartup(startup, "oci_storage", "succeeded", nil)
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
	reportGuestStartup(startup, "containerd", "started", nil)
	containerd, err := startContainerd()
	if err != nil {
		reportGuestStartup(startup, "containerd", "failed", err)
		os.Exit(1)
	}
	reportGuestStartup(startup, "containerd", "succeeded", nil)
	defer func() {
		_ = containerd.Process.Signal(syscall.SIGTERM)
		_ = containerd.Wait()
	}()
	reportGuestStartup(startup, "recovery", "started", nil)
	runtime, err := control.NewRuntime(prepared.StateDevice, prepared.ControlDevice, networkManager)
	if err != nil {
		reportGuestStartup(startup, "recovery", "failed", err)
		os.Exit(1)
	}
	reportGuestStartup(startup, "recovery", "succeeded", nil)
	defer runtime.Close()
	reportGuestStartup(startup, "listener", "started", nil)
	listener, err := vsock.Listen(vmmdPort)
	if err != nil {
		reportGuestStartup(startup, "listener", "failed", err)
		os.Exit(1)
	}
	reportGuestStartup(startup, "listener", "succeeded", nil)
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

type startupReference struct {
	OperationID string `json:"operation_id,omitempty"`
}

type guestStartupProgress struct {
	OperationID string `json:"operation_id,omitempty"`
	Stage       string `json:"stage"`
	State       string `json:"state"`
	Error       string `json:"error,omitempty"`
}

func guestStartupReference() startupReference {
	data, err := os.ReadFile("/proc/cmdline")
	if err != nil {
		return startupReference{}
	}
	return parseGuestStartupReference(string(data))
}

func parseGuestStartupReference(commandLine string) startupReference {
	for _, field := range strings.Fields(commandLine) {
		if value, ok := strings.CutPrefix(field, "agent.operation_id="); ok {
			return startupReference{OperationID: value}
		}
	}
	return startupReference{}
}

func reportGuestStartup(reference startupReference, stage, state string, failure error) {
	record := guestStartupProgress{OperationID: reference.OperationID, Stage: stage, State: state}
	if failure != nil {
		record.Error = failure.Error()
		if len(record.Error) > 4096 {
			record.Error = record.Error[:4096]
		}
	}
	if data, err := json.Marshal(record); err == nil {
		log.Printf("guest_startup=%s", data)
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
	// This launcher bypasses the OpenRC containerd service, so set its file
	// limit here. Gofer caches descriptors across mounts after workloads close
	// their files; the inherited 4096 limit is insufficient for those caches.
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 1048576, Max: 1048576}); err != nil {
		return fmt.Errorf("set containerd open file limit: %w", err)
	}
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
