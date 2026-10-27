//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"github.com/AFK-surf/agent-vmm/internal/guest/bootstrap"
	"github.com/AFK-surf/agent-vmm/internal/guest/containerddaemon"
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
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))
	if err := run(); err != nil {
		slog.Error("vmmd stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 1048576, Max: 1048576}); err != nil {
		return err
	}
	if len(os.Args) == 2 && os.Args[1] == containerdLauncherArg {
		return execContainerdWithoutQuotaBypass()
	}
	startup := guestStartupReference()
	reportGuestStartup(startup, "bootstrap", "started", nil)
	prepared, err := bootstrap.Prepare()
	if err != nil {
		reportGuestStartup(startup, "bootstrap", "failed", err)
		return err
	}
	reportGuestStartup(startup, "bootstrap", "succeeded", nil)
	reportGuestStartup(startup, "oci_storage", "started", nil)
	if err := control.PrepareOCIStorage(prepared.StateDevice); err != nil {
		reportGuestStartup(startup, "oci_storage", "failed", err)
		return err
	}
	reportGuestStartup(startup, "oci_storage", "succeeded", nil)
	bootID, err := os.ReadFile("/proc/sys/kernel/random/uuid")
	if err != nil {
		return err
	}
	pump, err := egress.New(strings.TrimSpace(string(bootID)))
	if err != nil {
		return err
	}
	pumpContext, stopPump := context.WithCancel(context.Background())
	defer stopPump()
	defer pump.Close()
	go func() {
		if err := pump.Run(pumpContext); err != nil {
			slog.Error("egress pump stopped", "component", "guest-egress", "error", err)
		}
	}()
	networkManager, err := network.NewManager(slog.Default())
	if err != nil {
		return err
	}
	defer networkManager.Close()
	reportGuestStartup(startup, "containerd", "started", nil)
	containerd, err := startContainerd()
	if err != nil {
		reportGuestStartup(startup, "containerd", "failed", err)
		return err
	}
	if err := dropQuotaBypass(); err != nil {
		return err
	}
	reportGuestStartup(startup, "containerd", "succeeded", nil)
	defer func() {
		_ = containerd.Process.Signal(syscall.SIGTERM)
		_ = containerd.Wait()
	}()
	reportGuestStartup(startup, "recovery", "started", nil)
	runtime, err := control.NewRuntime(prepared.StateDevice, prepared.ControlDevice, networkManager, slog.Default())
	if err != nil {
		reportGuestStartup(startup, "recovery", "failed", err)
		return err
	}
	reportGuestStartup(startup, "recovery", "succeeded", nil)
	defer runtime.Close()
	reportGuestStartup(startup, "listener", "started", nil)
	listener, err := vsock.Listen(vmmdPort)
	if err != nil {
		reportGuestStartup(startup, "listener", "failed", err)
		return err
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
		return err
	}
	return nil
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
		fmt.Fprintf(os.Stderr, "guest_startup=%s\n", data)
	}
}

func startContainerd() (*exec.Cmd, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stopErr := containerddaemon.Stop(ctx, "/run/containerd/containerd.sock")
	cancel()
	if stopErr != nil {
		return nil, fmt.Errorf("settle previous containerd writer: %w", stopErr)
	}
	required, err := containerddaemon.NativeRetirementRequired("/control/containerd")
	if err != nil {
		return nil, err
	}
	if required {
		recovery, err := launchContainerd()
		if err != nil {
			return nil, err
		}
		retireContext, retireCancel := context.WithTimeout(context.Background(), 10*time.Second)
		retireErr := containerddaemon.StopRetiredNativeTasks(retireContext, "/run/containerd/containerd.sock")
		retireCancel()
		stopContext, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		stopErr := containerddaemon.Stop(stopContext, "/run/containerd/containerd.sock")
		stopCancel()
		if stopErr != nil {
			_ = recovery.Process.Kill()
		}
		_ = recovery.Wait()
		if err := errors.Join(retireErr, stopErr); err != nil {
			return nil, fmt.Errorf("settle retired native tasks: %w", err)
		}
	}
	if err := containerddaemon.RemoveRetiredNative("/control/containerd"); err != nil {
		return nil, err
	}
	return launchContainerd()
}

func launchContainerd() (*exec.Cmd, error) {
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
		if containerddaemon.IsOwner("/run/containerd/containerd.sock", command.Process.Pid) {
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
