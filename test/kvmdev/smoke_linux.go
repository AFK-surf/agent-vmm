//go:build linux

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	guestvsock "github.com/AFK-surf/agent-vmm/internal/guest/vsock"
	"github.com/AFK-surf/agent-vmm/internal/vmmprocess"
	"golang.org/x/sys/unix"
)

const rootMarker = "agent-vmm smoke root"

var egressReply = bytes.Repeat([]byte("finite egress response\n"), 8192)
var controlReply = bytes.Repeat([]byte("finite control response\n"), 4096)

type smokeRequest struct{ Operation, Value string }
type smokeReport struct {
	BootID, State, Error string
	UnixSeconds          int64
	Echo                 []byte
}

func main() {
	var err error
	switch {
	case len(os.Args) > 1 && os.Args[1] == "__vmm":
		err = vmmprocess.RunChild()
	case len(os.Args) > 1 && os.Args[1] == "inner":
		err = runInner()
	default:
		watchdog := time.AfterFunc(2*time.Minute, func() { fmt.Fprintln(os.Stderr, "smoke exceeded two-minute deadline"); os.Exit(1) })
		defer watchdog.Stop()
		err = runOuter()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func runOuter() (result error) {
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	directory, err := os.MkdirTemp("/tmp", "kvm-smoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	root, state := filepath.Join(directory, "root.raw"), filepath.Join(directory, "state.raw")
	for _, path := range []string{root, state} {
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0600)
		if err != nil {
			return err
		}
		err = f.Truncate(4 << 20)
		if path == root && err == nil {
			_, err = f.WriteAt([]byte(rootMarker), 0)
		}
		_ = f.Close()
		if err != nil {
			return err
		}
	}
	config := vmmprocess.Config{KernelPath: "/work/Image", InitramfsPath: "/work/inner-initramfs.xz", RootFSPath: root, StatePath: state, InstanceDirectory: filepath.Join(directory, "instance"), SerialLog: "/work/inner.log", CommandLine: "console=ttyS0 panic=-1", GuestManifestDigest: "smoke-fixture", CPUCount: 1, MemoryBytes: 512 << 20}
	var client *vmmprocess.Client
	defer func() {
		if client != nil {
			_ = client.Stop()
		}
		if result != nil {
			if log, err := os.ReadFile(config.SerialLog); err == nil {
				fmt.Fprintf(os.Stderr, "inner serial:\n%s\n", log)
			}
		}
	}()
	start := func() error { var err error; client, err = vmmprocess.Start(ctx, config); return err }
	if err := start(); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	fmt.Println("VMM child started")
	if err := acceptEgress(ctx, client); err != nil {
		return err
	}
	marker := "persistent marker " + time.Now().UTC().Format(time.RFC3339Nano)
	first, err := control(ctx, client, smokeRequest{Operation: "write", Value: marker})
	if err != nil {
		return err
	}
	if first.State != marker {
		return fmt.Errorf("initial state %q", first.State)
	}
	fmt.Println("inner boot, read-only root, disk IDs, state write+Sync, and bidirectional vsock passed", first.BootID)
	previous := client
	if err := start(); err != nil {
		return fmt.Errorf("adopt: %w", err)
	}
	if client.Spawned() {
		return errors.New("adoption spawned a new VMM")
	}
	if conn, err := previous.Dial(ctx, 4050); err == nil {
		conn.Close()
		return errors.New("adoption did not fence the previous controller")
	}
	adopted, err := control(ctx, client, smokeRequest{Operation: "read"})
	if err != nil {
		return err
	}
	if adopted.BootID != first.BootID || adopted.State != marker {
		return fmt.Errorf("adoption changed boot or state: %+v", adopted)
	}
	fmt.Println("controller adoption preserved inner boot and disk state")
	if err := client.Stop(); err != nil {
		return fmt.Errorf("stop: %w", err)
	}
	client = nil
	if err := start(); err != nil {
		return fmt.Errorf("restart: %w", err)
	}
	if err := acceptEgress(ctx, client); err != nil {
		return err
	}
	restarted, err := control(ctx, client, smokeRequest{Operation: "read"})
	if err != nil {
		return err
	}
	if restarted.BootID == first.BootID || restarted.State != marker {
		return fmt.Errorf("restart did not preserve disk/change boot: %+v", restarted)
	}
	fmt.Println("explicit stop/restart preserved state and changed boot", restarted.BootID)
	if err := client.Stop(); err != nil {
		return err
	}
	client = nil
	fmt.Println("PASS nested KVM block/vsock/process smoke")
	return nil
}

func control(ctx context.Context, client *vmmprocess.Client, req smokeRequest) (smokeReport, error) {
	deadline := time.Now().Add(30 * time.Second)
	var conn net.Conn
	var err error
	for time.Now().Before(deadline) {
		attempt, cancel := context.WithTimeout(ctx, 2*time.Second)
		conn, err = client.Dial(attempt, 4050)
		cancel()
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return smokeReport{}, ctx.Err()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		return smokeReport{}, fmt.Errorf("dial inner: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return smokeReport{}, err
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		return smokeReport{}, err
	}
	data, err := io.ReadAll(io.LimitReader(conn, 1<<20))
	if err != nil {
		return smokeReport{}, fmt.Errorf("control reply: %w", err)
	}
	var report smokeReport
	if err := json.Unmarshal(data, &report); err != nil {
		return report, fmt.Errorf("decode control reply %q: %w", data, err)
	}
	if report.Error != "" {
		return report, errors.New(report.Error)
	}
	if !bytes.Equal(report.Echo, controlReply) {
		return report, fmt.Errorf("finite control response changed: %d bytes", len(report.Echo))
	}
	delta := time.Now().Unix() - report.UnixSeconds
	if delta < -10 || delta > 10 {
		return report, fmt.Errorf("guest wallclock differs from host by %d seconds", delta)
	}
	return report, nil
}

func acceptEgress(ctx context.Context, client *vmmprocess.Client) error {
	listener, err := client.Listen(4051)
	if err != nil {
		return err
	}
	defer listener.Close()
	timer := time.AfterFunc(35*time.Second, func() { _ = listener.Close() })
	defer timer.Stop()
	conn, err := listener.Accept()
	if err != nil {
		return fmt.Errorf("accept guest egress: %w", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	request, err := io.ReadAll(io.LimitReader(conn, 4096))
	if err != nil {
		return fmt.Errorf("egress request: %w", err)
	}
	if !strings.HasPrefix(string(request), "guest egress ") {
		return fmt.Errorf("unexpected egress %q", request)
	}
	if _, err := conn.Write(egressReply); err != nil {
		return err
	}
	if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
		return err
	}
	return ctx.Err()
}

func runInner() error {
	boot, err := os.ReadFile("/proc/sys/kernel/random/boot_id")
	if err != nil {
		return err
	}
	bootID := strings.TrimSpace(string(boot))
	root, state := "", ""
	entries, err := os.ReadDir("/sys/block")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		serial, err := os.ReadFile(filepath.Join("/sys/block", entry.Name(), "serial"))
		if err != nil {
			continue
		}
		switch strings.TrimSpace(string(serial)) {
		case "AGENT_ROOT":
			root = "/dev/" + entry.Name()
		case "AGENT_STATE":
			state = "/dev/" + entry.Name()
		}
	}
	if root == "" || state == "" {
		return fmt.Errorf("stable block identifiers missing: root=%q state=%q", root, state)
	}
	rootFile, err := os.OpenFile(root, os.O_RDWR, 0)
	if err == nil {
		defer rootFile.Close()
		if _, err := rootFile.WriteAt([]byte("forbidden"), 0); err == nil {
			return errors.New("root disk accepted write")
		}
	} else if !errors.Is(err, unix.EROFS) && !errors.Is(err, unix.EACCES) {
		return err
	}
	rootFile, err = os.Open(root)
	if err != nil {
		return err
	}
	buf := make([]byte, len(rootMarker))
	_, err = io.ReadFull(rootFile, buf)
	_ = rootFile.Close()
	if err != nil || string(buf) != rootMarker {
		return fmt.Errorf("root disk data: %q %v", buf, err)
	}
	stateFile, err := os.OpenFile(state, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer stateFile.Close()
	listener, err := guestvsock.Listen(4050)
	if err != nil {
		return err
	}
	defer listener.Close()
	egressDone := make(chan error, 1)
	go func() { egressDone <- guestEgress(bootID) }()
	egressReady := false
	var egressErr error
	fmt.Println("inner ready", bootID, root, state)
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		data, readErr := io.ReadAll(io.LimitReader(conn, 4096))
		report := smokeReport{BootID: bootID, Echo: controlReply, UnixSeconds: time.Now().Unix()}
		if !egressReady {
			egressErr = <-egressDone
			egressReady = true
		}
		if egressErr != nil {
			report.Error = egressErr.Error()
		}
		var req smokeRequest
		if readErr != nil {
			report.Error = readErr.Error()
		} else if err := json.Unmarshal(data, &req); err != nil {
			report.Error = err.Error()
		} else if req.Operation == "write" {
			block := make([]byte, 4096)
			copy(block, req.Value)
			if _, err := stateFile.WriteAt(block, 0); err != nil {
				report.Error = err.Error()
			} else if err := stateFile.Sync(); err != nil {
				report.Error = err.Error()
			}
		}
		if report.Error == "" {
			block := make([]byte, 4096)
			if _, err := stateFile.ReadAt(block, 0); err != nil {
				report.Error = err.Error()
			} else {
				report.State = strings.TrimRight(string(block), "\x00")
			}
		}
		_ = json.NewEncoder(conn).Encode(report)
		_ = conn.Close()
	}
}

func guestEgress(bootID string) error {
	fd, err := unix.Socket(unix.AF_VSOCK, unix.SOCK_STREAM|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		return err
	}
	file := os.NewFile(uintptr(fd), "smoke-egress")
	defer file.Close()
	if err := unix.Connect(fd, &unix.SockaddrVM{CID: unix.VMADDR_CID_HOST, Port: 4051}); err != nil {
		return err
	}
	if _, err := file.Write([]byte("guest egress " + bootID)); err != nil {
		return err
	}
	if err := unix.Shutdown(fd, unix.SHUT_WR); err != nil {
		return err
	}
	got, err := io.ReadAll(io.LimitReader(file, 1<<20))
	if err != nil {
		return err
	}
	if !bytes.Equal(got, egressReply) {
		return fmt.Errorf("finite egress response changed: %d bytes", len(got))
	}
	fmt.Println("inner egress finite response passed")
	return nil
}
