//go:build darwin && platform_spike

package vmm

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"github.com/Code-Hex/vz/v3"
)

//go:embed testdata/directory_share/guest.sh
var directoryShareGuestScript []byte

func TestDirectoryShareBootPlatform(t *testing.T) {
	guest := os.Getenv("AGENT_VMM_SPIKE_BUNDLE")
	if guest == "" {
		t.Fatal("AGENT_VMM_SPIKE_BUNDLE must name an existing Guest bundle")
	}
	work := t.TempDir()
	for _, name := range []string{"Image", "rootfs.squashfs"} {
		if err := os.Symlink(filepath.Join(guest, name), filepath.Join(work, name)); err != nil {
			t.Fatal(err)
		}
	}
	prepareDirectoryShareInitramfs(t, guest, work)
	if err := os.WriteFile(filepath.Join(work, "state.raw"), make([]byte, 1<<20), 0600); err != nil {
		t.Fatal(err)
	}
	serial, err := os.Create(filepath.Join(work, "serial.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer serial.Close()
	input, writeInput, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	defer writeInput.Close()
	manager := &Manager{config: Config{CPUCount: 2, MemoryBytes: 2 << 30, InstanceDirectory: work}, state: hostv1.VMState_VM_STATE_STOPPED}
	outside := filepath.Join(work, "outside")
	if err := os.Mkdir(outside, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("host-secret"), 0600); err != nil {
		t.Fatal(err)
	}
	entries := make(map[string]*vz.SharedDirectory)
	for _, name := range []string{"authorized", "readonly", "private"} {
		source := filepath.Join(work, name)
		mode := os.FileMode(0777)
		if name == "private" {
			mode = 0700
		}
		if err := os.Mkdir(source, mode); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(source, mode); err != nil {
			t.Fatal(err)
		}
		for _, file := range []string{"token", "data"} {
			if err := os.WriteFile(filepath.Join(source, file), []byte("source"), 0666); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.Symlink(outside, filepath.Join(source, "absolute")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("../outside", filepath.Join(source, "relative")); err != nil {
			t.Fatal(err)
		}
		directory, err := vz.NewSharedDirectory(source, name == "readonly")
		if err != nil {
			t.Fatal(err)
		}
		entries[name] = directory
	}
	if err := manager.SetHostDirectoryShares(entries); err != nil {
		t.Fatal(err)
	}
	verified := &bundle.Verified{Directory: work, Manifest: bundle.Manifest{Artifacts: map[string]bundle.Artifact{"kernel": {Path: "Image"}, "rootfs": {Path: "rootfs.squashfs"}, "initramfs": {Path: "initramfs.xz"}}}}
	config, err := manager.configuration(verified, serial, false, startObservation{})
	if err != nil {
		t.Fatal(err)
	}
	attachment, err := vz.NewFileHandleSerialPortAttachment(input, serial)
	if err != nil || attachment == nil {
		t.Fatalf("serial attachment = %v, %v", attachment, err)
	}
	port, err := vz.NewVirtioConsoleDeviceSerialPortConfiguration(attachment)
	if err != nil {
		t.Fatal(err)
	}
	config.SetSerialPortsVirtualMachineConfiguration([]*vz.VirtioConsoleDeviceSerialPortConfiguration{port})
	if valid, err := config.Validate(); err != nil || !valid {
		t.Fatal(valid, err)
	}
	machine, err := vz.NewVirtualMachine(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := machine.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if machine.CanStop() {
			if err := machine.Stop(); err != nil {
				t.Error(err)
			}
		}
		if t.Failed() {
			log, _ := os.ReadFile(serial.Name())
			t.Logf("Guest serial:\n%s", log)
		}
	}()
	waitDirectoryShareReply(t, serial.Name(), "SPIKE READY")
	number := 0
	command := func(action string) string {
		number++
		if _, err := fmt.Fprintf(writeInput, "%d %s\n", number, action); err != nil {
			t.Fatal(err)
		}
		result := waitDirectoryShareReply(t, serial.Name(), fmt.Sprintf("SPIKE %d %s ", number, strings.Fields(action)[0]))
		t.Logf("Guest %s: %s", action, result)
		return result
	}
	if result := command("READ source"); result != "source" {
		t.Fatal("boot share unavailable", result)
	}
	if result := command("RO"); result != "denied" {
		t.Fatal("read-only share accepted write", result)
	}
	if result := command("FSINFO"); result != "ready" {
		t.Fatal(result)
	}
	if result := command("START"); result != "complete" {
		t.Fatal(result)
	}
	if result := command("ISOLATION"); result != "denied" {
		t.Fatal("peer container saw unmounted share", result)
	}
	peerBefore, err := strconv.Atoi(command("PEER"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	dataBefore, err := os.Stat(filepath.Join(work, "authorized", "data"))
	if err != nil || dataBefore.Size() <= int64(len("source")) {
		t.Fatal("real runsc writer did not use share", dataBefore, err)
	}
	if result := command("STOP"); result != "complete" {
		t.Fatal(result)
	}
	settled, err := os.Stat(filepath.Join(work, "authorized", "data"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	after, err := os.Stat(filepath.Join(work, "authorized", "data"))
	if err != nil || settled.Size() != after.Size() {
		t.Fatal("stopped writer retained access", settled, after, err)
	}
	peerAfter, err := strconv.Atoi(command("PEER"))
	if err != nil || peerAfter <= peerBefore {
		t.Fatal("other environment stopped", peerBefore, peerAfter, err)
	}
	for _, name := range []string{"authorized", "readonly", "private"} {
		beforeLog, _ := os.ReadFile(serial.Name())
		if result := command("PROBE " + name); result != "complete" {
			t.Fatal(result)
		}
		log, err := os.ReadFile(serial.Name())
		if err != nil {
			t.Fatal(err)
		}
		observation := string(log[len(beforeLog):])
		if !strings.Contains(observation, "uid=1000 gid=1000") || strings.Count(observation, "ESCAPE_DENIED") != 2 || strings.Contains(observation, "host-secret") {
			t.Fatal("permission/symlink observation", observation)
		}
		if name == "private" {
			if !strings.Contains(observation, "READ_DENIED") || !strings.Contains(observation, "WRITE_DENIED") {
				t.Fatal("private directory permitted UID1000", observation)
			}
		} else if !strings.Contains(observation, "READ_OK") || (name == "authorized" && !strings.Contains(observation, "WRITE_OK")) || (name == "readonly" && !strings.Contains(observation, "WRITE_DENIED")) {
			t.Fatal("shared directory permissions", name, observation)
		}
	}
	if result := command("DONE"); result != "complete" {
		t.Fatal(result)
	}
	socket := machine.SocketDevices()[0]
	listener, err := socket.Listen(43218)
	if err != nil {
		t.Fatal(err)
	}
	verifySocketListenerClose(t, listener)
	listener, err = socket.Listen(43219)
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan error, 1)
	go func() { _, err := listener.Accept(); accepted <- err }()
	select {
	case err := <-accepted:
		t.Fatalf("Accept returned before VM stop: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := machine.Stop(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := waitState(ctx, &darwinMachine{VirtualMachine: machine}, hostv1.VMState_VM_STATE_STOPPED); err != nil {
		t.Fatal(err)
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept returned no error after stop and close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener close after VM stop did not wake Accept")
	}
}

func verifySocketListenerClose(t *testing.T, listener net.Listener) {
	t.Helper()
	accepted := make(chan error, 1)
	go func() { _, err := listener.Accept(); accepted <- err }()
	select {
	case err := <-accepted:
		t.Fatalf("Accept returned before close: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-accepted:
		if err == nil {
			t.Fatal("Accept returned no error after close")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener close did not wake Accept")
	}
}

func waitDirectoryShareReply(t *testing.T, path, marker string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		log, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(log, []byte("Kernel panic")) {
			t.Fatalf("Guest failed before %q", marker)
		}
		for _, line := range strings.Split(string(log), "\n") {
			if strings.HasPrefix(line, marker) {
				return strings.TrimSpace(strings.TrimPrefix(line, marker))
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("Guest did not emit %q", marker)
	return ""
}

func prepareDirectoryShareInitramfs(t *testing.T, guest, work string) {
	t.Helper()
	root := filepath.Join(work, "initramfs")
	if err := os.Mkdir(root, 0700); err != nil {
		t.Fatal(err)
	}
	unpack := exec.Command("xz", "-dc", filepath.Join(guest, "initramfs.xz"))
	var archive bytes.Buffer
	unpack.Stdout = &archive
	if err := unpack.Run(); err != nil {
		t.Fatal(err)
	}
	extract := exec.Command("cpio", "-idm")
	extract.Dir, extract.Stdin = root, &archive
	if output, err := extract.CombinedOutput(); err != nil {
		t.Fatal(string(output), err)
	}
	if err := os.WriteFile(filepath.Join(root, "spike.sh"), directoryShareGuestScript, 0600); err != nil {
		t.Fatal(err)
	}
	init, err := os.ReadFile(filepath.Join(root, "init"))
	if err != nil {
		t.Fatal(err)
	}
	init = bytes.Replace(init, []byte("exec switch_root /newroot /sbin/init"), []byte("mount -t tmpfs tmpfs /newroot/run\nmount -t tmpfs tmpfs /newroot/tmp\n/bin/busybox cp /spike.sh /newroot/tmp/spike.sh\nexec switch_root /newroot /bin/sh /tmp/spike.sh </newroot/dev/hvc0 >/newroot/dev/hvc0 2>&1"), 1)
	if err := os.WriteFile(filepath.Join(root, "init"), init, 0755); err != nil {
		t.Fatal(err)
	}
	entries, err := exec.Command("find", root, "-print").Output()
	if err != nil {
		t.Fatal(err)
	}
	entries = bytes.ReplaceAll(entries, []byte(root), []byte("."))
	pack := exec.Command("cpio", "-o", "-H", "newc")
	pack.Dir, pack.Stdin = root, bytes.NewReader(entries)
	packed, err := pack.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	compressed, err := os.Create(filepath.Join(work, "initramfs.xz"))
	if err != nil {
		t.Fatal(err)
	}
	defer compressed.Close()
	compress := exec.Command("xz", "-1", "--check=crc32")
	compress.Stdin, compress.Stdout = packed, compressed
	if err := pack.Start(); err != nil {
		t.Fatal(err)
	}
	if err := compress.Run(); err != nil {
		t.Fatal(err)
	}
	if err := pack.Wait(); err != nil {
		t.Fatal(err)
	}
}
