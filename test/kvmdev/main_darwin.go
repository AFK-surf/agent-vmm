//go:build darwin && arm64

// kvmdev boots a disposable Linux test environment with nested virtualization.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/Code-Hex/vz/v3"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	kernel := flag.String("kernel", "", "existing arm64 Linux Image")
	initrd := flag.String("initrd", "", "test initramfs")
	shared := flag.String("shared", "", "test files exposed as kvm-dev")
	logPath := flag.String("log", "", "outer VM serial log")
	memory := flag.Uint64("memory", 4<<30, "outer VM RAM bytes")
	timeout := flag.Duration("timeout", 5*time.Minute, "maximum test duration")
	flag.Parse()
	if *kernel == "" || *initrd == "" || *shared == "" || *logPath == "" {
		return errors.New("kernel, initrd, shared and log are required")
	}
	if !vz.IsNestedVirtualizationSupported() {
		return errors.New("nested virtualization unavailable")
	}
	boot, err := vz.NewLinuxBootLoader(*kernel, vz.WithInitrd(*initrd), vz.WithCommandLine("console=hvc0 panic=-1"))
	if err != nil {
		return err
	}
	config, err := vz.NewVirtualMachineConfiguration(boot, 2, *memory)
	if err != nil {
		return err
	}
	platform, err := vz.NewGenericPlatformConfiguration()
	if err != nil {
		return err
	}
	if err = platform.SetNestedVirtualizationEnabled(true); err != nil {
		return err
	}
	config.SetPlatformVirtualMachineConfiguration(platform)
	serial, err := vz.NewFileSerialPortAttachment(*logPath, false)
	if err != nil {
		return err
	}
	port, err := vz.NewVirtioConsoleDeviceSerialPortConfiguration(serial)
	if err != nil {
		return err
	}
	config.SetSerialPortsVirtualMachineConfiguration([]*vz.VirtioConsoleDeviceSerialPortConfiguration{port})
	dir, err := vz.NewSharedDirectory(*shared, false)
	if err != nil {
		return err
	}
	share, err := vz.NewSingleDirectoryShare(dir)
	if err != nil {
		return err
	}
	fs, err := vz.NewVirtioFileSystemDeviceConfiguration("kvm-dev")
	if err != nil {
		return err
	}
	fs.SetDirectoryShare(share)
	config.SetDirectorySharingDevicesVirtualMachineConfiguration([]vz.DirectorySharingDeviceConfiguration{fs})
	rng, err := vz.NewVirtioEntropyDeviceConfiguration()
	if err != nil {
		return err
	}
	config.SetEntropyDevicesVirtualMachineConfiguration([]*vz.VirtioEntropyDeviceConfiguration{rng})
	if _, err = config.Validate(); err != nil {
		return err
	}
	vm, err := vz.NewVirtualMachine(config)
	if err != nil {
		return err
	}
	if err = vm.Start(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	for {
		select {
		case state := <-vm.StateChangedNotify():
			if state == vz.VirtualMachineStateStopped {
				return nil
			}
			if state == vz.VirtualMachineStateError {
				return fmt.Errorf("outer VM failed; see %s", *logPath)
			}
		case <-ctx.Done():
			if vm.CanStop() {
				_ = vm.Stop()
			}
			return fmt.Errorf("outer VM timeout; see %s: %w", *logPath, ctx.Err())
		}
	}
}
