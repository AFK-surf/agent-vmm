//go:build darwin

package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

var stateDiskBusy = func(path string) (bool, error) {
	output, err := inspectOpenFiles("-Fn", "--", path)
	if err == nil {
		return len(output) != 0, nil
	}
	if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
		return false, err
	}
	// An unlinked disk can remain attached. Inspect its open descriptors before stopping the Host.
	parent, err := filepath.EvalSymlinks(filepath.Dir(path))
	if err != nil {
		return false, err
	}
	expected := filepath.Join(parent, filepath.Base(path))
	output, err = inspectOpenFiles("+L1", "-Fn")
	if err != nil {
		return false, err
	}
	for _, line := range strings.Split(string(output), "\n") {
		if line == "n"+expected {
			return true, nil
		}
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		return false, errors.New("state disk changed during attachment inspection")
	}
	return false, nil
}

func inspectOpenFiles(args ...string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, "/usr/sbin/lsof", args...)
	var output, diagnostic bytes.Buffer
	command.Stdout = &boundedInspectionOutput{buffer: &output, cancel: cancel}
	command.Stderr = &boundedInspectionOutput{buffer: &diagnostic, cancel: cancel}
	err := command.Run()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if exit, ok := err.(*exec.ExitError); ok && exit.ExitCode() == 1 && diagnostic.Len() == 0 {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if diagnostic.Len() != 0 {
		return nil, errors.New("open-file inspection returned a diagnostic")
	}
	return output.Bytes(), nil
}

type boundedInspectionOutput struct {
	buffer *bytes.Buffer
	cancel context.CancelFunc
}

func (w *boundedInspectionOutput) Write(p []byte) (int, error) {
	if w.buffer.Len()+len(p) > 16<<20 {
		w.cancel()
		return 0, io.ErrShortBuffer
	}
	return w.buffer.Write(p)
}
