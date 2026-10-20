//go:build linux

package build

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	runtimeoptions "github.com/containerd/containerd/api/types/runtimeoptions/v1"
)

const maxStartupLogBytes = 64 << 10

func (w *Workspace) startupRuntimeOptions() (*runtimeoptions.Options, string, error) {
	configuration, err := os.ReadFile("/etc/containerd/runsc-build.toml")
	if err != nil {
		return nil, "", err
	}
	directory := filepath.Join(w.root, "startup")
	if err := os.Mkdir(directory, 0o700); err != nil {
		return nil, "", err
	}
	path := filepath.Join(directory, "runsc-errors.json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, "", err
	}
	if err := file.Close(); err != nil {
		return nil, "", err
	}
	configuration = append(configuration, []byte("\nlog = "+strconv.Quote(path)+"\nlog-format = \"json\"\ndebug = \"false\"\n")...)
	configPath := filepath.Join(directory, "runsc.toml")
	if err := os.WriteFile(configPath, configuration, 0o600); err != nil {
		return nil, "", err
	}
	return &runtimeoptions.Options{TypeUrl: "io.containerd.runsc.v1.options", ConfigPath: configPath}, path, nil
}

func runtimeFailure(path string, createError error) error {
	file, err := os.Open(path)
	if err != nil {
		return createError
	}
	defer file.Close()
	decoder := json.NewDecoder(io.LimitReader(file, maxStartupLogBytes))
	for count := 0; count < 16; count++ {
		var entry struct {
			Message string `json:"msg"`
			Level   string `json:"level"`
		}
		if err := decoder.Decode(&entry); err != nil {
			return createError
		}
		if entry.Level != "error" || entry.Message == "" {
			continue
		}
		message := entry.Message
		if len(message) > 2048 {
			message = message[:2048]
		}
		return fmt.Errorf("%w; runsc fatal: %q", createError, message)
	}
	return createError
}
