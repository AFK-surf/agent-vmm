package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

const startupLogReadLimit = 1 << 20

type startupEvidence struct {
	OperationID         string `json:"operationId,omitempty"`
	RequestID           string `json:"requestId,omitempty"`
	LastSuccessfulStage string `json:"lastSuccessfulStage,omitempty"`
	CurrentStage        string `json:"currentStage,omitempty"`
	CurrentState        string `json:"currentState,omitempty"`
	FailureStage        string `json:"failureStage,omitempty"`
	FailureMessage      string `json:"failureMessage,omitempty"`
	FailureConfirmed    bool   `json:"failureConfirmed"`
	Source              string `json:"source"`
	ObservedAt          string `json:"observedAt,omitempty"`
}

type parsedStartupRecord struct {
	OperationID string `json:"operation_id"`
	RequestID   string `json:"request_id"`
	Stage       string `json:"stage"`
	State       string `json:"state"`
	Error       string `json:"error"`
}

func readStartupEvidence(value paths, operation *lifecycleOperation) *startupEvidence {
	result := &startupEvidence{}
	if operation != nil {
		result.RequestID = operation.RequestID
	}
	for _, source := range []struct {
		path   string
		marker string
		name   string
	}{
		{filepath.Join(value.logs, "host.stderr.log"), "vmm_start=", "host_start_log"},
		{filepath.Join(value.logs, "serial.log.previous"), "guest_startup=", "guest_serial_log"},
		{filepath.Join(value.logs, "serial.log"), "guest_startup=", "guest_serial_log"},
	} {
		data, observedAt, err := readTailNoFollow(source.path, startupLogReadLimit)
		if err != nil {
			continue
		}
		if applyStartupRecords(result, data, source.marker, operation) {
			result.Source = source.name
			result.ObservedAt = observedAt.UTC().Format(time.RFC3339Nano)
		}
	}
	if result.CurrentStage == "" && !result.FailureConfirmed {
		return nil
	}
	return result
}

func applyStartupRecords(result *startupEvidence, data []byte, marker string, operation *lifecycleOperation) bool {
	if marker == "guest_startup=" && result.OperationID == "" {
		return false
	}
	if marker == "guest_startup=" && (result.FailureConfirmed || result.CurrentStage == "wait_guest_health" && result.CurrentState == "succeeded") {
		return false
	}
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	scanner.Buffer(make([]byte, 4096), 8192)
	guestCurrent := ""
	applied := false
	for scanner.Scan() {
		line := scanner.Text()
		index := strings.Index(line, marker)
		if index < 0 {
			continue
		}
		var record parsedStartupRecord
		if json.Unmarshal([]byte(line[index+len(marker):]), &record) != nil || record.Stage == "" {
			continue
		}
		if operation != nil && record.RequestID != "" && record.RequestID != operation.RequestID {
			continue
		}
		if marker == "vmm_start=" && operation == nil && result.OperationID != "" && record.OperationID != "" && record.OperationID != result.OperationID {
			*result = startupEvidence{}
			guestCurrent = ""
		}
		if result.OperationID != "" && record.OperationID != "" && record.OperationID != result.OperationID {
			continue
		}
		applied = true
		if record.OperationID != "" {
			result.OperationID = record.OperationID
		}
		if record.RequestID != "" {
			result.RequestID = record.RequestID
		}
		if marker == "guest_startup=" && record.State == "started" && guestCurrent != "" && guestCurrent != record.Stage {
			result.LastSuccessfulStage = guestCurrent
		}
		result.CurrentStage = record.Stage
		result.CurrentState = record.State
		if marker == "guest_startup=" && record.State == "started" {
			guestCurrent = record.Stage
		}
		if record.State == "succeeded" {
			result.LastSuccessfulStage = record.Stage
		}
		if record.State == "failed" && record.Error != "" {
			result.FailureStage = record.Stage
			result.FailureMessage = record.Error
			result.FailureConfirmed = true
		}
	}
	return applied
}

func readTailNoFollow(path string, limit int64) ([]byte, time.Time, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, time.Time{}, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return nil, time.Time{}, errors.New("startup log is not a regular file")
	}
	start := info.Size() - limit
	if start < 0 {
		start = 0
	}
	if _, err := file.Seek(start, io.SeekStart); err != nil {
		return nil, time.Time{}, err
	}
	data, err := io.ReadAll(io.LimitReader(file, limit))
	return data, info.ModTime(), err
}
