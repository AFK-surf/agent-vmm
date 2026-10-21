package main

import (
	"encoding/json"
	"path/filepath"
	"testing"
)

func TestPreflightReportsInsufficientDiskForMissingInstallDirectory(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "Library", "Application Support", "AgentVMM")
	report, err := preflight(directory, 1)
	if err != nil || !report.OK || len(report.Issues) != 0 {
		t.Fatalf("small requirement = %+v, %v", report, err)
	}
	report, err = preflight(directory, 1<<62)
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	var decoded struct {
		OK     bool `json:"ok"`
		Issues []struct {
			Code           string `json:"code"`
			RequiredBytes  int64  `json:"requiredBytes"`
			AvailableBytes int64  `json:"availableBytes"`
		} `json:"issues"`
	}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.OK || len(decoded.Issues) != 1 || decoded.Issues[0].Code != "insufficient_disk" ||
		decoded.Issues[0].RequiredBytes != 1<<62 || decoded.Issues[0].AvailableBytes <= 0 || decoded.Issues[0].AvailableBytes >= 1<<62 {
		t.Fatalf("insufficient disk report = %s", data)
	}
}
