//go:build darwin || linux

package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagedLocalLogsPrioritizeCurrentAndResumeRotation(t *testing.T) {
	directory := t.TempDir()
	previous, current := filepath.Join(directory, "previous"), filepath.Join(directory, "current")
	paths := []string{previous, current}
	write := func(path, data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0600); err != nil {
			t.Fatal(err)
		}
	}
	read := func(cursor string) ([]managedLogRecord, string, bool) {
		t.Helper()
		records, next, gap, err := readManagedLocalLogFiles(paths, managedOptions{afterCursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		return records, next, gap
	}
	write(previous, strings.Repeat("p", managedLogBytes+1))
	write(current, "latest\n")
	records, cursor, gap := read("")
	if !gap || len(records) != 2 || records[1].Data != "latest\n" || len(records[0].Data)+len(records[1].Data) != managedLogBytes {
		t.Fatalf("latest records=%d gap=%t", len(records), gap)
	}
	records, unchanged, gap := read(cursor)
	if len(records) != 0 || unchanged != cursor || gap {
		t.Fatalf("unchanged records=%d gap=%t", len(records), gap)
	}
	file, err := os.OpenFile(current, os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = file.WriteString("append\n"); err != nil {
		t.Fatal(err)
	}
	file.Close()
	records, next, gap := read(cursor)
	if gap || len(records) != 1 || records[0].Data != "append\n" || next == cursor {
		t.Fatalf("append records=%+v gap=%t", records, gap)
	}
	os.Remove(previous)
	if err := os.Rename(current, previous); err != nil {
		t.Fatal(err)
	}
	write(current, "rotated\n")
	records, _, gap = read(next)
	if !gap || len(records) != 2 || records[1].Data != "rotated\n" {
		t.Fatalf("rotation records=%+v gap=%t", records, gap)
	}
	os.Remove(previous)
	records, _, gap = read(next)
	if !gap || len(records) != 1 || records[0].Data != "rotated\n" {
		t.Fatalf("expired records=%+v gap=%t", records, gap)
	}
}

func TestManagedLocalLogsCopyTruncateAndRecordLimit(t *testing.T) {
	directory := t.TempDir()
	previous, current := filepath.Join(directory, "previous"), filepath.Join(directory, "current")
	paths := []string{previous, current}
	if err := os.WriteFile(current, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	_, cursor, _, err := readManagedLocalLogFiles(paths, managedOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(previous, []byte("old"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(current, []byte("new-current-larger-than-old"), 0600); err != nil {
		t.Fatal(err)
	}
	records, _, gap, err := readManagedLocalLogFiles(paths, managedOptions{afterCursor: cursor, limit: 1})
	if err != nil || !gap || len(records) != 1 || records[0].Data != "new-current-larger-than-old" {
		t.Fatalf("copy truncate records=%+v gap=%t err=%v", records, gap, err)
	}
	os.Remove(previous)
	if err := os.Link(current, previous); err != nil {
		t.Fatal(err)
	}
	records, _, _, err = readManagedLocalLogFiles(paths, managedOptions{})
	if err != nil || len(records) != 1 {
		t.Fatalf("same inode records=%d err=%v", len(records), err)
	}
}

func TestManagedLocalLogJSONPreservesLatestTail(t *testing.T) {
	for _, records := range [][]managedLogRecord{
		{{Data: strings.Repeat("\"\n", managedLogBytes/2)}, {Data: "current-latest\n"}},
		{{Data: strings.Repeat("\"\n", managedLogBytes/2) + "current-latest\n"}},
	} {
		envelope := fitManagedLogEnvelope(managedEnvelope{Source: []string{"guest_log"}, Logs: records})
		encoded, err := json.Marshal(envelope)
		if err != nil || len(encoded)+1 > managedLogBytes || !envelope.Gap || !strings.HasSuffix(envelope.Logs[len(envelope.Logs)-1].Data, "current-latest\n") {
			t.Fatalf("encoded=%d gap=%t latest tail missing", len(encoded), envelope.Gap)
		}
	}
}
