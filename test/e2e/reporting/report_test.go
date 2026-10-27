package reporting

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadResultsReportsInterruptedSubtests(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.json")
	content := `{"Time":"2026-08-17T00:00:00Z","Action":"run","Test":"TestE2E"}
{"Time":"2026-08-17T00:00:01Z","Action":"run","Test":"TestE2E/remote_workflow"}
{"Time":"2026-08-17T00:00:03Z","Action":"output","Test":"TestE2E/remote_workflow","Output":"signal: interrupt\n"}
{"Time":"2026-08-17T00:00:04Z","Action":"fail","Test":"TestE2E","Elapsed":4}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	results, _, _, err := readResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Name != "remote_workflow" || results[0].Action != "interrupted" || results[0].Duration != 3 {
		t.Fatalf("results = %+v", results)
	}
}

func TestReadResultsDoesNotMarkParallelParentInterrupted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.json")
	content := `{"Time":"2026-08-17T00:00:00Z","Action":"run","Test":"TestE2E/parallel"}
{"Time":"2026-08-17T00:00:01Z","Action":"run","Test":"TestE2E/parallel/alpha"}
{"Time":"2026-08-17T00:00:02Z","Action":"pass","Test":"TestE2E/parallel/alpha","Elapsed":1}
{"Time":"2026-08-17T00:00:03Z","Action":"pass","Test":"TestE2E/parallel","Elapsed":3}
`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	results, _, _, err := readResults(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 1 || results[0].Name != "parallel/alpha" || results[0].Action != "pass" {
		t.Fatalf("results = %+v", results)
	}
}

func TestRenderPreparationFailureWithoutManifest(t *testing.T) {
	directory := t.TempDir()
	jsonPath := filepath.Join(directory, "test.json")
	if err := os.WriteFile(jsonPath, []byte(`{"Action":"fail","Output":"E2E preparation failed"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	report := filepath.Join(directory, "report.md")
	if err := Render(directory, directory, jsonPath, report, 1); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(report)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "| `preparation` | FAIL |") {
		t.Fatalf("report = %s", data)
	}
}

func TestReadResultsReportsTopLevelLeaves(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.json")
	if err := os.WriteFile(path, []byte(`{"Action":"run","Test":"TestExternalBridge"}
{"Action":"pass","Test":"TestExternalBridge","Elapsed":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	results, _, _, err := readResults(path)
	if err != nil || len(results) != 1 || results[0].Name != "TestExternalBridge" || results[0].Action != "pass" {
		t.Fatalf("results = %+v, error = %v", results, err)
	}
}
