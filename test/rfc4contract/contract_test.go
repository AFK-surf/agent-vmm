package rfc4contract

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

type schema struct {
	Type                 string            `json:"type"`
	AdditionalProperties any               `json:"additionalProperties"`
	Properties           map[string]schema `json:"properties"`
}

type tool struct {
	Name           string `json:"name"`
	ReadOnly       bool   `json:"read_only"`
	TimeoutSeconds int    `json:"timeout_seconds"`
	Input          schema `json:"input"`
	Output         string `json:"output"`
}

type contract struct {
	Tasks []string `json:"tasks"`
	MCP   struct {
		Protocol              string   `json:"protocol"`
		SerializedResultBytes int      `json:"serialized_result_bytes"`
		Programmatic          []string `json:"programmatic"`
		DirectOnly            []string `json:"direct_only"`
		Tools                 []tool   `json:"tools"`
	} `json:"mcp"`
	HostChanges      []string `json:"host_changes"`
	RemoteTranscript []string `json:"remote_transcript"`
	Faults           []string `json:"faults"`
}

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate contract test")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func loadJSON(t *testing.T, name string, target any) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(repositoryRoot(t), ".context", "rfc4", name))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		t.Fatal(err)
	}
}

func TestContractIsClosed(t *testing.T) {
	var value contract
	loadJSON(t, "contract.json", &value)
	if len(value.Tasks) != 12 {
		t.Fatalf("minimum tasks = %d, want 12", len(value.Tasks))
	}
	if value.MCP.Protocol != "2026-07-28" || value.MCP.SerializedResultBytes != 2<<20 {
		t.Fatalf("MCP limits = %+v", value.MCP)
	}
	if len(value.MCP.Tools) != 20 {
		t.Fatalf("tools = %d, want 20", len(value.MCP.Tools))
	}
	seen := make(map[string]bool, len(value.MCP.Tools))
	for _, tool := range value.MCP.Tools {
		if tool.Name == "" || seen[tool.Name] || tool.Output == "" || tool.TimeoutSeconds <= 0 {
			t.Fatalf("invalid tool contract: %+v", tool)
		}
		seen[tool.Name] = true
		if tool.Input.Type != "object" || tool.Input.AdditionalProperties != false {
			t.Fatalf("%s input is not a closed object", tool.Name)
		}
	}
	for _, prohibited := range []string{"image_build", "read_file", "write_file", "operator", "batch", "program"} {
		if seen[prohibited] {
			t.Fatalf("prohibited tool %q is present", prohibited)
		}
	}
	if len(value.HostChanges) != 6 || len(value.RemoteTranscript) != 7 || len(value.Faults) != 8 {
		t.Fatalf("open contract branch: host=%d transcript=%d faults=%d", len(value.HostChanges), len(value.RemoteTranscript), len(value.Faults))
	}
}

func TestGoldenEnvelope(t *testing.T) {
	var golden map[string]map[string]any
	loadJSON(t, "mcp-golden.json", &golden)
	if golden["success"]["ok"] != true || golden["failure"]["ok"] != false {
		t.Fatalf("invalid result envelopes: %+v", golden)
	}
}

func TestPerformanceGateIsNumericAndBounded(t *testing.T) {
	var gate struct {
		Frozen        bool   `json:"frozen_before_tunnel_implementation"`
		FrameBytes    int    `json:"frame_bytes"`
		StreamWindow  int    `json:"outer_stream_window_bytes"`
		ConnectWindow int    `json:"outer_connection_window_bytes"`
		InFlight      int    `json:"max_in_flight_bytes_per_direction"`
		RTT           []int  `json:"rtt_ms"`
		FailureAction string `json:"failure_action"`
	}
	loadJSON(t, "performance-gate.json", &gate)
	if !gate.Frozen || gate.FrameBytes < 16<<10 || gate.FrameBytes > 32<<10 {
		t.Fatalf("invalid frame gate: %+v", gate)
	}
	if gate.StreamWindow < 4<<20 || gate.StreamWindow > 16<<20 || gate.ConnectWindow < 4<<20 || gate.ConnectWindow > 16<<20 {
		t.Fatalf("invalid window gate: %+v", gate)
	}
	if gate.InFlight < 1<<20 || gate.InFlight > 2<<20 || len(gate.RTT) != 4 || gate.FailureAction == "" {
		t.Fatalf("invalid tunnel gate: %+v", gate)
	}
}

func TestSchemaV1MigrationExpectation(t *testing.T) {
	var fixture struct {
		SchemaVersion int              `json:"schema_version"`
		Environments  []map[string]any `json:"environments"`
		Expectation   struct {
			OwnerKind      string  `json:"owner_kind"`
			RegistrationID *string `json:"registration_id"`
			AllocationID   *string `json:"allocation_id"`
		} `json:"migration_expectation"`
	}
	loadJSON(t, "schema-v1-fixture.json", &fixture)
	if fixture.SchemaVersion != 1 || len(fixture.Environments) == 0 || fixture.Expectation.OwnerKind != "LOCAL" || fixture.Expectation.RegistrationID != nil || fixture.Expectation.AllocationID != nil {
		t.Fatalf("ambiguous migration fixture: %+v", fixture)
	}
}
