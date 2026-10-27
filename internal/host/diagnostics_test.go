//go:build darwin || linux

package host

import (
	"database/sql"
	"path/filepath"
	"testing"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/vmm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDiagnosticTerminalWriteFailureReturnsUnavailable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "host.db")
	database, err := openTestStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	connection, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	connection.SetMaxOpenConns(1)
	if _, err := connection.Exec(`PRAGMA busy_timeout=5000`); err != nil {
		t.Fatal(err)
	}
	if _, err := connection.Exec(`CREATE TRIGGER reject_diagnostic_terminal BEFORE UPDATE ON operations WHEN NEW.kind='diagnostic' BEGIN SELECT RAISE(ABORT,'terminal storage unavailable'); END`); err != nil {
		t.Fatal(err)
	}
	controller := newTestController(&Controller{store: database, manager: &vmm.Manager{}})
	response, err := (&operatorServer{controller: controller}).RunDiagnostics(t.Context(), &hostv1.RunDiagnosticsRequest{RequestId: "diagnostic"})
	if response != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("diagnostic response=%+v,error=%v", response, err)
	}
	operation, readErr := database.OperationByRequestID(t.Context(), "diagnostic")
	if readErr != nil || operation.State != int32(hostv1.OperationState_OPERATION_STATE_RUNNING) {
		t.Fatalf("unsaved terminal operation=%+v,error=%v", operation, readErr)
	}
}
