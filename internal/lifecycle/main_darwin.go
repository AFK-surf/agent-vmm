package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

func executeMaintenanceCommand(command string, service serviceRequest, args []string) {
	if len(args) != 0 || service.app != "" {
		fatal("maintenance commands accept a JSON request on stdin and service identity options only")
	}
	account, err := lookupServiceAccount(service.user)
	if err != nil {
		fatal("service user is unavailable")
	}
	uid, err := strconv.Atoi(account.Uid)
	if err != nil || uid == 0 || (os.Geteuid() != 0 && uid != os.Geteuid()) {
		fatal("maintenance requires the local service user")
	}
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, 16385))
	decoder.DisallowUnknownFields()
	if command == "maintenance-status" {
		var request struct {
			RequestID string `json:"requestId,omitempty"`
		}
		if err := decoder.Decode(&request); err != nil && err != io.EOF {
			fatal("invalid maintenance status request")
		}
		if decoder.Decode(&struct{}{}) != io.EOF {
			fatal("invalid maintenance status request")
		}
		result, err := maintenanceStatus(paths{home: account.HomeDir}, request.RequestID)
		if err != nil {
			fatal(err.Error())
		}
		_ = json.NewEncoder(os.Stdout).Encode(maintenanceReport(result))
		return
	}
	var request maintenanceRequest
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		fatal("invalid maintenance request")
	}
	saved, err := maintenanceServiceReceipt(paths{home: account.HomeDir}, request)
	if err != nil {
		fatal(err.Error())
	}
	// A reset may have removed service.json. Resume the original service mode
	// and pairing policy from the outside journal, rather than choosing anew.
	if saved != nil && (service.mode == "" || service.mode == serviceModeAuto) {
		service.mode = saved.ServiceMode
	}
	config, err := resolveServiceConfig(service)
	if err != nil {
		fatal(err.Error())
	}
	if saved != nil {
		config.DisablePairing = saved.DisablePairing
	}
	if config.UID != os.Geteuid() {
		fatal("maintenance must run as the configured non-root service user")
	}
	executable, err := os.Executable()
	if err != nil {
		fatal(err.Error())
	}
	executable, err = filepath.EvalSymlinks(executable)
	if err != nil {
		fatal(err.Error())
	}
	target := filepath.Join(account.HomeDir, "Library", "Application Support", "Agent VMM Host", "current", bundle.HostAppName)
	value, err := observationPathsForConfig(account.HomeDir, executable, config, target)
	if err != nil {
		fatal(err.Error())
	}
	result, err := runMaintenance(value, request, defaultMaintenanceActions())
	if errors.Is(err, errMaintenanceOwnerBusy) {
		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"error": map[string]string{"code": "owner_busy", "message": err.Error()}})
		fatal(fmt.Sprintf("Host maintenance: %v", err))
	}
	_ = json.NewEncoder(os.Stdout).Encode(maintenanceReport(&result))
	if err != nil {
		fatal(fmt.Sprintf("Host maintenance: %v", err))
	}
}

// Refuse symlink ancestors before any removal. The shared service home is the
// trusted root; missing descendants are valid for bootstrap and retries.
