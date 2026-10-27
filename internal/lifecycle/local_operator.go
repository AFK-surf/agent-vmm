//go:build darwin || linux

package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func localOperatorCommand(command string) bool {
	switch command {
	case "local-plan", "local-execute", "local-operation", "registration-recovery-proof", "local-disk", "local-environments", "local-registrations":
		return true
	}
	return false
}

// Inputs are exact protobuf requests. Execute requires the caller's fresh local confirmation.
// These commands neither create a lifecycle journal nor contact a cloud service.
func runLocalOperatorCommand(ctx context.Context, command, socket string, input io.Reader, output io.Writer) error {
	raw, err := io.ReadAll(io.LimitReader(input, 8193))
	if err != nil {
		return err
	}
	if len(raw) == 0 || len(raw) > 8192 {
		return errors.New("local operator request exceeds its input budget")
	}
	connection, err := operatorConnection(ctx, socket)
	if err != nil {
		return err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	var result proto.Message
	switch command {
	case "local-disk":
		request := new(hostv1.GetLocalDiskSummaryRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.GetLocalDiskSummary(ctx, request)
		}
	case "local-environments":
		request := new(hostv1.ListLocalEnvironmentPageRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.ListLocalEnvironmentPage(ctx, request)
		}
	case "local-registrations":
		request := new(hostv1.ListLocalRegistrationPageRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.ListLocalRegistrationPage(ctx, request)
		}
	case "local-plan":
		request := new(hostv1.PlanLocalDisposalRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.PlanLocalDisposal(ctx, request)
		}
	case "local-execute":
		request := new(hostv1.ExecuteLocalDisposalRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.ExecuteLocalDisposal(ctx, request)
		}
	case "local-operation":
		request := new(hostv1.GetOperationRequest)
		if err = protojson.Unmarshal(raw, request); err == nil {
			result, err = client.GetOperation(ctx, request)
		}
	case "registration-recovery-proof":
		// Host parses the fixed-purpose JSON. Do not expose a generic sign-bytes RPC.
		result, err = client.SignRegistrationRecovery(ctx, &hostv1.SignRegistrationRecoveryRequest{ChallengeJson: raw})
	default:
		return errors.New("unknown local operator command")
	}
	if err != nil {
		return err
	}
	encoded, err := protojson.Marshal(result)
	if err != nil {
		return err
	}
	_, err = output.Write(append(encoded, '\n'))
	return err
}

func executeLocalOperator(command string, request serviceRequest, arguments []string) {
	var err error
	if len(arguments) != 0 {
		err = errors.New("local operator requests must use bounded JSON stdin")
	}
	resolved := paths{}
	if err == nil {
		resolved, err = observationPathsForRequest(request)
	}
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		err = runLocalOperatorCommand(ctx, command, filepath.Join(resolved.applicationData, "operator.sock"), os.Stdin, os.Stdout)
	}
	if err != nil {
		reason := "LOCAL_OPERATOR_FAILED"
		if value, ok := hostv1.ErrorReasonOf(err); ok {
			reason = strings.TrimPrefix(value.String(), "ERROR_REASON_")
		}

		_ = json.NewEncoder(os.Stdout).Encode(map[string]any{"error": map[string]string{"code": status.Code(err).String(), "reason": reason}})
		os.Exit(1)
	}
}
