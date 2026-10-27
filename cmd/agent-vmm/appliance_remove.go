//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
)

func applianceRemove(arguments []string) {
	flags := flag.NewFlagSet("appliance remove", flag.ExitOnError)
	socket := flags.String("operator-socket", filepath.Join(platformDataDirectory(os.Getenv("HOME")), "operator.sock"), "Agent VMM operator socket")
	installation := flags.String("installation", "", "exact installation ID")
	requestID := flags.String("request-id", "", "removal plan request ID; retain for retries")
	revision := flags.Uint64("expected-revision", 0, "revision from the reviewed removal plan")
	execute := flags.Bool("execute", false, "execute the explicitly confirmed removal plan")
	timeout := flags.Duration("timeout", 10*time.Minute, "finite operation deadline")
	var confirmed []string
	flags.Func("confirm-resource", "exact resource ID approved for deletion; repeat for each resource", func(value string) error {
		if value == "" {
			return errors.New("confirmed resource ID must not be empty")
		}
		confirmed = append(confirmed, value)
		return nil
	})
	flags.Parse(arguments)
	check(runApplianceRemove(*socket, *installation, *requestID, *revision, *execute, confirmed, *timeout))
}

func runApplianceRemove(socket, installation, requestID string, revision uint64, execute bool, confirmed []string, timeout time.Duration) error {
	if installation == "" || timeout <= 0 {
		return errors.New("remove requires --installation and a positive timeout")
	}
	if execute && (requestID == "" || revision == 0 || len(confirmed) == 0) {
		return errors.New("execute requires the plan's --request-id, --expected-revision and explicit --confirm-resource IDs")
	}
	if !execute && (revision != 0 || len(confirmed) != 0) {
		return errors.New("resource confirmation and expected revision require --execute")
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := appliancev1.NewApplianceAdminServiceClient(connection)
	deadline := time.Now().Add(timeout).UnixMilli()
	if !execute {
		status, err := client.GetApplianceStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: installation})
		if err != nil {
			return err
		}
		if requestID == "" {
			requestID = lifecycleRequestID()
		}
		plan, err := client.PlanRemove(ctx, &appliancev1.PlanRemoveRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: status.GetStatus().GetRevision(), DeadlineUnixMillis: deadline})
		if err != nil {
			return err
		}
		fmt.Printf("request_id=%s expected_revision=%d\n%s\n", requestID, plan.GetPlan().GetRevision(), protojson.Format(plan))
		return nil
	}
	request, err := freezeApplianceRemove(socket, &appliancev1.ExecuteRemoveRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline, ConfirmedResourceIds: confirmed}, timeout)
	if err != nil {
		return err
	}
	response, err := client.ExecuteRemove(ctx, request)
	if err != nil {
		if reason, ok := hostv1.ErrorReasonOf(err); ok && reason == hostv1.ErrorReason_ERROR_REASON_APPLIANCE_OPERATION_NOT_ADMITTED {
			return fmt.Errorf("original removal intent was not accepted; review a new plan with appliance remove --installation %s, then approve it with a new --request-id: %w", installation, err)
		}
		return err
	}
	err = watchApplianceOperation(ctx, client, installation, response.GetOperation())
	if status.Code(err) != codes.DeadlineExceeded {
		return err
	}
	arguments := []string{os.Args[0], "appliance", "remove", "--operator-socket", socket, "--installation", installation, "--request-id", request.GetRequestId(), "--execute", "--expected-revision", strconv.FormatUint(request.GetExpectedRevision(), 10), "--timeout", timeout.String()}
	for _, resource := range request.GetConfirmedResourceIds() {
		arguments = append(arguments, "--confirm-resource", resource)
	}
	for index, argument := range arguments {
		arguments[index] = "'" + strings.ReplaceAll(argument, "'", "'\"'\"'") + "'"
	}
	return fmt.Errorf("remove watch deadline reached. This deadline stops observation only.\ninstallation=%s operation=%s request_id=%s\nContinue with the original removal request:\n%s\n%w", installation, response.GetOperation().GetOperationId(), request.GetRequestId(), strings.Join(arguments, " "), err)
}
