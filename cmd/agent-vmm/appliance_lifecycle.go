//go:build darwin

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func applianceLifecycle(command string, arguments []string) {
	flags := flag.NewFlagSet("appliance "+command, flag.ExitOnError)
	socket := flags.String("operator-socket", filepath.Join(os.Getenv("HOME"), "Library/Application Support/AgentVMM/operator.sock"), "Agent VMM operator socket")
	product := flags.String("product", "", "installed appliance product")
	installation := flags.String("installation", "", "installation ID; otherwise select the unique product")
	packagePath := flags.String("package", "", "appliance package directory containing manifest.pb")
	operationID := flags.String("operation", "", "update operation to recover")
	timeout := flags.Duration("timeout", 10*time.Minute, "finite operation deadline")
	bootstrapStdin := flags.Bool("bootstrap-stdin", false, "read opaque bootstrap seed from stdin (install only)")
	endpoints := flags.String("host-tcp-endpoints", "", "operator-approved Host IPv4:TCP-port list, comma separated")
	approveChanges := flags.Bool("approve-grant-changes", false, "approve ordinary grant changes requested by the new release")
	resources := registerApplianceResources(flags)
	flags.Parse(arguments)
	var endpointApproval *string
	flags.Visit(func(value *flag.Flag) {
		if value.Name == "host-tcp-endpoints" {
			endpointApproval = endpoints
		}
		resources.record(value.Name)
	})
	check(runApplianceLifecycle(command, *socket, *product, *installation, *packagePath, *operationID, *timeout, *bootstrapStdin, os.Stdin, endpointApproval, *approveChanges, resources))
}

func lifecycleRequestID() string {
	value := make([]byte, 16)
	_, err := rand.Read(value)
	check(err)
	return hex.EncodeToString(value)
}

func selectApplianceInstallation(statuses []*appliancev1.ApplianceStatus, product, installation string) (*appliancev1.ApplianceStatus, error) {
	var selected *appliancev1.ApplianceStatus
	for _, candidate := range statuses {
		if installation != "" && candidate.GetInstallationId() != installation {
			continue
		}
		if product != "" && candidate.GetProductId() != product {
			continue
		}
		if selected != nil {
			return nil, errors.New("more than one installation matches; specify --installation")
		}
		selected = candidate
	}
	if selected == nil {
		return nil, errors.New("no installation matches")
	}
	return selected, nil
}

func runApplianceLifecycle(command, socket, product, installation, packagePath, operationID string, timeout time.Duration, bootstrapStdin bool, stdin io.Reader, hostEndpoints *string, approveChanges bool, resources *applianceResources) error {
	if timeout <= 0 {
		return errors.New("timeout must be positive")
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
	var response *appliancev1.ApplianceOperationResponse
	if command == "recover" {
		if operationID == "" {
			return errors.New("recover requires --operation")
		}
		watchCtx, watchCancel := context.WithCancel(ctx)
		watch, err := client.WatchOperation(watchCtx, &appliancev1.WatchApplianceOperationRequest{InstallationId: installation, OperationId: operationID})
		if err != nil {
			watchCancel()
			return err
		}
		observed, err := watch.Recv()
		watchCancel()
		if err != nil {
			return err
		}
		operation := observed.GetOperation()
		if installation != "" && operation.GetInstallationId() != installation {
			return errors.New("operation belongs to another installation")
		}
		installation = operation.GetInstallationId()
		if deadline <= operation.GetDeadlineUnixMillis() {
			return errors.New("recovery timeout must extend the operation deadline")
		}
		response, err = client.RecoverUpdate(ctx, &appliancev1.RecoverApplianceUpdateRequest{InstallationId: installation, OperationId: operationID, ExpectedUpdatedUnixNanos: operation.GetUpdatedAt().AsTime().UnixNano(), DeadlineUnixMillis: deadline})
		if err != nil {
			return err
		}
	} else if command == "stop" || command == "start" || command == "activate" || command == "restart" || command == "revoke" {
		listed, listErr := client.List(ctx, &appliancev1.ListAppliancesRequest{})
		if listErr != nil {
			return listErr
		}
		selected, selectErr := selectApplianceInstallation(listed.GetAppliances(), product, installation)
		if selectErr != nil {
			return selectErr
		}
		installation = selected.GetInstallationId()
		requestID := lifecycleRequestID()
		revision := selected.GetRevision()
		switch command {
		case "stop":
			response, err = client.Stop(ctx, &appliancev1.StopApplianceRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
		case "revoke":
			response, err = client.Revoke(ctx, &appliancev1.RevokeApplianceRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
		case "restart":
			response, err = client.Restart(ctx, &appliancev1.RestartApplianceRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
		default:
			plan, planErr := client.PlanActivation(ctx, &appliancev1.PlanActivationRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
			if planErr != nil {
				return planErr
			}
			fmt.Println(protojson.Format(plan))
			response, err = client.Activate(ctx, &appliancev1.ActivateRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
		}
		if err != nil {
			return err
		}
	} else {
		if packagePath == "" {
			return errors.New("install/update requires --package")
		}
		encoded, err := os.ReadFile(filepath.Join(packagePath, "manifest.pb"))
		if err != nil {
			return err
		}
		manifest := new(appliancev1.ControllerApplianceManifest)
		if err := proto.Unmarshal(encoded, manifest); err != nil {
			return err
		}
		if product != "" && manifest.GetProductId() != product {
			return errors.New("package product does not match --product")
		}
		product = manifest.GetProductId()
		if err := stagePackageContent(ctx, client, packagePath, manifest, deadline); err != nil {
			return err
		}
		grant, err := lifecycleGrant(manifest, nil, hostEndpoints, true)
		if err != nil {
			return err
		}
		if err := resources.apply(grant, manifest); err != nil {
			return err
		}
		staged, err := client.StageArtifact(ctx, &appliancev1.StageArtifactRequest{RequestId: lifecycleRequestID(), DeadlineUnixMillis: deadline, Manifest: manifest})
		if err != nil {
			return err
		}
		artifact := staged.GetArtifactId()
		requestID := lifecycleRequestID()
		if command == "install" {
			if !bootstrapStdin {
				return errors.New("install requires --bootstrap-stdin; pass the seed through an anonymous pipe")
			}
			seed, err := io.ReadAll(io.LimitReader(stdin, (1<<20)+1))
			if err != nil {
				return errors.New("cannot read bootstrap seed from stdin")
			}
			defer clear(seed)
			if len(seed) == 0 || len(seed) > 1<<20 {
				return errors.New("bootstrap seed must contain 1 to 1048576 bytes")
			}
			if installation == "" {
				installation = product + "-" + lifecycleRequestID()
			}
			_, err = client.PlanInstall(ctx, &appliancev1.PlanInstallRequest{InstallationId: installation, RequestId: requestID, ArtifactId: artifact, DeadlineUnixMillis: deadline, ApprovedGrant: grant})
			if err != nil {
				return fmt.Errorf("install plan rejected; reduce --system-disk-gib or --environment-disk-gib to fit VM capacity, within manifest limits: %w", err)
			}
			response, err = client.Install(ctx, &appliancev1.InstallRequest{InstallationId: installation, RequestId: requestID, ArtifactId: artifact, DeadlineUnixMillis: deadline, ApprovedGrant: grant, OpaqueBootstrapSeed: seed})
			if err != nil {
				return err
			}
			fmt.Printf("installation=%s operation=%s\n", installation, response.GetOperation().GetOperationId())
			status, err := client.GetStatus(ctx, &appliancev1.GetApplianceStatusRequest{InstallationId: installation})
			if err != nil {
				return fmt.Errorf("installation %s is installed; continue with appliance activate --installation %s: %w", installation, installation, err)
			}
			revision := status.GetStatus().GetRevision()
			activateID := lifecycleRequestID()
			_, err = client.PlanActivation(ctx, &appliancev1.PlanActivationRequest{InstallationId: installation, RequestId: activateID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
			if err != nil {
				return fmt.Errorf("installation %s is installed; continue with appliance activate --installation %s: %w", installation, installation, err)
			}
			response, err = client.Activate(ctx, &appliancev1.ActivateRequest{InstallationId: installation, RequestId: activateID, ExpectedRevision: revision, DeadlineUnixMillis: deadline})
			if err != nil {
				return fmt.Errorf("installation %s is installed; continue with appliance activate --installation %s: %w", installation, installation, err)
			}
		} else {
			listed, err := client.List(ctx, &appliancev1.ListAppliancesRequest{})
			if err != nil {
				return err
			}
			selected, err := selectApplianceInstallation(listed.GetAppliances(), product, installation)
			if err != nil {
				return err
			}
			grant, err = lifecycleGrant(manifest, selected.GetApprovedGrant(), hostEndpoints, approveChanges)
			if err != nil {
				return err
			}
			if err := resources.apply(grant, manifest); err != nil {
				return err
			}
			installation = selected.GetInstallationId()
			revision := selected.GetRevision()
			requested, _ := lifecycleGrant(manifest, nil, nil, true)
			fmt.Printf("current_grant=%s\nrequested_grant=%s\nproposed_grant=%s\n", protojson.Format(selected.GetApprovedGrant()), protojson.Format(requested), protojson.Format(grant))
			plan, planErr := client.PlanUpdate(ctx, &appliancev1.PlanUpdateRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, ArtifactId: artifact, DeadlineUnixMillis: deadline, ApprovedGrant: grant})
			if planErr != nil {
				return fmt.Errorf("update plan rejected the proposed grant; inspect target permissions and adjust approval explicitly: %w", planErr)
			}

			fmt.Println(protojson.Format(plan))
			response, err = client.Update(ctx, &appliancev1.UpdateApplianceRequest{InstallationId: installation, RequestId: requestID, ExpectedRevision: revision, ArtifactId: artifact, DeadlineUnixMillis: deadline, ApprovedGrant: grant})
			if err != nil {
				return err
			}
		}
	}
	return watchApplianceOperation(ctx, client, installation, response)
}

func watchApplianceOperation(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, installation string, response *appliancev1.ApplianceOperationResponse) error {
	operation := response.GetOperation()
	fmt.Printf("installation=%s operation=%s phase=%s\n", installation, operation.GetOperationId(), operation.GetPhase())
	watch, err := client.WatchOperation(ctx, &appliancev1.WatchApplianceOperationRequest{InstallationId: installation, OperationId: operation.GetOperationId()})
	if err != nil {
		return err
	}
	lastPhase := appliancev1.ApplianceOperationPhase_APPLIANCE_OPERATION_PHASE_UNSPECIFIED
	for {
		observed, err := watch.Recv()
		if err != nil {
			return err
		}
		operation = observed.GetOperation()
		if operation.GetPhase() != lastPhase {
			fmt.Printf("phase=%s state=%s\n", operation.GetPhase(), operation.GetState())
			lastPhase = operation.GetPhase()
		}
		if operation.GetState() == appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_SUCCEEDED {
			return nil
		}
		if operation.GetState() != appliancev1.ApplianceOperationState_APPLIANCE_OPERATION_STATE_RUNNING {
			return fmt.Errorf("operation %s retained phase %s: %s", operation.GetOperationId(), operation.GetPhase(), operation.GetError().GetReason())
		}
	}
}

func lifecycleGrant(manifest *appliancev1.ControllerApplianceManifest, previous *appliancev1.ApprovedGrant, endpoints *string, approveChanges bool) (*appliancev1.ApprovedGrant, error) {
	requested := &appliancev1.ApprovedGrant{SystemLimits: manifest.GetSystemLimits(), Capacity: manifest.GetRequestedCapacity(), EgressMode: manifest.GetRequestedEgressMode(), Frontends: manifest.GetRequestedFrontends(), DynamicHostLoopback: manifest.GetRequestedDynamicHostLoopback(), LanEgress: manifest.GetRequestedLanEgress()}
	grant := proto.Clone(requested).(*appliancev1.ApprovedGrant)
	if previous != nil {
		if !approveChanges {
			grant = proto.Clone(previous).(*appliancev1.ApprovedGrant)
		} else {
			grant.HostTcpEndpoints = proto.Clone(previous).(*appliancev1.ApprovedGrant).GetHostTcpEndpoints()
		}
	}
	if endpoints != nil {
		grant.HostTcpEndpoints = nil
		for _, value := range strings.Split(*endpoints, ",") {
			if value == "" {
				continue
			}
			endpoint, err := netip.ParseAddrPort(value)
			if err != nil || !endpoint.Addr().Is4() || endpoint.Port() == 0 {
				return nil, errors.New("--host-tcp-endpoints requires exact IPv4:TCP-port values")
			}
			grant.HostTcpEndpoints = append(grant.HostTcpEndpoints, &appliancev1.HostTcpEndpoint{Ip: endpoint.Addr().String(), Port: uint32(endpoint.Port())})
		}
	}
	return grant, nil
}
