package main

// This file is the production adapter for the provider-neutral lifecycle
// coordinator. The coordinator remains the only operation ordering owner;
// this adapter only translates its bounded steps to the authenticated local
// OperatorService and ApplianceAdminService.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/protobuf/proto"
)

const (
	lifecycleApplianceConfigEnv     = "AGENT_VMM_APPLIANCE_CONFIG"
	lifecycleInstallationIDEnv      = "AGENT_VMM_APPLIANCE_INSTALLATION_ID"
	lifecycleAppliancePollInterval  = 100 * time.Millisecond
	lifecycleApplianceConfigVersion = 1
)

var errApplianceLifecycleConfigUnavailable = errors.New("Agent VMM appliance lifecycle configuration is unavailable")

// applianceLifecycleConfig is deliberately file based. The signed manifest,
// grant and bootstrap seed are release/operator inputs and must not be
// embedded in the helper or passed through a renderer/CLI payload.
type applianceLifecycleConfig struct {
	Version                int    `json:"version"`
	InstallationID         string `json:"installation_id"`
	ManifestPath           string `json:"manifest_path"`
	PublisherSignaturePath string `json:"publisher_signature_path"`
	ApprovedGrantPath      string `json:"approved_grant_path"`
	BootstrapSeedPath      string `json:"bootstrap_seed_path"`
}

type applianceInstallSpec struct {
	installationID string
	manifest       *appliancev1.ControllerApplianceManifest
	signature      []byte
	grant          *appliancev1.ApprovedGrant
	bootstrapSeed  []byte
}

type productionLifecycleAdapter struct {
	value paths
}

func newProductionLifecycleAdapter(value paths) LifecycleAdapter {
	adapter := &productionLifecycleAdapter{value: value}
	return newHelperLifecycleAdapter(value, lifecycleAdapterHooks{
		ApplianceInstall: adapter.applianceInstall,
		Bootstrap:        adapter.bootstrap,
		Register:         adapter.register,
		Reconnect:        adapter.reconnect,
		Observe:          adapter.observe,
	})
}

func (adapter *productionLifecycleAdapter) applianceInstall(ctx context.Context, request LifecycleRequest) error {
	connection, client, err := adapter.applianceClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()

	installationID, err := adapter.installationID(ctx, client, request)
	if err != nil {
		return err
	}
	if status, found, err := findAppliance(ctx, client, installationID); err != nil {
		return err
	} else if found {
		if status.GetRegistrationId() == "" {
			return errors.New("existing appliance has no registration identity")
		}
		return nil
	}

	spec, err := adapter.installSpec()
	if err != nil {
		return err
	}
	if spec.installationID != "" && spec.installationID != installationID {
		return fmt.Errorf("appliance installation id %q does not match configured id %q", installationID, spec.installationID)
	}
	deadline := lifecycleDeadline(ctx)
	stage, err := client.StageArtifact(ctx, &appliancev1.StageArtifactRequest{
		RequestId:          request.RequestID + ":stage",
		DeadlineUnixMillis: deadline,
		Manifest:           spec.manifest,
		PublisherSignature: spec.signature,
	})
	if err != nil {
		return fmt.Errorf("stage appliance artifact: %w", err)
	}
	artifactID := stage.GetArtifactId()
	if artifactID == "" {
		return errors.New("stage appliance artifact returned no artifact id")
	}
	grant := proto.Clone(spec.grant).(*appliancev1.ApprovedGrant)
	plan, err := client.PlanInstall(ctx, &appliancev1.PlanInstallRequest{
		InstallationId:     installationID,
		RequestId:          request.RequestID + ":plan-install",
		ExpectedRevision:   0,
		ArtifactId:         artifactID,
		DeadlineUnixMillis: deadline,
		ApprovedGrant:      grant,
	})
	if err != nil {
		return fmt.Errorf("plan appliance install: %w", err)
	}
	if plan.GetPlan().GetArtifactId() != artifactID {
		return errors.New("appliance install plan does not match staged artifact")
	}
	_, err = client.Install(ctx, &appliancev1.InstallRequest{
		InstallationId:      installationID,
		RequestId:           request.RequestID + ":install",
		ExpectedRevision:    0,
		ArtifactId:          artifactID,
		DeadlineUnixMillis:  deadline,
		ApprovedGrant:       grant,
		OpaqueBootstrapSeed: append([]byte(nil), spec.bootstrapSeed...),
	})
	for index := range spec.bootstrapSeed {
		spec.bootstrapSeed[index] = 0
	}
	if err != nil {
		return fmt.Errorf("install appliance: %w", err)
	}
	return nil
}

func (adapter *productionLifecycleAdapter) bootstrap(ctx context.Context, request LifecycleRequest) error {
	connection, client, err := adapter.applianceClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	installationID, err := adapter.installationID(ctx, client, request)
	if err != nil {
		return err
	}
	status, found, err := findAppliance(ctx, client, installationID)
	if err != nil {
		return err
	}
	if !found {
		return errors.New("appliance installation was not observed after install")
	}
	if status.GetEnabled() && status.GetApplianceState() == appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING {
		return nil
	}
	_, err = client.Activate(ctx, &appliancev1.ActivateRequest{
		InstallationId:     installationID,
		RequestId:          request.RequestID + ":activate",
		ExpectedRevision:   status.GetRevision(),
		DeadlineUnixMillis: lifecycleDeadline(ctx),
	})
	if err != nil {
		return fmt.Errorf("activate appliance: %w", err)
	}
	return nil
}

func (adapter *productionLifecycleAdapter) register(ctx context.Context, request LifecycleRequest) error {
	connection, client, err := adapter.applianceClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	installationID, err := adapter.installationID(ctx, client, request)
	if err != nil {
		return err
	}
	status, found, err := findAppliance(ctx, client, installationID)
	if err != nil {
		return err
	}
	if !found || status.GetRegistrationId() == "" {
		return errors.New("appliance registration was not observed")
	}
	return nil
}

func (adapter *productionLifecycleAdapter) reconnect(ctx context.Context, request LifecycleRequest) error {
	connection, client, err := adapter.applianceClient(ctx)
	if err != nil {
		return err
	}
	defer connection.Close()
	installationID, err := adapter.installationID(ctx, client, request)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(lifecycleAppliancePollInterval)
	defer ticker.Stop()
	for {
		status, found, statusErr := findAppliance(ctx, client, installationID)
		if statusErr != nil {
			return statusErr
		}
		if found && applianceReady(status) {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (adapter *productionLifecycleAdapter) observe(ctx context.Context, request LifecycleRequest) (LifecycleFacts, error) {
	connection, client, err := adapter.applianceClient(ctx)
	if err != nil {
		return LifecycleFacts{}, err
	}
	defer connection.Close()
	installationID, err := adapter.installationID(ctx, client, request)
	if err != nil {
		return LifecycleFacts{}, err
	}
	status, found, err := findAppliance(ctx, client, installationID)
	if err != nil {
		return LifecycleFacts{}, err
	}
	if !found {
		return LifecycleFacts{}, errors.New("target appliance was not observed")
	}
	host := readStatusContext(ctx, adapter.value)
	return LifecycleFacts{
		HostHealthy:       host.HostHealthy,
		ApplianceHealthy:  status.GetHealth().GetHealthy(),
		ControllerActive:  status.GetChannelState() == appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE,
		InventoryComplete: status.GetInventoryWatermark() > 0,
		SalixReady:        host.HostHealthy && applianceReady(status),
	}, nil
}

func (adapter *productionLifecycleAdapter) applianceClient(ctx context.Context) (interface{ Close() error }, appliancev1.ApplianceAdminServiceClient, error) {
	connection, err := operatorConnection(ctx, filepath.Join(adapter.value.applicationData, "operator.sock"))
	if err != nil {
		return nil, nil, errors.New("Agent VMM operator is not readable")
	}
	return connection, appliancev1.NewApplianceAdminServiceClient(connection), nil
}

func (adapter *productionLifecycleAdapter) installationID(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, request LifecycleRequest) (string, error) {
	if request.InstallationID != "" {
		return request.InstallationID, nil
	}
	if configured := os.Getenv(lifecycleInstallationIDEnv); configured != "" {
		return configured, nil
	}
	if config, err := adapter.readConfig(); err == nil && config.InstallationID != "" {
		return config.InstallationID, nil
	}
	response, err := client.List(ctx, &appliancev1.ListAppliancesRequest{})
	if err != nil {
		return "", fmt.Errorf("list appliance installations: %w", err)
	}
	if len(response.GetAppliances()) == 1 && response.GetAppliances()[0].GetInstallationId() != "" {
		return response.GetAppliances()[0].GetInstallationId(), nil
	}
	return "", errors.New("one appliance installation id is required")
}

func (adapter *productionLifecycleAdapter) installSpec() (applianceInstallSpec, error) {
	config, err := adapter.readConfig()
	if err != nil {
		return applianceInstallSpec{}, err
	}
	manifestBytes, err := os.ReadFile(config.ManifestPath)
	if err != nil {
		return applianceInstallSpec{}, fmt.Errorf("read appliance manifest: %w", err)
	}
	manifest := new(appliancev1.ControllerApplianceManifest)
	if err := proto.Unmarshal(manifestBytes, manifest); err != nil {
		return applianceInstallSpec{}, fmt.Errorf("decode appliance manifest: %w", err)
	}
	signature, err := os.ReadFile(config.PublisherSignaturePath)
	if err != nil {
		return applianceInstallSpec{}, fmt.Errorf("read appliance publisher signature: %w", err)
	}
	grantBytes, err := os.ReadFile(config.ApprovedGrantPath)
	if err != nil {
		return applianceInstallSpec{}, fmt.Errorf("read appliance approved grant: %w", err)
	}
	grant := new(appliancev1.ApprovedGrant)
	if err := proto.Unmarshal(grantBytes, grant); err != nil {
		return applianceInstallSpec{}, fmt.Errorf("decode appliance approved grant: %w", err)
	}
	seed, err := os.ReadFile(config.BootstrapSeedPath)
	if err != nil {
		return applianceInstallSpec{}, fmt.Errorf("read appliance bootstrap seed: %w", err)
	}
	if len(seed) == 0 {
		return applianceInstallSpec{}, errors.New("appliance bootstrap seed is empty")
	}
	return applianceInstallSpec{installationID: config.InstallationID, manifest: manifest, signature: signature, grant: grant, bootstrapSeed: seed}, nil
}

func (adapter *productionLifecycleAdapter) readConfig() (applianceLifecycleConfig, error) {
	path := os.Getenv(lifecycleApplianceConfigEnv)
	if path == "" {
		return applianceLifecycleConfig{}, errApplianceLifecycleConfigUnavailable
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return applianceLifecycleConfig{}, fmt.Errorf("read %s: %w", lifecycleApplianceConfigEnv, err)
	}
	var config applianceLifecycleConfig
	if err := json.Unmarshal(data, &config); err != nil {
		return applianceLifecycleConfig{}, fmt.Errorf("decode %s: %w", lifecycleApplianceConfigEnv, err)
	}
	if config.Version != 0 && config.Version != lifecycleApplianceConfigVersion {
		return applianceLifecycleConfig{}, fmt.Errorf("unsupported appliance lifecycle config version %d", config.Version)
	}
	for field, value := range map[string]string{
		"manifest_path":            config.ManifestPath,
		"publisher_signature_path": config.PublisherSignaturePath,
		"approved_grant_path":      config.ApprovedGrantPath,
		"bootstrap_seed_path":      config.BootstrapSeedPath,
	} {
		if value == "" || !filepath.IsAbs(value) {
			return applianceLifecycleConfig{}, fmt.Errorf("appliance config %s must be an absolute path", field)
		}
	}
	return config, nil
}

func findAppliance(ctx context.Context, client appliancev1.ApplianceAdminServiceClient, installationID string) (*appliancev1.ApplianceStatus, bool, error) {
	response, err := client.List(ctx, &appliancev1.ListAppliancesRequest{})
	if err != nil {
		return nil, false, fmt.Errorf("list appliance installations: %w", err)
	}
	for _, status := range response.GetAppliances() {
		if status.GetInstallationId() == installationID {
			return status, true, nil
		}
	}
	return nil, false, nil
}

func applianceReady(status *appliancev1.ApplianceStatus) bool {
	return status != nil && status.GetRegistrationId() != "" && status.GetEnabled() &&
		status.GetApplianceState() == appliancev1.ApplianceState_APPLIANCE_STATE_RUNNING &&
		status.GetChannelState() == appliancev1.ControllerChannelState_CONTROLLER_CHANNEL_STATE_ACTIVE &&
		status.GetInventoryWatermark() > 0 && status.GetHealth().GetHealthy()
}

func lifecycleDeadline(ctx context.Context) int64 {
	if deadline, ok := ctx.Deadline(); ok {
		return deadline.UnixMilli()
	}
	return time.Now().Add(defaultCoordinatorStepTimeout).UnixMilli()
}
