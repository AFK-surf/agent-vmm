//go:build darwin

package host

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/appliancecontent"
	"google.golang.org/protobuf/proto"
)

func (controller *Controller) applianceContentStageLock() *sync.Mutex {
	value, _ := controller.integrationLocks.LoadOrStore("appliance-content-stage", new(sync.Mutex))
	return value.(*sync.Mutex)
}

// Caller holds content-stage admission. The same existing remote admission
// lock protects Install/Update acceptance through the final unlink; collection
// never waits for a lifecycle operation while a content stream holds admission.
func (controller *Controller) collectApplianceContent(ctx context.Context) (int, error) {
	if !controller.remoteAdmissionMu.TryLock() {
		return 0, nil
	}
	defer controller.remoteAdmissionMu.Unlock()
	artifacts, operations, err := controller.store.ApplianceContentRetentionFacts(ctx)
	if err != nil {
		return 0, err
	}
	retained := make(map[string]bool)
	retain := func(ref *appliancev1.ApplianceContentReference) error {
		if err := appliancecontent.ValidateReference(ref); err != nil {
			return fmt.Errorf("retain appliance content %s: %w", ref.GetImageReference(), err)
		}
		retained[appliancecontent.ArchiveName(ref)] = true
		return nil
	}
	retainArtifact := func(id string) error {
		artifact, err := controller.store.GetApplianceArtifact(ctx, id)
		if err != nil {
			return err
		}
		artifacts = append(artifacts, artifact)
		return nil
	}
	for _, operation := range operations {
		switch appliancev1.ApplianceOperationKind(operation.Kind) {
		case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE_CONTENT:
			header := new(appliancev1.StageContentHeader)
			if err := proto.Unmarshal(operation.Receipt, header); err != nil {
				return 0, err
			}
			if err := retain(header.GetContentRef()); err != nil {
				return 0, err
			}
		case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_STAGE:
			if err := retainArtifact(string(operation.Receipt)); err != nil {
				return 0, err
			}
		case appliancev1.ApplianceOperationKind_APPLIANCE_OPERATION_KIND_UPDATE:
			progress, err := updateProgress(operation)
			if err != nil {
				return 0, err
			}
			if err := retainArtifact(progress.GetRequest().GetArtifactId()); err != nil {
				return 0, err
			}
		}
	}
	for _, artifact := range artifacts {
		refs, err := artifactContentReferences(artifact)
		if err != nil {
			return 0, err
		}
		for _, ref := range refs {
			if err := retain(ref); err != nil {
				return 0, err
			}
		}
	}
	root := controller.applianceContentRoot()
	directory, err := os.Open(root)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer directory.Close()
	// Only bounded archive collection uses the admission lock. User volumes and
	// installation/artifact/operation receipts are never collection candidates.
	entries, err := directory.ReadDir(-1)
	if err != nil {
		return 0, err
	}
	removed := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		name := entry.Name()
		archive := strings.TrimSuffix(name, ".pb")
		_, hashErr := hex.DecodeString(strings.TrimSuffix(archive, ".oci.tar"))
		staleTemporary := strings.HasPrefix(name, ".content-stage-") || strings.HasPrefix(name, ".content-descriptor-")
		if !staleTemporary && (len(archive) != 64+len(".oci.tar") || hashErr != nil || !strings.HasSuffix(archive, ".oci.tar") || retained[archive]) {
			continue
		}
		if entry.IsDir() {
			continue
		}
		if err := os.Remove(filepath.Join(root, name)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return 0, err
		}
		removed++
		if removed >= 256 {
			break
		}
	}
	return removed, nil
}

func (controller *Controller) requireApplianceContent(ctx context.Context, manifest *appliancev1.ControllerApplianceManifest) error {
	for _, ref := range manifest.GetContentRefs() {
		available, err := appliancecontent.Available(controller.applianceContentRoot(), ref)
		if err != nil {
			return err
		}
		if !available {
			return errors.New("approved appliance content must be staged before admission")
		}
	}
	return ctx.Err()
}
