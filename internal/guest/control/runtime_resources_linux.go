//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/namespaces"
	"github.com/containerd/errdefs"
	digest "github.com/opencontainers/go-digest"
)

const (
	runtimeOwnerLabel      = labelPrefix + "owner-namespace"
	runtimeExternalIDLabel = labelPrefix + "external-id"
	runtimeImageLabel      = labelPrefix + "logical-image"
)

func runtimeImageReference(value digest.Digest) string {
	return "agent-vmm.internal/materialized@" + value.String()
}

func runtimeContext(ctx context.Context) context.Context {
	return namespaces.WithNamespace(ctx, runtimeContainerdNamespace)
}

func runtimeResourceKey(kind, namespaceID, externalID string) string {
	sum := sha256.Sum256([]byte(namespaceID))
	return kind + "-" + hex.EncodeToString(sum[:12]) + "-" + externalID
}

func runtimeContainerKey(namespaceID, containerID string) string {
	return runtimeResourceKey("container", namespaceID, containerID)
}

func runtimeSnapshotKey(namespaceID, containerID string) string {
	return runtimeResourceKey("snapshot", namespaceID, containerID)
}

func ensureRuntimeContainerdNamespace(ctx context.Context, client *containerd.Client) error {
	err := client.NamespaceService().Create(ctx, runtimeContainerdNamespace, map[string]string{
		labelPrefix + "role": "runtime-internal",
	})
	if errdefs.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func ownedRuntimeContainer(ctx context.Context, client *containerd.Client, namespaceID, containerID string) (containerd.Container, error) {
	container, err := client.LoadContainer(runtimeContext(ctx), runtimeContainerKey(namespaceID, containerID))
	if err != nil {
		return nil, err
	}
	labels, err := container.Labels(runtimeContext(ctx))
	if err != nil {
		return nil, err
	}
	if labels[runtimeOwnerLabel] != namespaceID || labels[runtimeExternalIDLabel] != containerID {
		return nil, errors.New("runtime container ownership mismatch")
	}
	return container, nil
}
