//go:build linux

package control

import (
	"context"
	"fmt"
	"strings"
)

func imagePublicationKey(namespace, reference, target string) string {
	return "image-publication\x00" + namespace + "\x00" + reference + "\x00" + target
}

// Unknown publication results retain the new candidate without replacing an
// existing grant. An acknowledged retry indexes the owner before releasing it.
func (r *Runtime) pinImagePublication(namespace, reference, target string) (bool, error) {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return false, err
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	key := imagePublicationKey(namespace, reference, target)
	_, existed := inventory.imagePins[key]
	inventory.setImagePinLocked(key, target)
	return !existed, nil
}

func (r *Runtime) releaseImagePin(key string) {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	inventory.setImagePinLocked(key, "")
}

// Initial receipt uncertainty blocks orphan deletion, not runtime service.
// Same-daemon recovery merges facts: an absent record cannot disprove a write
// whose transport failed while containerd could still commit it.
func (r *Runtime) ensureSourceImagePins(ctx context.Context) error {
	inventory, err := r.requireRuntimeInventory()
	if err != nil {
		return err
	}
	inventory.mu.RLock()
	ready := inventory.sourcePinsReady
	inventory.mu.RUnlock()
	if ready {
		return nil
	}
	inventory.mu.Lock()
	defer inventory.mu.Unlock()
	if inventory.sourcePinsReady {
		return nil
	}
	labels, err := r.client.NamespaceService().Labels(ctx, runtimeContainerdNamespace)
	if err != nil {
		return fmt.Errorf("recover image retention pins: %w", err)
	}
	pins := make(map[string]string)
	for key, text := range labels {
		if !strings.HasPrefix(key, labelPrefix+"image-operation.") {
			continue
		}
		target, err := imageReceiptSourcePin(text)
		if err != nil {
			return fmt.Errorf("recover image retention pins: %w", err)
		}
		if target != "" {
			pins[key] = target
		}
	}
	for key, target := range pins {
		inventory.setImagePinLocked(key, target)
	}
	inventory.sourcePinsReady = true
	return nil
}
