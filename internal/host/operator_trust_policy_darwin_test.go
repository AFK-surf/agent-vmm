//go:build darwin

package host

import (
	"testing"

	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
)

func TestManagedPolicyOnlyBlocksPersonalMeshAdmission(t *testing.T) {
	current := &trustv1.MeshSnapshot{ActiveMembers: []*trustv1.MeshMembership{{DeviceId: "a", RootKeyRevision: 1}, {DeviceId: "b", RootKeyRevision: 2}}}
	refresh := &trustv1.MeshSnapshot{ActiveMembers: []*trustv1.MeshMembership{{DeviceId: "a", RootKeyRevision: 1}, {DeviceId: "b", RootKeyRevision: 2}}}
	revoke := &trustv1.MeshSnapshot{ActiveMembers: []*trustv1.MeshMembership{{DeviceId: "a", RootKeyRevision: 1}}}
	join := &trustv1.MeshSnapshot{ActiveMembers: []*trustv1.MeshMembership{{DeviceId: "a", RootKeyRevision: 1}, {DeviceId: "b", RootKeyRevision: 2}, {DeviceId: "c", RootKeyRevision: 1}}}
	rootReplacement := &trustv1.MeshSnapshot{ActiveMembers: []*trustv1.MeshMembership{{DeviceId: "a", RootKeyRevision: 2}, {DeviceId: "b", RootKeyRevision: 2}}}

	if addsActiveMember(current, refresh) || addsActiveMember(current, revoke) {
		t.Fatal("freshness or revoke projection was classified as pairing")
	}
	if !addsActiveMember(current, join) || !addsActiveMember(current, rootReplacement) {
		t.Fatal("new or replaced member root was not classified as pairing")
	}
}
