//go:build linux

package control

import (
	"math"
	"testing"

	"github.com/AFK-surf/agent-vmm/internal/guestdisk"
)

func TestSystemNamespaceCommitmentCountsUsageNotQuota(t *testing.T) {
	facts := map[uint32]namespaceDiskCommitment{
		20001: {namespace: "system.mcp", limit: 3 << 30, usage: 200 << 20},
		20002: {namespace: "system.gitea", limit: 3 << 30, usage: 1 << 30},
	}
	committed, overflow := systemNamespaceCommitment(facts, 0, false, 2<<30)
	if overflow || committed != 3<<30+200<<20 || committed > guestdisk.ControlSystemBytes {
		t.Fatalf("new namespace committed=%d overflow=%t", committed, overflow)
	}
	committed, overflow = systemNamespaceCommitment(facts, 20002, true, 3<<30+512<<20)
	if overflow || committed != 3<<30+712<<20 {
		t.Fatalf("own quota growth committed=%d overflow=%t", committed, overflow)
	}
	facts[20003] = namespaceDiskCommitment{namespace: "system.full", usage: math.MaxUint64}
	if _, overflow := systemNamespaceCommitment(facts, 0, false, 1); !overflow {
		t.Fatal("usage overflow was accepted")
	}
}
