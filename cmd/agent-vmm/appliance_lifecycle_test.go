//go:build darwin || linux

package main

import (
	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"testing"
)

func TestSelectApplianceInstallationUsesExactProductAndRejectsAmbiguity(t *testing.T) {
	statuses := []*appliancev1.ApplianceStatus{{InstallationId: "one", ProductId: "product", Revision: 17}, {InstallationId: "two", ProductId: "product", Revision: 18}, {InstallationId: "other", ProductId: "else"}}
	selected, err := selectApplianceInstallation(statuses, "product", "one")
	if err != nil || selected.GetRevision() != 17 {
		t.Fatalf("selection=%+v err=%v", selected, err)
	}
	if _, err := selectApplianceInstallation(statuses, "product", ""); err == nil {
		t.Fatal("ambiguous product selected")
	}
	if _, err := selectApplianceInstallation(statuses, "else", "one"); err == nil {
		t.Fatal("product mismatch selected")
	}
}
