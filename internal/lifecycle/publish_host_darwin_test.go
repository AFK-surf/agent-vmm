package lifecycle

import (
	"errors"
	"github.com/AFK-surf/agent-vmm/internal/bundle"
	"os"
	"path/filepath"
	"testing"
)

func publicationFixture(t *testing.T) (paths, string, publicationActions) {
	t.Helper()
	p := testPaths(t.TempDir())
	p.app = sharedHostApp(p.home)
	source := filepath.Join(t.TempDir(), "Agent VMM Host.app")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(source, "Contents", "Resources"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, bundle.HostInventoryRelativePath), []byte(`{"release_id":"release-1"}`), 0600); err != nil {
		t.Fatal(err)
	}
	a := publicationActions{validate: func(string) (string, error) { return "release-1", nil }, copy: func(_, target string) error { return os.MkdirAll(target, 0700) }}
	return p, source, a
}

func TestPublicationReplaysWithoutOverwriting(t *testing.T) {
	p, s, a := publicationFixture(t)
	got, err := publishHost(p, s, a)
	if err != nil || got.Outcome != "published" {
		t.Fatalf("%+v %v", got, err)
	}
	marker := filepath.Join(p.app, "retained")
	if err = os.WriteFile(marker, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	a.copy = func(string, string) error { t.Fatal("replay copied"); return nil }
	got, err = publishHost(p, s, a)
	if err != nil || got.Outcome != "already_present" {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err = os.Stat(marker); err != nil {
		t.Fatal(err)
	}
	a.validate = func(path string) (string, error) {
		if path == p.app {
			return "different", nil
		}
		return "release-1", nil
	}
	if _, err = publishHost(p, s, a); err == nil {
		t.Fatal("overwrote a different release")
	}
}

func TestPublicationChecksPolicyInsideOwnerLock(t *testing.T) {
	for _, state := range []maintenanceState{{Version: 1, Uninstalled: true}, {Version: 1, ActiveRequest: "maint-1"}} {
		t.Run(state.ActiveRequest+"policy", func(t *testing.T) {
			p, s, a := publicationFixture(t)
			directory, unlock, err := lockMaintenance(p)
			if err != nil {
				t.Fatal(err)
			}
			if err = writeMaintenanceJSON(directory, "state.json", state); err != nil {
				t.Fatal(err)
			}
			unlock()
			a.copy = func(string, string) error { t.Fatal("blocked publication copied"); return nil }
			if _, err = publishHost(p, s, a); err == nil {
				t.Fatal("publication ignored owner policy")
			}
			if _, err = os.Stat(p.app); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("blocked target exists", err)
			}
		})
	}
}

func TestPublicationHoldsOwnerLockThroughCopy(t *testing.T) {
	p, s, a := publicationFixture(t)
	a.copy = func(_, target string) error {
		if _, release, err := lockMaintenance(p); err == nil {
			release()
			t.Fatal("publication released owner lock before copy")
		}
		return os.MkdirAll(target, 0700)
	}
	if _, err := publishHost(p, s, a); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationCannotReplaceConcurrentTarget(t *testing.T) {
	p, s, a := publicationFixture(t)
	a.copy = func(_, target string) error {
		if err := os.MkdirAll(p.app, 0700); err != nil {
			return err
		}
		if err := os.WriteFile(filepath.Join(p.app, "other"), []byte("keep"), 0600); err != nil {
			return err
		}
		return os.MkdirAll(target, 0700)
	}
	if _, err := publishHost(p, s, a); err == nil {
		t.Fatal("publication replaced concurrent target")
	}
	if _, err := os.Stat(filepath.Join(p.app, "other")); err != nil {
		t.Fatal(err)
	}
}

func TestPublicationRejectsInvalidOrChangedSource(t *testing.T) {
	p, s, a := publicationFixture(t)
	a.validate = func(string) (string, error) { return "", errors.New("invalid signature") }
	if _, err := publishHost(p, s, a); err == nil {
		t.Fatal("published invalid signature")
	}
	a.validate = func(path string) (string, error) {
		if path == s {
			return "source", nil
		}
		return "changed", nil
	}
	if _, err := publishHost(p, s, a); err == nil {
		t.Fatal("published changed copy")
	}
	if _, err := os.Stat(p.app); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("invalid target exists", err)
	}
}

func TestSharedHostFlagKeepsArbitraryTargetRestricted(t *testing.T) {
	request, args, err := extractServiceRequest([]string{"--shared-host", "--service-type", "auto", "--request-id", "abc"})
	if err != nil || !request.sharedHost || len(args) != 2 {
		t.Fatalf("%+v %v %v", request, args, err)
	}
	if _, _, err = extractServiceRequest([]string{"--shared-host", "--service-app", "/elsewhere/Agent VMM Host.app"}); err == nil {
		t.Fatal("accepted arbitrary target with shared target")
	}
}
