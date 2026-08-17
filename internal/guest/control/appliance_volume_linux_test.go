//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
)

func TestApplianceVolumeRequiresExplicitInitializationAndClearsKey(t *testing.T) {
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(index + 1)
	}
	commands := make([]string, 0)
	manager := applianceVolumeManager{
		deviceByIdentifier: func(identifier string) (string, error) {
			if identifier != "AVMM_DATA_A" {
				t.Fatalf("device identifier = %q", identifier)
			}
			return "/dev/vdc", nil
		},
		command: func(_ context.Context, input []byte, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, name+" "+arguments[0])
			if arguments[0] == "luksUUID" {
				return nil, errors.New("not luks")
			}
			if (arguments[0] == "luksFormat" || arguments[0] == "open") && len(input) != 32 {
				t.Fatalf("%s key bytes = %d", arguments[0], len(input))
			}
			return nil, nil
		},
		mkdir:   func(string, os.FileMode) error { return nil },
		mount:   func(string, string, string, uintptr, string) error { return nil },
		unmount: func(string, int) error { return nil },
	}
	identity := "01234567-89ab-4cde-8fab-0123456789ab"
	if _, err := manager.unlock(context.Background(), "controller-a", "AVMM_DATA_A", identity, append([]byte(nil), key...), false); err == nil {
		t.Fatal("uninitialized volume unlocked without explicit initialization")
	}
	commands = nil
	workingKey := append([]byte(nil), key...)
	volume, err := manager.unlock(context.Background(), "controller-a", "AVMM_DATA_A", identity, workingKey, true)
	if err != nil {
		t.Fatal(err)
	}
	if volume.identity != identity || volume.mapper != "avmm-7ba589c761abd60ec4522bf2" {
		t.Fatalf("volume = %+v", volume)
	}
	if want := []string{"/sbin/cryptsetup luksUUID", "/sbin/cryptsetup luksFormat", "/sbin/cryptsetup open", "/sbin/mkfs.ext4 -F"}; !reflect.DeepEqual(commands, want) {
		t.Fatalf("commands = %q, want %q", commands, want)
	}
	for _, value := range workingKey {
		if value != 0 {
			t.Fatal("volume key was not cleared")
		}
	}
}

func TestApplianceVolumeWrongIdentityAndKeySealWithoutFormatting(t *testing.T) {
	for _, test := range []struct {
		name       string
		identity   string
		openError  error
		wantReason string
	}{
		{name: "identity", identity: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee", wantReason: "VOLUME_IDENTITY_MISMATCH"},
		{name: "key", identity: "01234567-89ab-4cde-8fab-0123456789ab", openError: errors.New("wrong key"), wantReason: "VOLUME_UNLOCK_FAILED"},
	} {
		t.Run(test.name, func(t *testing.T) {
			formatted := false
			manager := applianceVolumeManager{
				deviceByIdentifier: func(string) (string, error) { return "/dev/vdc", nil },
				command: func(_ context.Context, _ []byte, _ string, arguments ...string) ([]byte, error) {
					switch arguments[0] {
					case "luksUUID":
						return []byte("01234567-89ab-4cde-8fab-0123456789ab\n"), nil
					case "luksFormat":
						formatted = true
					case "open":
						return nil, test.openError
					}
					return nil, nil
				},
				mkdir: func(string, os.FileMode) error { return nil }, mount: func(string, string, string, uintptr, string) error { return nil }, unmount: func(string, int) error { return nil },
			}
			_, err := manager.unlock(context.Background(), "controller-a", "AVMM_DATA_A", test.identity, make([]byte, 32), false)
			var sealed sealedVolumeError
			if !errors.As(err, &sealed) || sealed.reason != test.wantReason {
				t.Fatalf("error = %#v", err)
			}
			if formatted {
				t.Fatal("existing volume was reformatted")
			}
		})
	}
}
