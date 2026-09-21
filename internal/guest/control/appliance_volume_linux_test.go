//go:build linux

package control

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestApplianceVolumeCreatesImageAndClearsKey(t *testing.T) {
	key := make([]byte, 32)
	for index := range key {
		key[index] = byte(index + 1)
	}
	commands := make([]string, 0)
	imageRoot := t.TempDir()
	if err := os.MkdirAll(filepath.Join(imageRoot, "controller-a"), 0o700); err != nil {
		t.Fatal(err)
	}
	manager := applianceVolumeManager{
		command: func(_ context.Context, input []byte, name string, arguments ...string) ([]byte, error) {
			commands = append(commands, name+" "+arguments[0])
			if name == "/sbin/losetup" && arguments[0] == "--find" {
				return []byte("/dev/loop0\n"), nil
			}
			if arguments[0] == "luksUUID" {
				return nil, errors.New("not luks")
			}
			if (arguments[0] == "luksFormat" || arguments[0] == "open") && len(input) != 32 {
				t.Fatalf("%s key bytes = %d", arguments[0], len(input))
			}
			return nil, nil
		},
		mkdir:     func(string, os.FileMode) error { return nil },
		mount:     func(string, string, string, uintptr, string) error { return nil },
		unmount:   func(string, int) error { return nil },
		imageRoot: imageRoot,
	}
	identity := "01234567-89ab-4cde-8fab-0123456789ab"
	workingKey := append([]byte(nil), key...)
	volume, err := manager.unlock(context.Background(), "controller-a", "volume-a", "", identity, 1<<30, workingKey)
	if err != nil {
		t.Fatal(err)
	}
	if volume.identity != identity || volume.mapper != "avmm-7ba589c761abd60ec4522bf2" {
		t.Fatalf("volume = %+v", volume)
	}
	if want := []string{"/sbin/losetup --find", "/sbin/cryptsetup luksUUID", "/sbin/cryptsetup luksFormat", "/sbin/cryptsetup open", "/sbin/mkfs.ext4 -F"}; !reflect.DeepEqual(commands, want) {
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
			imageRoot := t.TempDir()
			imagePath := filepath.Join(imageRoot, "controller-a", "volume-a.luks")
			if err := os.MkdirAll(filepath.Dir(imagePath), 0o700); err != nil {
				t.Fatal(err)
			}
			file, err := os.OpenFile(imagePath, os.O_CREATE|os.O_RDWR, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			if err := file.Truncate(1 << 30); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			manager := applianceVolumeManager{
				command: func(_ context.Context, _ []byte, _ string, arguments ...string) ([]byte, error) {
					if arguments[0] == "--find" {
						return []byte("/dev/loop0\n"), nil
					}
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
				mkdir: os.MkdirAll, mount: func(string, string, string, uintptr, string) error { return nil }, unmount: func(string, int) error { return nil }, imageRoot: imageRoot,
			}
			_, err = manager.unlock(context.Background(), "controller-a", "volume-a", "", test.identity, 1<<30, make([]byte, 32))
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

func TestApplianceVolumeConversionReplacesInterruptedTemporaryCopy(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "legacy.luks")
	data := make([]byte, 2<<20)
	copy(data[4096:], []byte("durable-controller-data"))
	if err := os.WriteFile(source, data, 0o600); err != nil {
		t.Fatal(err)
	}
	imageRoot := filepath.Join(root, "images")
	if err := os.MkdirAll(imageRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	installationRoot := filepath.Join(imageRoot, "controller-a")
	if err := os.MkdirAll(installationRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(installationRoot, "volume-a.luks.converting"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := applianceVolumeManager{
		deviceByIdentifier: func(identifier string) (string, error) {
			if identifier != "legacy-device" {
				t.Fatalf("identifier = %q", identifier)
			}
			return source, nil
		},
		mkdir: os.MkdirAll, imageRoot: imageRoot,
	}
	path, initialize, err := manager.prepareImage(context.Background(), "controller-a", "volume-a", "legacy-device", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if initialize {
		t.Fatal("converted image was marked for formatting")
	}
	converted, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(converted, data) {
		t.Fatal("converted image does not match the legacy data")
	}
	if _, err := os.Stat(path + ".converting"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary conversion file remains: %v", err)
	}
}

func TestApplianceVolumeDeleteIsIdempotent(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "controller-a", "volume-a.luks")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := applianceVolumeManager{imageRoot: root}
	for range 2 {
		if err := manager.delete("controller-a", "volume-a"); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("data image remains: %v", err)
	}
}

func TestApplianceVolumeDeleteBindsInstallationAndVolume(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "controller-b", "volume-a.luks")
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	manager := applianceVolumeManager{imageRoot: root}
	if err := manager.delete("controller-a", "volume-a"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("mismatched installation deleted data: %v", err)
	}
}
