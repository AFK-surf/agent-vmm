package main

import (
	"archive/tar"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type descriptor struct {
	MediaType string            `json:"mediaType"`
	Digest    string            `json:"digest"`
	Size      int64             `json:"size"`
	Platform  map[string]string `json:"platform,omitempty"`
}

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: oci ROOTFS_TAR OUTPUT_OCI_TAR")
		os.Exit(2)
	}
	directory, err := os.MkdirTemp("", "agent-vmm-builder-oci-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(directory)
	blobs := filepath.Join(directory, "blobs", "sha256")
	if err := os.MkdirAll(blobs, 0o755); err != nil {
		panic(err)
	}
	layer, err := copyBlob(os.Args[1], blobs)
	check(err)
	config := mustJSON(map[string]any{"architecture": "arm64", "os": "linux", "rootfs": map[string]any{"type": "layers", "diff_ids": []string{layer.Digest}}})
	configDescriptor, err := writeBlob(config, "application/vnd.oci.image.config.v1+json", blobs)
	check(err)
	manifest := mustJSON(map[string]any{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json", "config": configDescriptor, "layers": []descriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: layer.Digest, Size: layer.Size}}})
	manifestDescriptor, err := writeBlob(manifest, "application/vnd.oci.image.manifest.v1+json", blobs)
	check(err)
	manifestDescriptor.Platform = map[string]string{"architecture": "arm64", "os": "linux"}
	check(os.WriteFile(filepath.Join(directory, "oci-layout"), []byte("{\"imageLayoutVersion\":\"1.0.0\"}\n"), 0o644))
	check(os.WriteFile(filepath.Join(directory, "index.json"), mustJSON(map[string]any{"schemaVersion": 2, "manifests": []descriptor{manifestDescriptor}}), 0o644))
	check(writeArchive(directory, os.Args[2]))
}

func copyBlob(source, directory string) (descriptor, error) {
	input, err := os.Open(source)
	if err != nil {
		return descriptor{}, err
	}
	defer input.Close()
	temporary, err := os.CreateTemp(directory, "layer-")
	if err != nil {
		return descriptor{}, err
	}
	hash := sha256.New()
	size, copyErr := io.Copy(io.MultiWriter(temporary, hash), input)
	closeErr := temporary.Close()
	if copyErr != nil {
		return descriptor{}, copyErr
	}
	if closeErr != nil {
		return descriptor{}, closeErr
	}
	digest := hex.EncodeToString(hash.Sum(nil))
	if err := os.Rename(temporary.Name(), filepath.Join(directory, digest)); err != nil {
		return descriptor{}, err
	}
	return descriptor{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: "sha256:" + digest, Size: size}, nil
}

func writeBlob(data []byte, mediaType, directory string) (descriptor, error) {
	hash := sha256.Sum256(data)
	digest := hex.EncodeToString(hash[:])
	if err := os.WriteFile(filepath.Join(directory, digest), data, 0o644); err != nil {
		return descriptor{}, err
	}
	return descriptor{MediaType: mediaType, Digest: "sha256:" + digest, Size: int64(len(data))}, nil
}

func writeArchive(directory, destination string) error {
	output, err := os.Create(destination)
	if err != nil {
		return err
	}
	writer := tar.NewWriter(output)
	entries := []string{}
	err = filepath.Walk(directory, func(path string, info os.FileInfo, err error) error {
		if err == nil && path != directory {
			entries = append(entries, path)
		}
		return err
	})
	sort.Strings(entries)
	for _, path := range entries {
		info, statErr := os.Stat(path)
		if statErr != nil {
			err = statErr
			break
		}
		header, headerErr := tar.FileInfoHeader(info, "")
		if headerErr != nil {
			err = headerErr
			break
		}
		header.Name, _ = filepath.Rel(directory, path)
		header.ModTime = time.Unix(0, 0)
		header.AccessTime = header.ModTime
		header.ChangeTime = header.ModTime
		header.Uid, header.Gid = 0, 0
		if err = writer.WriteHeader(header); err != nil {
			break
		}
		if info.IsDir() {
			continue
		}
		file, openErr := os.Open(path)
		if openErr != nil {
			err = openErr
			break
		}
		_, err = io.Copy(writer, file)
		file.Close()
		if err != nil {
			break
		}
	}
	err = errors.Join(err, writer.Close(), output.Close())
	return err
}

func mustJSON(value any) []byte {
	data, err := json.Marshal(value)
	check(err)
	return data
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}
