package main

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type ociDescriptor struct {
	MediaType string            `json:"mediaType"`
	Digest    string            `json:"digest"`
	Size      int64             `json:"size"`
	Platform  map[string]string `json:"platform,omitempty"`
}

func runReferenceProcess(mode string) error {
	switch mode {
	case "service":
		if err := os.WriteFile("/data/service-ready", []byte("durable\n"), 0o600); err != nil {
			return err
		}
		listener, err := net.Listen("tcp", ":8080")
		if err != nil {
			return err
		}
		for {
			connection, err := listener.Accept()
			if err != nil {
				return err
			}
			go func() { _, _ = io.Copy(connection, connection); _ = connection.Close() }()
		}
	case "sidecar":
		deadline := time.Now().Add(10 * time.Second)
		for {
			value, err := os.ReadFile("/data/service-ready")
			if err == nil && string(value) == "durable\n" {
				break
			}
			if time.Now().After(deadline) {
				return errors.New("durable service volume is unavailable")
			}
			time.Sleep(50 * time.Millisecond)
		}
		return waitReferenceSignal()
	case "idle":
		return waitReferenceSignal()
	case "task":
		connection, err := net.DialTimeout("tcp", "reference-service:8080", 10*time.Second)
		if err != nil {
			return err
		}
		defer connection.Close()
		if _, err := connection.Write([]byte("reference-route")); err != nil {
			return err
		}
		response := make([]byte, len("reference-route"))
		if _, err := io.ReadFull(connection, response); err != nil || string(response) != "reference-route" {
			return errors.Join(errors.New("service route echo mismatch"), err)
		}
		return nil
	default:
		return errors.New("unknown reference workload mode")
	}
}

func waitReferenceSignal() error {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()
	<-ctx.Done()
	return nil
}

func buildReferenceOCI() ([]byte, string, error) {
	executable, err := os.ReadFile("/proc/self/exe")
	if err != nil {
		return nil, "", err
	}
	var layer bytes.Buffer
	layerTar := tar.NewWriter(&layer)
	if err := layerTar.WriteHeader(&tar.Header{Name: "usr/bin/reference-workload", Mode: 0o755, Size: int64(len(executable)), ModTime: time.Unix(0, 0), Uid: 0, Gid: 0}); err != nil {
		return nil, "", err
	}
	if _, err := layerTar.Write(executable); err != nil {
		return nil, "", err
	}
	if err := layerTar.Close(); err != nil {
		return nil, "", err
	}
	layerHash := sha256.Sum256(layer.Bytes())
	layerDigest := "sha256:" + hex.EncodeToString(layerHash[:])
	config, _ := json.Marshal(map[string]any{"architecture": "arm64", "os": "linux", "config": map[string]any{"Entrypoint": []string{"/usr/bin/reference-workload"}}, "rootfs": map[string]any{"type": "layers", "diff_ids": []string{layerDigest}}})
	configHash := sha256.Sum256(config)
	configDescriptor := ociDescriptor{MediaType: "application/vnd.oci.image.config.v1+json", Digest: "sha256:" + hex.EncodeToString(configHash[:]), Size: int64(len(config))}
	manifest, _ := json.Marshal(map[string]any{"schemaVersion": 2, "mediaType": "application/vnd.oci.image.manifest.v1+json", "config": configDescriptor, "layers": []ociDescriptor{{MediaType: "application/vnd.oci.image.layer.v1.tar", Digest: layerDigest, Size: int64(layer.Len())}}})
	manifestHash := sha256.Sum256(manifest)
	manifestDigest := "sha256:" + hex.EncodeToString(manifestHash[:])
	index, _ := json.Marshal(map[string]any{"schemaVersion": 2, "manifests": []ociDescriptor{{MediaType: "application/vnd.oci.image.manifest.v1+json", Digest: manifestDigest, Size: int64(len(manifest)), Platform: map[string]string{"architecture": "arm64", "os": "linux"}}}})
	blobs := map[string][]byte{hex.EncodeToString(layerHash[:]): layer.Bytes(), hex.EncodeToString(configHash[:]): config, hex.EncodeToString(manifestHash[:]): manifest}
	var archive bytes.Buffer
	writer := tar.NewWriter(&archive)
	entries := []struct {
		name  string
		value []byte
	}{{"oci-layout", []byte("{\"imageLayoutVersion\":\"1.0.0\"}\n")}, {"index.json", index}}
	for _, entry := range entries {
		if err := writeReferenceTarFile(writer, entry.name, entry.value); err != nil {
			return nil, "", err
		}
	}
	for _, digest := range []string{hex.EncodeToString(configHash[:]), hex.EncodeToString(layerHash[:]), hex.EncodeToString(manifestHash[:])} {
		if err := writeReferenceTarFile(writer, "blobs/sha256/"+digest, blobs[digest]); err != nil {
			return nil, "", err
		}
	}
	if err := writer.Close(); err != nil {
		return nil, "", err
	}
	return archive.Bytes(), manifestDigest, nil
}

func writeReferenceTarFile(writer *tar.Writer, name string, value []byte) error {
	if err := writer.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(value)), ModTime: time.Unix(0, 0)}); err != nil {
		return err
	}
	_, err := writer.Write(value)
	return err
}
