//go:build linux

package build

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
)

// ArchiveResult is the immutable result of a build-only Dockerfile execution.
// The image manifest digest is the OCI descriptor selected from index.json.
type ArchiveResult struct {
	ArchiveSHA256  []byte
	ManifestDigest string
	Size           uint64
	CacheHit       bool
}

func hashArchive(source io.ReadSeeker) ([]byte, uint64, error) {
	if _, err := source.Seek(0, io.SeekStart); err != nil {
		return nil, 0, err
	}
	hash := sha256.New()
	count, err := io.Copy(hash, source)
	if err != nil {
		return nil, 0, fmt.Errorf("hash OCI archive: %w", err)
	}
	if count <= 0 {
		return nil, 0, errors.New("OCI archive is empty")
	}
	return hash.Sum(nil), uint64(count), nil
}
