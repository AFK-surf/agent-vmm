package appliancecontent

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"os"
	"path/filepath"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"github.com/AFK-surf/agent-vmm/internal/atomicfile"
	"github.com/AFK-surf/agent-vmm/internal/ociarchive"
	digest "github.com/opencontainers/go-digest"
	"google.golang.org/protobuf/proto"
)

func Available(root string, content *appliancev1.ApplianceContentReference) (bool, error) {
	if err := ValidateReference(content); err != nil {
		return false, err
	}
	info, err := os.Lstat(filepath.Join(root, ArchiveName(content)))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() || uint64(info.Size()) != content.GetArchiveSize() {
		return false, errors.New("content archive size or type conflicts with its identity")
	}
	file, err := os.Open(filepath.Join(root, ArchiveName(content)+".pb"))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	defer file.Close()
	encoded, err := io.ReadAll(io.LimitReader(file, 4097))
	if err != nil || len(encoded) > 4096 {
		return false, errors.New("content descriptor is invalid")
	}
	stored := new(appliancev1.ApplianceContentReference)
	if err := proto.Unmarshal(encoded, stored); err != nil {
		return false, err
	}
	expected := proto.Clone(content).(*appliancev1.ApplianceContentReference)
	expected.Role = appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_UNSPECIFIED
	if !proto.Equal(stored, expected) {
		return false, errors.New("content descriptor conflicts with archive metadata")
	}
	return true, nil
}

func ValidateArchive(ctx context.Context, file io.ReadSeeker, content *appliancev1.ApplianceContentReference) error {
	if err := ValidateReference(content); err != nil {
		return err
	}
	reader := contextArchive{ReadSeeker: file, ctx: ctx}
	if _, err := reader.Seek(0, io.SeekStart); err != nil {
		return err
	}
	hash := sha256.New()
	size, err := io.Copy(hash, io.LimitReader(reader, int64(content.GetArchiveSize())+1))
	if err != nil {
		return err
	}
	if uint64(size) != content.GetArchiveSize() || subtle.ConstantTimeCompare(hash.Sum(nil), content.GetArchiveSha256()) != 1 {
		return errors.New("content archive sha256 or size does not match its reference")
	}
	_, err = ociarchive.Validate(reader, digest.Digest(content.GetImageManifestDigest()))
	return err
}

func Publish(root string, file *os.File, content *appliancev1.ApplianceContentReference) error {
	if err := file.Chmod(0444); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	if err := os.Rename(file.Name(), filepath.Join(root, ArchiveName(content))); err != nil {
		return err
	}
	descriptor := proto.Clone(content).(*appliancev1.ApplianceContentReference)
	descriptor.Role = appliancev1.ApplianceContentRole_APPLIANCE_CONTENT_ROLE_UNSPECIFIED
	encoded, err := proto.MarshalOptions{Deterministic: true}.Marshal(descriptor)
	if err != nil {
		return err
	}
	return atomicfile.ReplaceFile(filepath.Join(root, ArchiveName(content)+".pb"), encoded, 0444)
}

type contextArchive struct {
	io.ReadSeeker
	ctx context.Context
}

func (reader contextArchive) Read(bytes []byte) (int, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.ReadSeeker.Read(bytes)
}

func (reader contextArchive) Seek(offset int64, whence int) (int64, error) {
	if err := reader.ctx.Err(); err != nil {
		return 0, err
	}
	return reader.ReadSeeker.Seek(offset, whence)
}
