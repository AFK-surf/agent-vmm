//go:build linux

package control

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func prepareWorkspaceWriteParent(namespaceID, volumeID, relative string) (*os.File, error) {
	clean, err := cleanWorkspaceRelative(relative)
	if err != nil {
		return nil, err
	}
	current, err := openWorkspaceRoot(namespaceID, volumeID, ".", unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return nil, err
	}
	var owner unix.Stat_t
	if err := unix.Fstat(int(current.Fd()), &owner); err != nil {
		current.Close()
		return nil, err
	}
	if clean == "." {
		return current, nil
	}
	for _, component := range strings.Split(clean, "/") {
		created := false
		next, err := openBeneath(current, component, unix.O_RDONLY|unix.O_DIRECTORY)
		if errors.Is(err, unix.ENOENT) {
			if err = unix.Mkdirat(int(current.Fd()), component, 0700); err == nil {
				created = true
			} else if !errors.Is(err, unix.EEXIST) {
				current.Close()
				return nil, err
			}
			next, err = openBeneath(current, component, unix.O_RDONLY|unix.O_DIRECTORY)
		}
		if err != nil {
			current.Close()
			return nil, err
		}
		if created {
			if err = unix.Fchown(int(next.Fd()), int(owner.Uid), int(owner.Gid)); err == nil {
				err = next.Sync()
			}
			if err == nil {
				err = current.Sync()
			}
			if err != nil {
				next.Close()
				current.Close()
				return nil, err
			}
		}
		current.Close()
		current = next
	}
	return current, nil
}

func (s *Server) WriteWorkspace(stream vmmdv1.VMMDService_WriteWorkspaceServer) (resultErr error) {
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return rpcError(errors.New("workspace write header must be first"))
	}
	header := first.GetHeader()
	phase := "admission"
	var rawErr error
	fail := func(err error) error { rawErr = err; return rpcError(err) }
	defer func() {
		if resultErr != nil {
			if rawErr == nil {
				rawErr = resultErr
			}
			s.runtime.logger.Warn("workspace write failed", "namespace_id", header.GetActorNamespaceId(), "volume_id", header.GetVolumeId(), "path", header.GetPath(), "request_id", header.GetRequestId(), "stage", phase, "error", rawErr)
		}
	}()
	if err := validateRequestID(header.GetRequestId()); err != nil {
		return fail(err)
	}
	release, err := s.runtime.acquireWorkspace(stream.Context())
	if err != nil {
		return fail(err)
	}
	defer release()
	clean, err := cleanWorkspaceRelative(header.GetPath())
	if err != nil || clean == "." {
		return fail(errors.New("workspace write requires a file path"))
	}
	key := "workspace-upload\x00" + header.GetActorNamespaceId() + "\x00" + header.GetVolumeId() + "\x00" + clean + "\x00" + header.GetRequestId()
	if _, active := s.runtime.resourceLocks.LoadOrStore(key, struct{}{}); active {
		return fail(status.Error(codes.Aborted, "workspace upload is already in progress"))
	}
	defer s.runtime.resourceLocks.Delete(key)
	parentPath, name := path.Dir(clean), path.Base(clean)
	phase = "parent"
	parent, err := prepareWorkspaceWriteParent(header.GetActorNamespaceId(), header.GetVolumeId(), parentPath)
	if err != nil {
		return fail(err)
	}
	defer parent.Close()
	phase = "staging"
	stagingName := workspaceUploadStagingName(name, header.GetRequestId())
	flags := unix.O_RDWR | unix.O_CREAT | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if header.GetOffset() == 0 {
		flags |= unix.O_TRUNC
	}
	fd, err := unix.Openat(int(parent.Fd()), stagingName, flags, 0o600)
	if err != nil {
		return fail(err)
	}
	staging := os.NewFile(uintptr(fd), stagingName)
	defer staging.Close()
	info, err := staging.Stat()
	if err != nil || uint64(info.Size()) != header.GetOffset() {
		return fail(errors.New("workspace resume offset does not match staging file"))
	}
	digest := sha256.New()
	if header.GetOffset() != 0 {
		if _, err := io.Copy(digest, staging); err != nil || !equalDigest(digest.Sum(nil), header.GetExpectedPrefixSha256()) {
			return fail(errors.New("workspace resume prefix digest mismatch"))
		}
	}
	if _, err := staging.Seek(int64(header.GetOffset()), io.SeekStart); err != nil {
		return fail(err)
	}
	phase = "transfer"
	finished := false
	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return recvErr
		}
		if data := message.GetData(); data != nil {
			if len(data) > maxWorkspaceChunk {
				return fail(errors.New("workspace write exceeds chunk limit"))
			}
			if _, err := staging.Write(data); err != nil {
				return fail(err)
			}
			_, _ = digest.Write(data)
		} else if message.GetFinish() {
			finished = true
			break
		} else {
			return fail(errors.New("workspace write stream is invalid"))
		}
	}
	if !finished {
		return fail(errors.New("workspace write did not finish"))
	}
	phase = "commit"
	if err := staging.Sync(); err != nil {
		return fail(err)
	}
	mode := os.FileMode(header.GetMode())
	if mode == 0 {
		mode = 0o600
	}
	if mode&^0o777 != 0 {
		return fail(errors.New("workspace mode is invalid"))
	}
	if err := staging.Chmod(mode); err != nil {
		return fail(err)
	}
	if header.GetVolumeId() != "" {
		var owner unix.Stat_t
		if err := unix.Fstat(int(parent.Fd()), &owner); err != nil {
			return fail(err)
		}
		if err := unix.Fchown(int(staging.Fd()), int(owner.Uid), int(owner.Gid)); err != nil {
			return fail(err)
		}
	}
	if err := unix.Renameat(int(parent.Fd()), stagingName, int(parent.Fd()), name); err != nil {
		return fail(err)
	}
	if err := parent.Sync(); err != nil {
		return fail(err)
	}
	phase = "result"
	result, err := openWorkspaceRoot(header.GetActorNamespaceId(), header.GetVolumeId(), clean, unix.O_RDONLY)
	if err != nil {
		return fail(err)
	}
	defer result.Close()
	entry, err := workspaceEntry(clean, result, false)
	if err != nil {
		return fail(err)
	}
	entry.Sha256 = digest.Sum(nil)
	return stream.SendAndClose(&vmmdv1.WriteWorkspaceResponse{Entry: entry, Sha256: entry.GetSha256()})
}

func workspaceUploadStagingName(name, requestID string) string {
	return fmt.Sprintf(".%s.upload-%x", name, sha256.Sum256([]byte(requestID)))
}
