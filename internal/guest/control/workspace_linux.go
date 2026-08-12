//go:build linux

package control

import (
	"archive/tar"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	"golang.org/x/sys/unix"
)

const (
	maxWorkspaceBytes   = 256 << 20
	maxWorkspaceChunk   = 64 << 10
	maxWorkspaceEntries = 1000
)

func cleanWorkspaceRelative(value string) (string, error) {
	if len(value) > 1024 || strings.IndexByte(value, 0) >= 0 || strings.HasPrefix(value, "/") {
		return "", errors.New("workspace path must be a bounded relative path")
	}
	clean := path.Clean(value)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("workspace path escapes its namespace")
	}
	if clean == "." {
		return ".", nil
	}
	return clean, nil
}

func (r *Runtime) acquireWorkspace(ctx context.Context) (func(), error) {
	select {
	case r.workspaceSlots <- struct{}{}:
		return func() { <-r.workspaceSlots }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func openWorkspace(namespaceID, relative string, flags int) (*os.File, error) {
	if err := validateID("actor_namespace_id", namespaceID); err != nil {
		return nil, err
	}
	clean, err := cleanWorkspaceRelative(relative)
	if err != nil {
		return nil, err
	}
	root, err := os.Open(workspacePath(namespaceID))
	if err != nil {
		return nil, err
	}
	defer root.Close()
	fd, err := unix.Openat2(int(root.Fd()), clean, &unix.OpenHow{Flags: uint64(flags | unix.O_CLOEXEC),
		Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_MAGICLINKS | unix.RESOLVE_NO_SYMLINKS})
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(fd), clean), nil
}

func workspaceEntry(relative string, file *os.File, includeDigest bool) (*vmmdv1.WorkspaceEntry, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	kind := vmmdv1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_UNSPECIFIED
	if info.Mode().IsRegular() {
		kind = vmmdv1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_FILE
	} else if info.IsDir() {
		kind = vmmdv1.WorkspaceEntryKind_WORKSPACE_ENTRY_KIND_DIRECTORY
	} else {
		return nil, errors.New("workspace entry type is not supported")
	}
	entry := &vmmdv1.WorkspaceEntry{Path: relative, Kind: kind, Size: uint64(info.Size()), Mode: uint32(info.Mode().Perm()),
		ModifiedUnixNanos: info.ModTime().UnixNano()}
	if includeDigest && info.Mode().IsRegular() {
		if info.Size() > maxWorkspaceBytes {
			return nil, errors.New("workspace file exceeds digest limit")
		}
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return nil, err
		}
		digest := sha256.New()
		if _, err := io.Copy(digest, io.LimitReader(file, maxWorkspaceBytes+1)); err != nil {
			return nil, err
		}
		entry.Sha256 = digest.Sum(nil)
	}
	return entry, nil
}

func (s *Server) StatWorkspace(ctx context.Context, request *vmmdv1.WorkspacePathRequest) (*vmmdv1.WorkspaceEntry, error) {
	release, err := s.runtime.acquireWorkspace(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	defer release()
	file, err := openWorkspace(request.GetActorNamespaceId(), request.GetPath(), unix.O_RDONLY)
	if err != nil {
		return nil, rpcError(err)
	}
	defer file.Close()
	entry, err := workspaceEntry(path.Clean(request.GetPath()), file, true)
	if err != nil {
		return nil, rpcError(err)
	}
	return entry, nil
}

func (s *Server) ListWorkspace(ctx context.Context, request *vmmdv1.ListWorkspaceRequest) (*vmmdv1.ListWorkspaceResponse, error) {
	limit := request.GetLimit()
	if limit == 0 || limit > maxWorkspaceEntries {
		limit = maxWorkspaceEntries
	}
	release, err := s.runtime.acquireWorkspace(ctx)
	if err != nil {
		return nil, rpcError(err)
	}
	defer release()
	directory, err := openWorkspace(request.GetActorNamespaceId(), request.GetPath(), unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return nil, rpcError(err)
	}
	defer directory.Close()
	names, err := directory.Readdirnames(int(limit) + 1)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, rpcError(err)
	}
	if len(names) > int(limit) {
		return nil, rpcError(errors.New("workspace directory exceeds requested bounded window"))
	}
	sort.Strings(names)
	response := &vmmdv1.ListWorkspaceResponse{}
	base := path.Clean(request.GetPath())
	for _, name := range names {
		relative := path.Join(base, name)
		file, err := openWorkspace(request.GetActorNamespaceId(), relative, unix.O_RDONLY)
		if err != nil {
			return nil, rpcError(err)
		}
		entry, entryErr := workspaceEntry(relative, file, false)
		file.Close()
		if entryErr != nil {
			return nil, rpcError(entryErr)
		}
		response.Entries = append(response.Entries, entry)
	}
	return response, nil
}

func (s *Server) ReadWorkspace(request *vmmdv1.ReadWorkspaceRequest, stream vmmdv1.VMMDService_ReadWorkspaceServer) error {
	release, err := s.runtime.acquireWorkspace(stream.Context())
	if err != nil {
		return rpcError(err)
	}
	defer release()
	file, err := openWorkspace(request.GetActorNamespaceId(), request.GetPath(), unix.O_RDONLY)
	if err != nil {
		return rpcError(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		return rpcError(errors.New("workspace read requires a regular file"))
	}
	maximum := request.GetMaxBytes()
	if maximum == 0 {
		maximum = maxWorkspaceBytes
	}
	if maximum > maxWorkspaceBytes || request.GetOffset() > uint64(info.Size()) {
		return rpcError(errors.New("workspace read range exceeds limits"))
	}
	if _, err := file.Seek(int64(request.GetOffset()), io.SeekStart); err != nil {
		return rpcError(err)
	}
	digest := sha256.New()
	buffer := make([]byte, maxWorkspaceChunk)
	offset, remaining := request.GetOffset(), maximum
	for remaining != 0 {
		count, readErr := file.Read(buffer[:min(uint64(len(buffer)), remaining)])
		if count != 0 {
			chunk := append([]byte(nil), buffer[:count]...)
			_, _ = digest.Write(chunk)
			if err := stream.Send(&vmmdv1.ReadWorkspaceResponse{Data: chunk, Offset: offset}); err != nil {
				return err
			}
			offset += uint64(count)
			remaining -= uint64(count)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return rpcError(readErr)
		}
	}
	return stream.Send(&vmmdv1.ReadWorkspaceResponse{Offset: offset, Sha256: digest.Sum(nil), Final: true})
}

func (s *Server) WriteWorkspace(stream vmmdv1.VMMDService_WriteWorkspaceServer) error {
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return rpcError(errors.New("workspace write header must be first"))
	}
	header := first.GetHeader()
	if err := validateRequestID(header.GetRequestId()); err != nil {
		return rpcError(err)
	}
	release, err := s.runtime.acquireWorkspace(stream.Context())
	if err != nil {
		return rpcError(err)
	}
	defer release()
	clean, err := cleanWorkspaceRelative(header.GetPath())
	if err != nil || clean == "." {
		return rpcError(errors.New("workspace write requires a file path"))
	}
	parentPath, name := path.Dir(clean), path.Base(clean)
	parent, err := openWorkspace(header.GetActorNamespaceId(), parentPath, unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return rpcError(err)
	}
	defer parent.Close()
	stagingName := "." + name + ".upload-" + header.GetRequestId()
	flags := unix.O_RDWR | unix.O_CREAT | unix.O_CLOEXEC | unix.O_NOFOLLOW
	if header.GetOffset() == 0 {
		flags |= unix.O_TRUNC
	}
	fd, err := unix.Openat(int(parent.Fd()), stagingName, flags, 0o600)
	if err != nil {
		return rpcError(err)
	}
	staging := os.NewFile(uintptr(fd), stagingName)
	defer staging.Close()
	info, err := staging.Stat()
	if err != nil || uint64(info.Size()) != header.GetOffset() {
		return rpcError(errors.New("workspace resume offset does not match staging file"))
	}
	if header.GetOffset() != 0 {
		digest := sha256.New()
		if _, err := io.Copy(digest, staging); err != nil || !equalDigest(digest.Sum(nil), header.GetExpectedPrefixSha256()) {
			return rpcError(errors.New("workspace resume prefix digest mismatch"))
		}
	}
	if _, err := staging.Seek(int64(header.GetOffset()), io.SeekStart); err != nil {
		return rpcError(err)
	}
	total := header.GetOffset()
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
			if len(data) > maxWorkspaceChunk || total+uint64(len(data)) > maxWorkspaceBytes {
				return rpcError(errors.New("workspace write exceeds chunk or file limit"))
			}
			if _, err := staging.Write(data); err != nil {
				return rpcError(err)
			}
			total += uint64(len(data))
		} else if message.GetFinish() {
			finished = true
			break
		} else {
			return rpcError(errors.New("workspace write stream is invalid"))
		}
	}
	if !finished {
		return rpcError(errors.New("workspace write did not finish"))
	}
	if err := staging.Sync(); err != nil {
		return rpcError(err)
	}
	mode := os.FileMode(header.GetMode())
	if mode == 0 {
		mode = 0o600
	}
	if mode&^0o777 != 0 {
		return rpcError(errors.New("workspace mode is invalid"))
	}
	if err := staging.Chmod(mode); err != nil {
		return rpcError(err)
	}
	if err := unix.Renameat(int(parent.Fd()), stagingName, int(parent.Fd()), name); err != nil {
		return rpcError(err)
	}
	result, err := openWorkspace(header.GetActorNamespaceId(), clean, unix.O_RDONLY)
	if err != nil {
		return rpcError(err)
	}
	defer result.Close()
	entry, err := workspaceEntry(clean, result, true)
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(&vmmdv1.WriteWorkspaceResponse{Entry: entry, Sha256: entry.GetSha256()})
}

func equalDigest(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func (s *Server) ArchiveWorkspace(request *vmmdv1.ArchiveWorkspaceRequest, stream vmmdv1.VMMDService_ArchiveWorkspaceServer) error {
	// Archive streaming is intentionally implemented without a host mount. The
	// walker rejects every symlink and the namespace root is opened beneath /state.
	release, err := s.runtime.acquireWorkspace(stream.Context())
	if err != nil {
		return rpcError(err)
	}
	defer release()
	clean, err := cleanWorkspaceRelative(request.GetPath())
	if err != nil {
		return rpcError(err)
	}
	reader, writer := io.Pipe()
	go func() {
		archive := tar.NewWriter(writer)
		err := walkWorkspaceArchive(request.GetActorNamespaceId(), clean, archive)
		if closeErr := archive.Close(); err == nil {
			err = closeErr
		}
		_ = writer.CloseWithError(err)
	}()
	digest := sha256.New()
	buffer := make([]byte, maxWorkspaceChunk)
	var total uint64
	for {
		count, readErr := reader.Read(buffer)
		if count != 0 {
			total += uint64(count)
			if total > maxWorkspaceBytes {
				reader.CloseWithError(errors.New("workspace archive exceeds limit"))
				return rpcError(errors.New("workspace archive exceeds limit"))
			}
			chunk := append([]byte(nil), buffer[:count]...)
			_, _ = digest.Write(chunk)
			if err := stream.Send(&vmmdv1.ArchiveWorkspaceResponse{Data: chunk}); err != nil {
				return err
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return rpcError(readErr)
		}
	}
	return stream.Send(&vmmdv1.ArchiveWorkspaceResponse{Sha256: digest.Sum(nil), Final: true})
}

func walkWorkspaceArchive(namespaceID, relative string, archive *tar.Writer) error {
	root := workspacePath(namespaceID)
	start := filepath.Join(root, filepath.FromSlash(relative))
	entries := 0
	return filepath.WalkDir(start, func(current string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		entries++
		if entries > maxWorkspaceEntries {
			return errors.New("workspace archive exceeds entry limit")
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return errors.New("workspace archive cannot contain symlinks")
		}
		itemRelative, err := filepath.Rel(root, current)
		if err != nil {
			return err
		}
		itemRelative = filepath.ToSlash(itemRelative)
		file, err := openWorkspace(namespaceID, itemRelative, unix.O_RDONLY)
		if err != nil {
			return err
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return err
		}
		if !info.IsDir() && !info.Mode().IsRegular() {
			return errors.New("workspace archive entry type is unsupported")
		}
		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = itemRelative
		if info.IsDir() && !strings.HasSuffix(header.Name, "/") {
			header.Name += "/"
		}
		if err := archive.WriteHeader(header); err != nil {
			return err
		}
		if info.Mode().IsRegular() {
			if info.Size() > maxWorkspaceBytes {
				return errors.New("workspace archive file exceeds limit")
			}
			if _, err := io.CopyN(archive, file, info.Size()); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Server) ExtractWorkspace(stream vmmdv1.VMMDService_ExtractWorkspaceServer) error {
	first, err := stream.Recv()
	if err != nil || first.GetHeader() == nil {
		return rpcError(errors.New("workspace extract header must be first"))
	}
	header := first.GetHeader()
	if err := validateRequestID(header.GetRequestId()); err != nil {
		return rpcError(err)
	}
	clean, err := cleanWorkspaceRelative(header.GetPath())
	if err != nil || clean == "." {
		return rpcError(errors.New("workspace extract requires a new directory path"))
	}
	release, err := s.runtime.acquireWorkspace(stream.Context())
	if err != nil {
		return rpcError(err)
	}
	defer release()
	parentRelative, targetName := path.Dir(clean), path.Base(clean)
	parent, err := openWorkspace(header.GetActorNamespaceId(), parentRelative, unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return rpcError(err)
	}
	defer parent.Close()
	stagingName := "." + targetName + ".extract-" + header.GetRequestId()
	parentAbsolute := filepath.Join(workspacePath(header.GetActorNamespaceId()), filepath.FromSlash(parentRelative))
	stagingAbsolute := filepath.Join(parentAbsolute, stagingName)
	if err := os.RemoveAll(stagingAbsolute); err != nil {
		return rpcError(err)
	}
	if err := unix.Mkdirat(int(parent.Fd()), stagingName, 0o700); err != nil {
		return rpcError(err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(stagingAbsolute)
		}
	}()
	archiveFile, err := os.CreateTemp(stagingAbsolute, ".archive-*")
	if err != nil {
		return rpcError(err)
	}
	archivePath := archiveFile.Name()
	var total uint64
	finished := false
	for {
		message, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			archiveFile.Close()
			return recvErr
		}
		if data := message.GetData(); data != nil {
			if len(data) > maxWorkspaceChunk || total+uint64(len(data)) > maxWorkspaceBytes {
				archiveFile.Close()
				return rpcError(errors.New("workspace extract exceeds chunk or archive limit"))
			}
			if _, err := archiveFile.Write(data); err != nil {
				archiveFile.Close()
				return rpcError(err)
			}
			total += uint64(len(data))
		} else if message.GetFinish() {
			finished = true
			break
		} else {
			archiveFile.Close()
			return rpcError(errors.New("workspace extract stream is invalid"))
		}
	}
	if !finished {
		archiveFile.Close()
		return rpcError(errors.New("workspace extract did not finish"))
	}
	if _, err := archiveFile.Seek(0, io.SeekStart); err != nil {
		archiveFile.Close()
		return rpcError(err)
	}
	reader := tar.NewReader(archiveFile)
	entries := 0
	for {
		item, readErr := reader.Next()
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			archiveFile.Close()
			return rpcError(readErr)
		}
		entries++
		itemName, err := cleanWorkspaceRelative(item.Name)
		if err != nil || itemName == "." || entries > maxWorkspaceEntries {
			archiveFile.Close()
			return rpcError(errors.New("workspace archive entry is invalid"))
		}
		destination := filepath.Join(stagingAbsolute, filepath.FromSlash(itemName))
		if !strings.HasPrefix(destination, stagingAbsolute+string(os.PathSeparator)) {
			archiveFile.Close()
			return rpcError(errors.New("workspace archive entry escapes staging root"))
		}
		switch item.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destination, os.FileMode(item.Mode)&0o777); err != nil {
				archiveFile.Close()
				return rpcError(err)
			}
		case tar.TypeReg, tar.TypeRegA:
			if item.Size < 0 || item.Size > maxWorkspaceBytes {
				archiveFile.Close()
				return rpcError(errors.New("workspace archive file exceeds limit"))
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
				archiveFile.Close()
				return rpcError(err)
			}
			file, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, os.FileMode(item.Mode)&0o777)
			if err != nil {
				archiveFile.Close()
				return rpcError(err)
			}
			_, copyErr := io.CopyN(file, reader, item.Size)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				archiveFile.Close()
				return rpcError(errors.Join(copyErr, closeErr))
			}
		default:
			archiveFile.Close()
			return rpcError(errors.New("workspace archive links and special files are forbidden"))
		}
	}
	if err := archiveFile.Close(); err != nil {
		return rpcError(err)
	}
	if err := os.Remove(archivePath); err != nil {
		return rpcError(err)
	}
	if err := unix.Renameat(int(parent.Fd()), stagingName, int(parent.Fd()), targetName); err != nil {
		return rpcError(err)
	}
	cleanup = false
	result, err := openWorkspace(header.GetActorNamespaceId(), clean, unix.O_RDONLY|unix.O_DIRECTORY)
	if err != nil {
		return rpcError(err)
	}
	defer result.Close()
	entry, err := workspaceEntry(clean, result, false)
	if err != nil {
		return rpcError(err)
	}
	return stream.SendAndClose(&vmmdv1.WriteWorkspaceResponse{Entry: entry})
}
