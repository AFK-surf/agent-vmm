package build

import (
	"archive/tar"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	securejoin "github.com/cyphar/filepath-securejoin"
)

const (
	MaxContextBytes = 256 << 20
	MaxFileBytes    = 64 << 20
	MaxEntries      = 10000
	MaxHardlinks    = 1024
)

type pendingHardlink struct {
	name   string
	target string
}

func ExtractContext(reader io.Reader, root string) error {
	archive := tar.NewReader(reader)
	var logicalBytes int64
	entries, hardlinks := 0, make([]pendingHardlink, 0)
	for {
		header, err := archive.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read context archive: %w", err)
		}
		entries++
		if entries > MaxEntries {
			return errors.New("context has too many entries")
		}
		name, err := archivePath(header.Name)
		if err != nil {
			return err
		}
		destination, err := securejoin.SecureJoin(root, name)
		if err != nil {
			return fmt.Errorf("resolve context path: %w", err)
		}
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(destination, os.FileMode(header.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if header.Size < 0 || header.Size > MaxFileBytes {
				return errors.New("context file exceeds size limit")
			}
			logicalBytes += header.Size
			if logicalBytes > MaxContextBytes {
				return errors.New("context exceeds size limit")
			}
			if err := writeFile(archive, destination, header.Size, os.FileMode(header.Mode)&0o777); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := validateLinkTarget(name, header.Linkname); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
				return err
			}
			if err := os.Symlink(header.Linkname, destination); err != nil {
				return err
			}
		case tar.TypeLink:
			if len(hardlinks) >= MaxHardlinks {
				return errors.New("context has too many hardlinks")
			}
			target, err := archivePath(header.Linkname)
			if err != nil {
				return errors.New("invalid hardlink target")
			}
			hardlinks = append(hardlinks, pendingHardlink{name: name, target: target})
		default:
			return errors.New("context contains unsupported entry type")
		}
	}
	for _, link := range hardlinks {
		target, err := securejoin.SecureJoin(root, link.target)
		if err != nil {
			return err
		}
		info, err := os.Stat(target)
		if err != nil || !info.Mode().IsRegular() {
			return errors.New("hardlink target is not a regular context file")
		}
		destination, err := securejoin.SecureJoin(root, link.name)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
			return err
		}
		if err := os.Link(target, destination); err != nil {
			return err
		}
	}
	return nil
}

func archivePath(name string) (string, error) {
	if name == "" || path.IsAbs(name) || strings.ContainsRune(name, '\x00') || strings.Contains(name, "\\") {
		return "", errors.New("context contains an invalid path")
	}
	clean := path.Clean(name)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("context path escapes its root")
	}
	return clean, nil
}

func validateLinkTarget(name, target string) error {
	if target == "" || path.IsAbs(target) || strings.ContainsRune(target, '\x00') || strings.Contains(target, "\\") {
		return errors.New("context contains an invalid symlink")
	}
	resolved := path.Clean(path.Join(path.Dir(name), target))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return errors.New("context symlink escapes its root")
	}
	return nil
}

func writeFile(reader io.Reader, destination string, size int64, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(destination), 0o700); err != nil {
		return err
	}
	file, err := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	_, copyErr := io.CopyN(file, reader, size)
	return errors.Join(copyErr, file.Close())
}
