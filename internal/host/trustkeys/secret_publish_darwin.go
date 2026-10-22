package trustkeys

import "golang.org/x/sys/unix"

// PublishSecret moves a completed secret into place and never replaces an
// existing file.
func PublishSecret(source, destination string) error {
	return unix.RenameatxNp(unix.AT_FDCWD, source, unix.AT_FDCWD, destination, unix.RENAME_EXCL)
}
