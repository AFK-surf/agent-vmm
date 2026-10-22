//go:build darwin || linux

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sys/unix"
)

func readManagedLocalLogs(status map[string]any, opts managedOptions, _ time.Time) ([]managedLogRecord, string, bool, error) {
	account, err := user.Lookup(stringFact(status, "serviceUser"))
	if err != nil {
		return nil, "", false, errors.New("service user log directory is unavailable")
	}
	base := serviceLogDirectory(account.HomeDir)
	paths := []string{filepath.Join(base, "host.stderr.log")}
	if opts.component == "guest" {
		paths = []string{filepath.Join(base, "serial.log.previous"), filepath.Join(base, "serial.log")}
	}
	return readManagedLocalLogFiles(paths, opts)
}

func readManagedLocalLogFiles(paths []string, opts managedOptions) ([]managedLogRecord, string, bool, error) {
	type segment struct {
		file     *os.File
		info     os.FileInfo
		identity string
		offset   int64
	}
	segments := make([]segment, 0, len(paths))
	defer func() {
		for _, segment := range segments {
			segment.file.Close()
		}
	}()
	for _, path := range paths {
		fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, "", false, err
		}
		file := os.NewFile(uintptr(fd), path)
		info, err := file.Stat()
		var stat unix.Stat_t
		if err == nil {
			err = unix.Fstat(fd, &stat)
		}
		if err != nil || !info.Mode().IsRegular() {
			file.Close()
			return nil, "", false, errors.New("log is not a regular file")
		}
		identity := fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
		if len(segments) > 0 && segments[len(segments)-1].identity == identity {
			segments[len(segments)-1].file.Close()
			segments = segments[:len(segments)-1]
		}
		segments = append(segments, segment{file: file, info: info, identity: identity})
	}
	rotation := "none"
	if len(segments) > 1 {
		previous := segments[0]
		rotation = fmt.Sprintf("%s:%d", previous.identity, previous.info.ModTime().UnixNano())
	}
	cursorOffset, cursorRotation, hasRotation := strings.Cut(opts.afterCursor, "|")
	first, gap := 0, false
	if opts.afterCursor != "" {
		first = -1
		for index := range segments {
			prefix := segments[index].identity + ":"
			if hasRotation && cursorRotation == rotation && strings.HasPrefix(cursorOffset, prefix) {
				offset, err := strconv.ParseInt(strings.TrimPrefix(cursorOffset, prefix), 10, 64)
				if err == nil && offset >= 0 && offset <= segments[index].info.Size() {
					first = index
					segments[index].offset = offset
				}
				break
			}
		}
		if first < 0 {
			first = 0
			gap = true
		}
	}
	remaining := int64(managedLogBytes)
	records := make([]managedLogRecord, 0, len(segments))
	latest := opts.afterCursor
	if len(segments) > 0 {
		last := segments[len(segments)-1]
		latest = fmt.Sprintf("%s:%d|%s", last.identity, last.info.Size(), rotation)
	}
	if opts.limit > 0 && len(segments)-first > opts.limit {
		first = len(segments) - opts.limit
		gap = true
	}
	// Allocate the bounded tail to the newest segment first.
	for index := len(segments) - 1; index >= first; index-- {
		segment := segments[index]
		start := segment.offset
		truncated := false
		if segment.info.Size()-start > remaining {
			start = segment.info.Size() - remaining
			truncated = true
			gap = true
		}
		if start == segment.info.Size() {
			continue
		}
		if _, err := segment.file.Seek(start, io.SeekStart); err != nil {
			return nil, "", false, err
		}
		data, err := io.ReadAll(io.LimitReader(segment.file, segment.info.Size()-start))
		if err != nil {
			return nil, "", false, err
		}
		remaining -= int64(len(data))
		data = []byte(redactManagedText(string(data)))
		if opts.operationID != "" {
			data = filterManagedLogOperation(data, opts.operationID)
		}
		records = append(records, managedLogRecord{Cursor: fmt.Sprintf("%s:%d|%s", segment.identity, segment.info.Size(), rotation), Timestamp: segment.info.ModTime().UTC().Format(time.RFC3339Nano), Stream: "stderr", Data: string(data), Truncated: truncated})
	}
	for left, right := 0, len(records)-1; left < right; left, right = left+1, right-1 {
		records[left], records[right] = records[right], records[left]
	}
	return records, latest, gap, nil
}

func filterManagedLogOperation(data []byte, operationID string) []byte {
	lines := strings.Split(string(data), "\n")
	filtered := lines[:0]
	quoted := `"operation_id":"` + operationID + `"`
	for _, line := range lines {
		if strings.Contains(line, quoted) {
			filtered = append(filtered, line)
		}
	}
	return []byte(strings.Join(filtered, "\n"))
}
