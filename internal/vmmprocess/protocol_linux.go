//go:build linux

package vmmprocess

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"path/filepath"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

type Config struct {
	KernelPath, InitramfsPath, RootFSPath, StatePath, LegacyStatePath string
	InstanceDirectory, SerialLog, CommandLine, GuestManifestDigest    string
	CPUCount                                                          uint
	MemoryBytes                                                       uint64
}

type request struct {
	Operation string
	Session   string
	Port      uint32
	Config    *Config
}
type launchSpec struct {
	Config Config
	Launch string
}

type reply struct {
	Launch string
	Error  string
	State  hostv1.VMState
	PID    int
}

func socketPath(config Config) string { return filepath.Join(config.InstanceDirectory, "vmm.sock") }

func readMessage(reader *bufio.Reader, value any) error {
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return err
	}
	return json.Unmarshal(line, value)
}
func writeMessage(writer io.Writer, value any) error { return json.NewEncoder(writer).Encode(value) }

type stream struct {
	*net.UnixConn
	reader *bufio.Reader
}

func (s *stream) Read(p []byte) (int, error) { return s.reader.Read(p) }

func exchange(ctx context.Context, path string, req request) (*stream, reply, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
	if err != nil {
		return nil, reply{}, err
	}
	raw := conn.(*net.UnixConn)
	stop := context.AfterFunc(ctx, func() { raw.Close() })
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}
	s := &stream{raw, bufio.NewReaderSize(raw, 16<<10)}
	err = writeMessage(raw, req)
	var result reply
	if err == nil {
		err = readMessage(s.reader, &result)
	}
	if !stop() {
		err = ctx.Err()
		if err == nil {
			err = context.Canceled
		}
	}
	if err == nil && result.Error != "" {
		err = errors.New(result.Error)
	}
	if err != nil {
		raw.Close()
		return nil, result, err
	}
	_ = raw.SetDeadline(time.Time{})
	return s, result, nil
}

func relay(left, right net.Conn) {
	defer left.Close()
	defer right.Close()
	done := make(chan struct{}, 2)
	copyTo := func(dst, src net.Conn) {
		_, err := io.Copy(dst, src)
		if err != nil {
			for _, conn := range []net.Conn{left, right} {
				if abort, ok := conn.(interface{ Abort() error }); ok {
					_ = abort.Abort()
				} else {
					_ = conn.Close()
				}
			}
		} else if c, ok := dst.(interface{ CloseWrite() error }); ok {
			_ = c.CloseWrite()
		} else {
			dst.Close()
		}
		done <- struct{}{}
	}
	go copyTo(left, right)
	go copyTo(right, left)
	<-done
	<-done
}

func requestError(format string, args ...any) reply {
	return reply{Error: fmt.Sprintf(format, args...)}
}
