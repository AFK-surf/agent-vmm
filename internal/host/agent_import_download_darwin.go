package host

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"

	vmmdv1 "github.com/AFK-surf/agent-vmm/internal/api/vmmd/v1"
)

type importCurlCommand func(context.Context, string, ...string) *exec.Cmd

func downloadImportArchivePlatform(ctx context.Context, raw string, size uint64, send func([]byte) error) error {
	return downloadImportArchiveWithCurlResolver(ctx, raw, size, send, net.DefaultResolver, exec.CommandContext)
}

func downloadImportArchiveWithCurlResolver(ctx context.Context, raw string, size uint64, send func([]byte) error, resolver importAddressResolver, command importCurlCommand) error {
	endpoint, err := resolveImportArchiveEndpoint(ctx, raw, resolver)
	if err != nil {
		return fmt.Errorf("resolve runtime OCI archive: %w", err)
	}
	return downloadImportArchiveWithCurl(ctx, raw, size, send, endpoint, command)
}

func downloadImportArchiveWithCurl(ctx context.Context, raw string, size uint64, send func([]byte) error, endpoint importArchiveEndpoint, command importCurlCommand) error {
	reader, writer, err := os.Pipe()
	if err != nil {
		return err
	}
	defer reader.Close()
	defer writer.Close()
	processContext, cancel := context.WithCancel(ctx)
	defer cancel()
	args := []string{"-q", "--globoff", "--silent", "--show-error", "--fail", "--proto", "=https", "--proto-redir", "=https", "--tlsv1.2", "--max-redirs", "0", "--proxy", "", "--noproxy", "*", "--ipv4", "--resolve", endpoint.hostname + ":" + endpoint.port + ":" + endpoint.address.String(), "--max-time", "600", "--output", "/dev/fd/3", "--write-out", "%{http_code}"}
	args = append(args, importArchiveCurlTrustArgs()...)
	args = append(args, "--url", raw)
	process := command(processContext, "/usr/bin/curl", args...)
	process.Env = importCurlEnvironment()
	process.ExtraFiles = []*os.File{writer}
	status, diagnostic := &boundedImportOutput{limit: 16}, &boundedImportOutput{limit: 4096}
	process.Stdout, process.Stderr = status, diagnostic
	if err := process.Start(); err != nil {
		return fmt.Errorf("start runtime OCI download: %w", err)
	}
	_ = writer.Close()
	buffer := make([]byte, vmmdv1.MaxImportArchiveChunkBytes)
	var received uint64
	var transferErr error
	for {
		count, readErr := io.ReadFull(reader, buffer)
		if count > 0 {
			if uint64(count) > size-received {
				transferErr = errors.New("runtime OCI archive exceeds committed size")
				break
			}
			received += uint64(count)
			if transferErr = send(append([]byte(nil), buffer[:count]...)); transferErr != nil {
				break
			}
		}
		if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
		if readErr != nil {
			transferErr = fmt.Errorf("read runtime OCI archive: %w", readErr)
			break
		}
	}
	if transferErr != nil {
		cancel()
		_ = reader.Close()
	}
	waitErr := process.Wait()
	if transferErr != nil {
		return transferErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if waitErr != nil {
		return fmt.Errorf("download runtime OCI archive: %w: %s", waitErr, diagnostic.data)
	}
	if status.overflow || string(status.data) != "200" {
		return fmt.Errorf("download runtime OCI archive: HTTP %s", status.data)
	}
	if received != size {
		return fmt.Errorf("runtime OCI archive size = %d, want %d", received, size)
	}
	return nil
}

type boundedImportOutput struct {
	data     []byte
	limit    int
	overflow bool
}

func (output *boundedImportOutput) Write(value []byte) (int, error) {
	remaining := output.limit - len(output.data)
	count := min(len(value), remaining)
	output.data = append(output.data, value[:count]...)
	output.overflow = output.overflow || count < len(value)
	return len(value), nil
}

func importCurlEnvironment() []string {
	result := make([]string, 0, len(os.Environ()))
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		key = strings.ToUpper(key)
		if strings.HasPrefix(key, "CURL_") || strings.HasPrefix(key, "SSL_CERT_") || key == "SSLKEYLOGFILE" {
			continue
		}
		result = append(result, value)
	}
	return result
}
