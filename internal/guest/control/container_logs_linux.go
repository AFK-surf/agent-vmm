//go:build linux

package control

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	guestbuild "github.com/AFK-surf/agent-vmm/internal/guest/build"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/errdefs"
	"google.golang.org/grpc/codes"
)

const logHeaderBytes = 21

func (r *Runtime) cleanupTasks(ctx context.Context) error {
	names, err := r.client.NamespaceService().List(ctx)
	if err != nil {
		return err
	}
	for _, namespaceID := range names {
		nsctx := namespaceContext(ctx, namespaceID)
		containers, err := r.client.Containers(nsctx)
		if err != nil {
			return err
		}
		for _, container := range containers {
			labels, err := container.Labels(nsctx)
			if err != nil {
				return err
			}
			task, err := container.Task(nsctx, nil)
			if labels[guestbuild.InternalContainerLabel] == "true" {
				if err == nil {
					_, _ = task.Delete(nsctx, containerd.WithProcessKill)
				} else if !errdefs.IsNotFound(err) {
					return err
				}
				if err := container.Delete(nsctx, containerd.WithSnapshotCleanup); err != nil && !errdefs.IsNotFound(err) {
					return err
				}
				continue
			}
			if errdefs.IsNotFound(err) {
				if labels[containerStateLabel] == "running" {
					_, _ = container.SetLabels(nsctx, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(containerd.UnknownExitStatus, 10)})
				}
				continue
			}
			if err != nil {
				return err
			}
			status, _ := task.Delete(nsctx, containerd.WithProcessKill)
			code := uint32(containerd.UnknownExitStatus)
			if status != nil && status.Error() == nil {
				code = status.ExitCode()
			}
			_, _ = container.SetLabels(nsctx, map[string]string{containerStateLabel: "stopped", containerExitLabel: strconv.FormatUint(uint64(code), 10)})
		}
	}
	monitorsDone := make(chan struct{})
	go func() {
		r.tasks.Wait()
		close(monitorsDone)
	}()
	select {
	case <-monitorsDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := os.RemoveAll("/run/containerd/fifo"); err != nil {
		return err
	}
	return os.MkdirAll("/run/containerd/fifo", 0o700)
}

type recordLog struct {
	mu       sync.Mutex
	file     *os.File
	metaPath string
	limit    uint64
	written  uint64
	sequence uint64
	dropped  uint64
	writers  int
}

type recordWriter struct {
	log    *recordLog
	stream vmmdv1.LogStream
	once   sync.Once
}

func newRecordLog(path, metaPath string, limit uint64) (io.WriteCloser, io.WriteCloser, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0o600)
	if err != nil {
		return nil, nil, err
	}
	info, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	sequence, dropped, err := readLogMeta(metaPath)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	log := &recordLog{file: file, metaPath: metaPath, limit: limit, written: uint64(info.Size()), sequence: sequence, dropped: dropped, writers: 2}
	return &recordWriter{log: log, stream: vmmdv1.LogStream_LOG_STREAM_STDOUT}, &recordWriter{log: log, stream: vmmdv1.LogStream_LOG_STREAM_STDERR}, nil
}

func (writer *recordWriter) Write(data []byte) (int, error) {
	original := len(data)
	writer.log.mu.Lock()
	defer writer.log.mu.Unlock()
	writer.log.sequence++
	recordBytes := uint64(logHeaderBytes + len(data))
	if writer.log.written+recordBytes > writer.log.limit {
		writer.log.dropped++
		available := writer.log.limit - writer.log.written
		if available <= logHeaderBytes {
			return original, writer.log.writeMeta()
		}
		data = data[:available-logHeaderBytes]
		recordBytes = uint64(logHeaderBytes + len(data))
	}
	header := make([]byte, logHeaderBytes)
	binary.BigEndian.PutUint64(header[0:8], writer.log.sequence)
	binary.BigEndian.PutUint64(header[8:16], uint64(time.Now().UnixNano()))
	header[16] = byte(writer.stream)
	binary.BigEndian.PutUint32(header[17:21], uint32(len(data)))
	if _, err := writer.log.file.Write(append(header, data...)); err != nil {
		return 0, err
	}
	writer.log.written += recordBytes
	return original, writer.log.writeMeta()
}

func (writer *recordWriter) Close() error {
	var err error
	writer.once.Do(func() {
		writer.log.mu.Lock()
		defer writer.log.mu.Unlock()
		writer.log.writers--
		if writer.log.writers == 0 {
			err = errors.Join(writer.log.writeMeta(), writer.log.file.Close())
		}
	})
	return err
}

func (log *recordLog) writeMeta() error {
	value := fmt.Sprintf("%d %d\n", log.sequence, log.dropped)
	return os.WriteFile(log.metaPath, []byte(value), 0o600)
}

func readLogMeta(path string) (uint64, uint64, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return 0, 0, errors.New("invalid structured log metadata")
	}
	sequence, err := strconv.ParseUint(fields[0], 10, 64)
	if err != nil {
		return 0, 0, err
	}
	dropped, err := strconv.ParseUint(fields[1], 10, 64)
	return sequence, dropped, err
}

func (r *Runtime) StreamLogs(ctx context.Context, request *vmmdv1.LogsRequest, send func(*vmmdv1.LogsResponse) error) error {
	container, err := r.InspectContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
	if err != nil {
		return err
	}
	generationID := container.GetGenerationId()
	path := logPath(request.GetActorNamespaceId(), request.GetContainerId(), generationID)
	position := int64(0)
	var after uint64
	if request.GetAfterCursor() != "" {
		parsed, err := parseLogCursor(request.GetAfterCursor(), request.GetContainerId(), generationID)
		if err != nil {
			return err
		}
		after = parsed
	}
	for {
		records, next, err := readLogRecords(path, position)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		position = next
		filtered := records[:0]
		for _, record := range records {
			if record.sequence > after && record.timestamp >= request.GetSinceUnixNano() {
				filtered = append(filtered, record)
			}
		}
		if request.GetTailRecords() != 0 && uint64(len(filtered)) > request.GetTailRecords() && after == 0 && position == next {
			filtered = filtered[len(filtered)-int(request.GetTailRecords()):]
		}
		_, dropped, metaErr := readLogMeta(logMetaPath(request.GetActorNamespaceId(), request.GetContainerId(), generationID))
		if metaErr != nil {
			return metaErr
		}
		for _, record := range filtered {
			if err := send(&vmmdv1.LogsResponse{Cursor: request.GetContainerId() + ":" + generationID + ":" + strconv.FormatUint(record.sequence, 10), TimestampUnixNano: record.timestamp, Stream: record.stream, Data: record.data, Truncated: dropped != 0, DroppedRecords: dropped}); err != nil {
				return err
			}
			after = record.sequence
		}
		if !request.GetFollow() {
			return nil
		}
		container, err := r.InspectContainer(ctx, request.GetActorNamespaceId(), request.GetContainerId())
		if err != nil {
			return err
		}
		if container.GetGenerationId() != generationID {
			return detailedError(codes.Aborted, "LOG_CURSOR_STALE", "container was replaced while logs were streaming", "container", request.GetContainerId())
		}
		if len(records) == 0 && container.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func parseLogCursor(cursor, containerID, generationID string) (uint64, error) {
	parts := strings.Split(cursor, ":")
	if len(parts) != 3 || parts[0] != containerID {
		return 0, errors.New("invalid log cursor")
	}
	if parts[1] != generationID {
		return 0, detailedError(codes.Aborted, "LOG_CURSOR_STALE", "log cursor belongs to a replaced container", "container", containerID)
	}
	value, err := strconv.ParseUint(parts[2], 10, 64)
	if err != nil {
		return 0, errors.New("invalid log cursor")
	}
	return value, nil
}

type storedLogRecord struct {
	sequence  uint64
	timestamp int64
	stream    vmmdv1.LogStream
	data      []byte
}

func readLogRecords(path string, offset int64) ([]storedLogRecord, int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, offset, err
	}
	defer file.Close()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, offset, err
	}
	reader := bufio.NewReader(file)
	var records []storedLogRecord
	for {
		header := make([]byte, logHeaderBytes)
		if _, err := io.ReadFull(reader, header); errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			break
		} else if err != nil {
			return nil, offset, err
		}
		length := binary.BigEndian.Uint32(header[17:21])
		data := make([]byte, length)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, offset, err
		}
		offset += int64(logHeaderBytes) + int64(length)
		records = append(records, storedLogRecord{sequence: binary.BigEndian.Uint64(header[0:8]), timestamp: int64(binary.BigEndian.Uint64(header[8:16])), stream: vmmdv1.LogStream(header[16]), data: data})
	}
	return records, offset, nil
}

func (r *Runtime) SignalContainer(ctx context.Context, namespaceID, containerID string, signal syscall.Signal) error {
	nsctx, container, current, err := r.loadContainer(ctx, namespaceID, containerID)
	if err != nil {
		return err
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return failedPrecondition("container is not running")
	}
	task, err := container.Task(nsctx, nil)
	if err != nil {
		return err
	}
	return task.Kill(nsctx, signal)
}
