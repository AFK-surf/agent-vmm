//go:build linux

package control

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"syscall"
	"time"

	vmmdv1 "github.com/AFK-surf/agent-vmm/api/vmmd/v1"
	containerd "github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/pkg/cio"
	specs "github.com/opencontainers/runtime-spec/specs-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type execRecord struct {
	fingerprint string
	done        chan struct{}
	exitCode    uint32
	err         error
	completed   time.Time
}

type processOutput struct {
	stderr bool
	data   []byte
}

func (s *Server) Exec(stream grpc.BidiStreamingServer[vmmdv1.ExecRequest, vmmdv1.ExecResponse]) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	start := first.GetStart()
	if start == nil {
		return status.Error(codes.InvalidArgument, "first exec event must be start")
	}
	if err := validateExecStart(start); err != nil {
		return invalid(err)
	}
	fingerprintBytes, _ := proto.MarshalOptions{Deterministic: true}.Marshal(start)
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(fingerprintBytes))
	key := start.GetActorNamespaceId() + "/" + start.GetContainerId() + "/" + start.GetRequestId()
	record, leader, err := s.beginExec(key, fingerprint)
	if err != nil {
		return invalid(err)
	}
	if !leader {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case <-record.done:
			if record.err != nil {
				return rpcError(record.err)
			}
			return stream.Send(&vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_ExitCode{ExitCode: record.exitCode}})
		}
	}
	exitCode, runErr := s.runtime.exec(stream, start)
	s.finishExec(key, record, exitCode, runErr)
	return rpcError(runErr)
}

func (s *Server) beginExec(key, fingerprint string) (*execRecord, bool, error) {
	s.execMu.Lock()
	defer s.execMu.Unlock()
	if s.executions == nil {
		s.executions = make(map[string]*execRecord)
	}
	if record := s.executions[key]; record != nil {
		if record.fingerprint != fingerprint {
			return nil, false, errors.New("request_id was reused with different exec input")
		}
		if record.completed.IsZero() || time.Since(record.completed) < dedupeTTL {
			return record, false, nil
		}
		delete(s.executions, key)
	}
	if len(s.executions) >= 64 {
		return nil, false, errors.New("too many concurrent exec requests")
	}
	record := &execRecord{fingerprint: fingerprint, done: make(chan struct{})}
	s.executions[key] = record
	return record, true, nil
}

func (s *Server) finishExec(key string, record *execRecord, code uint32, err error) {
	s.execMu.Lock()
	record.exitCode, record.err, record.completed = code, err, time.Now()
	close(record.done)
	if len(s.executions) > dedupeLimit {
		delete(s.executions, key)
	}
	s.execMu.Unlock()
}

func validateExecStart(start *vmmdv1.ExecStart) error {
	if err := validateRequestID(start.GetRequestId()); err != nil {
		return err
	}
	if err := validateID("actor_namespace_id", start.GetActorNamespaceId()); err != nil {
		return err
	}
	if err := validateID("container_id", start.GetContainerId()); err != nil {
		return err
	}
	if len(start.GetArgv()) == 0 || len(start.GetArgv()) > 256 || len(start.GetEnv()) > 256 {
		return errors.New("exec argv is required and bounded to 256 entries")
	}
	if start.GetTty() && (start.GetRows() == 0 || start.GetColumns() == 0) {
		return errors.New("TTY dimensions are required")
	}
	if err := validateEnvironment(start.GetEnv()); err != nil {
		return err
	}
	return validateProcessStrings(start.GetArgv(), start.GetWorkdir(), start.GetUser())
}

func (r *Runtime) exec(stream grpc.BidiStreamingServer[vmmdv1.ExecRequest, vmmdv1.ExecResponse], start *vmmdv1.ExecStart) (uint32, error) {
	ctx := stream.Context()
	nsctx, container, current, err := r.loadContainer(ctx, start.GetActorNamespaceId(), start.GetContainerId())
	if err != nil {
		return 0, err
	}
	if current.GetState() != vmmdv1.ContainerState_CONTAINER_STATE_RUNNING {
		return 0, errors.New("container is not running")
	}
	task, err := container.Task(nsctx, nil)
	if err != nil {
		return 0, err
	}
	baseProcess, err := containerMainProcess(nsctx, container)
	if err != nil {
		return 0, err
	}
	process := *baseProcess
	process.Args = append([]string(nil), start.GetArgv()...)
	process.Env = mergeEnvironment(process.Env, start.GetEnv())
	if start.Workdir != nil {
		process.Cwd = start.GetWorkdir()
	}
	if start.User != nil {
		user, err := numericUser(start.GetUser())
		if err != nil {
			return 0, err
		}
		process.User = user
	}
	process.Terminal = start.GetTty()
	if start.GetTty() {
		process.ConsoleSize = &specs.Box{Height: uint(start.GetRows()), Width: uint(start.GetColumns())}
	} else {
		process.ConsoleSize = nil
	}
	stdinReader, stdinWriter := io.Pipe()
	defer stdinWriter.Close()
	output := make(chan processOutput, 16)
	stdout := &streamOutput{ctx: ctx, output: output}
	var stderr io.Writer = &streamOutput{ctx: ctx, output: output, stderr: true}
	options := []cio.Opt{cio.WithStreams(stdinReader, stdout, stderr)}
	if start.GetTty() {
		options = []cio.Opt{cio.WithStreams(stdinReader, stdout, nil), cio.WithTerminal}
	}
	processHandle, err := task.Exec(nsctx, "exec-"+start.GetRequestId(), &process, cio.NewCreator(options...))
	if err != nil {
		return 0, err
	}
	defer processHandle.Delete(context.WithoutCancel(nsctx), containerd.WithProcessKill)
	exit, err := processHandle.Wait(nsctx)
	if err != nil {
		return 0, err
	}
	if err := processHandle.Start(nsctx); err != nil {
		return 0, err
	}
	inputErrors := make(chan error, 1)
	go receiveExecInput(stream, processHandle, stdinWriter, inputErrors)
	finished := make(chan containerd.ExitStatus, 1)
	go func() { finished <- <-exit }()
	ioDone := make(chan struct{})
	go func() {
		processHandle.IO().Wait()
		close(output)
		close(ioDone)
	}()
	var exitStatus *containerd.ExitStatus
	outputClosed := false
	for {
		select {
		case event, ok := <-output:
			if !ok {
				outputClosed = true
				output = nil
				if exitStatus != nil {
					return sendExecExit(stream, *exitStatus)
				}
				continue
			}
			response := &vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_Stdout{Stdout: event.data}}
			if event.stderr {
				response.Event = &vmmdv1.ExecResponse_Stderr{Stderr: event.data}
			}
			if err := stream.Send(response); err != nil {
				_ = processHandle.Kill(context.WithoutCancel(nsctx), syscall.SIGKILL)
				return 0, err
			}
		case err := <-inputErrors:
			if err != nil && !errors.Is(err, io.EOF) {
				_ = processHandle.Kill(context.WithoutCancel(nsctx), syscall.SIGKILL)
				return 0, err
			}
			inputErrors = nil
		case status := <-finished:
			exitStatus = &status
			finished = nil
			if outputClosed {
				return sendExecExit(stream, status)
			}
		case <-ctx.Done():
			_ = processHandle.Kill(context.WithoutCancel(nsctx), syscall.SIGKILL)
			processHandle.IO().Cancel()
			<-ioDone
			return 0, ctx.Err()
		}
	}
}

func sendExecExit(stream grpc.BidiStreamingServer[vmmdv1.ExecRequest, vmmdv1.ExecResponse], result containerd.ExitStatus) (uint32, error) {
	code, _, err := result.Result()
	if err != nil {
		return 0, err
	}
	if err := stream.Send(&vmmdv1.ExecResponse{Event: &vmmdv1.ExecResponse_ExitCode{ExitCode: code}}); err != nil {
		return 0, err
	}
	return code, nil
}

func receiveExecInput(stream grpc.BidiStreamingServer[vmmdv1.ExecRequest, vmmdv1.ExecResponse], process containerd.Process, stdin *io.PipeWriter, result chan<- error) {
	for {
		request, err := stream.Recv()
		if err != nil {
			_ = stdin.Close()
			result <- err
			return
		}
		switch event := request.Event.(type) {
		case *vmmdv1.ExecRequest_Stdin:
			_, err = stdin.Write(event.Stdin)
		case *vmmdv1.ExecRequest_CloseStdin:
			if !event.CloseStdin {
				err = errors.New("close_stdin event must be true")
			} else {
				err = stdin.Close()
			}
		case *vmmdv1.ExecRequest_Resize:
			if event.Resize.GetRows() == 0 || event.Resize.GetColumns() == 0 {
				err = errors.New("resize dimensions must be positive")
			} else {
				err = process.Resize(stream.Context(), event.Resize.GetColumns(), event.Resize.GetRows())
			}
		case *vmmdv1.ExecRequest_Signal:
			var signal syscall.Signal
			signal, err = processSignal(event.Signal)
			if err == nil {
				err = process.Kill(stream.Context(), signal)
			}
		default:
			err = errors.New("unexpected exec event")
		}
		if err != nil {
			result <- err
			return
		}
	}
}

type streamOutput struct {
	ctx    context.Context
	output chan<- processOutput
	stderr bool
}

func (writer *streamOutput) Write(data []byte) (int, error) {
	copyData := append([]byte(nil), data...)
	select {
	case writer.output <- processOutput{stderr: writer.stderr, data: copyData}:
		return len(data), nil
	case <-writer.ctx.Done():
		return 0, writer.ctx.Err()
	}
}

func mergeEnvironment(base, overrides []string) []string {
	values := make(map[string]string, len(base)+len(overrides))
	order := make([]string, 0, len(base)+len(overrides))
	for _, entry := range append(append([]string(nil), base...), overrides...) {
		key, _, _ := strings.Cut(entry, "=")
		if _, exists := values[key]; !exists {
			order = append(order, key)
		}
		values[key] = entry
	}
	result := make([]string, 0, len(values))
	for _, key := range order {
		result = append(result, values[key])
	}
	return result
}

func numericUser(value string) (specs.User, error) {
	user, group, hasGroup := strings.Cut(value, ":")
	uid, err := strconv.ParseUint(user, 10, 32)
	if err != nil {
		return specs.User{}, errors.New("exec user must be a numeric uid or uid:gid")
	}
	result := specs.User{UID: uint32(uid)}
	if hasGroup {
		gid, err := strconv.ParseUint(group, 10, 32)
		if err != nil {
			return specs.User{}, errors.New("exec user must be a numeric uid or uid:gid")
		}
		result.GID = uint32(gid)
	}
	return result, nil
}
