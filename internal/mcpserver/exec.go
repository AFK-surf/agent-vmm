package mcpserver

import (
	"context"
	"io"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (adapter *Adapter) containerExec(ctx context.Context, _ *mcp.CallToolRequest, input ContainerExecInput) (*mcp.CallToolResult, Envelope, error) {
	timeout := input.TimeoutSeconds
	if timeout == 0 {
		timeout = 600
	}
	maxOutput := input.MaxOutputBytes
	if maxOutput == 0 {
		maxOutput = 128 << 10
	}
	if timeout < 1 || timeout > 600 || maxOutput < 1 || maxOutput > 128<<10 {
		return adapter.failure(status.Error(codes.InvalidArgument, "invalid exec bounds"))
	}
	return adapter.call(ctx, time.Duration(timeout)*time.Second, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		stream, err := adapter.runtime.Exec(ctx)
		if err != nil {
			return nil, err
		}
		if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Start{Start: &hostv1.ExecStart{
			RequestId: requestID, ContainerId: input.ContainerID, Argv: input.Argv,
			Env: environmentList(input.Env), Workdir: optionalString(input.Workdir), User: optionalString(input.User), Tty: false,
		}}}); err != nil {
			return nil, err
		}
		if input.Stdin != "" {
			if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_Stdin{Stdin: []byte(input.Stdin)}}); err != nil {
				return nil, err
			}
		}
		if err := stream.Send(&hostv1.ExecRequest{Event: &hostv1.ExecRequest_CloseStdin{CloseStdin: true}}); err != nil {
			return nil, err
		}
		if err := stream.CloseSend(); err != nil {
			return nil, err
		}
		stdout, stderr := newBoundedText((maxOutput+1)/2), newBoundedText(maxOutput/2)
		var exitCode uint32
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			switch event := response.GetEvent().(type) {
			case *hostv1.ExecResponse_Stdout:
				stdout.Write(event.Stdout)
			case *hostv1.ExecResponse_Stderr:
				stderr.Write(event.Stderr)
			case *hostv1.ExecResponse_ExitCode:
				exitCode = event.ExitCode
			}
		}
		return ExecData{ExitCode: exitCode, Stdout: stdout.String(), Stderr: stderr.String(), Truncated: stdout.truncated || stderr.truncated}, nil
	})
}

type boundedText struct {
	limit      int
	head, tail []byte
	total      int
	truncated  bool
}

func newBoundedText(limit int) *boundedText { return &boundedText{limit: limit} }
func (buffer *boundedText) Write(value []byte) {
	buffer.total += len(value)
	half := buffer.limit / 2
	if len(buffer.head) < half {
		take := min(half-len(buffer.head), len(value))
		buffer.head = append(buffer.head, value[:take]...)
		value = value[take:]
	}
	if len(value) > 0 {
		buffer.tail = append(buffer.tail, value...)
		if len(buffer.tail) > buffer.limit-len(buffer.head) {
			buffer.tail = append([]byte(nil), buffer.tail[len(buffer.tail)-(buffer.limit-len(buffer.head)):]...)
		}
	}
	buffer.truncated = buffer.total > buffer.limit
}
func (buffer *boundedText) String() string {
	value := append(append([]byte(nil), buffer.head...), buffer.tail...)
	text := strings.ToValidUTF8(string(value), "�")
	if buffer.truncated {
		return validPrefix(text, buffer.limit)
	}
	return text
}
