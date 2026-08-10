package mcpserver

import (
	"context"
	"io"
	"slices"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (adapter *Adapter) registerMutationTools(server *mcp.Server) {
	addTool(server, toolDefinition{name: "image_pull", description: "Pull one image explicitly and report bounded progress.", inputSchema: imagePullSchema, openWorld: true, timeout: 10 * time.Minute}, adapter.imagePull)
	addTool(server, toolDefinition{name: "container_run", description: "Create then start one container. The image ENTRYPOINT and CMD are preserved unless entrypoint or command is explicitly provided.", inputSchema: containerRunSchema, destructive: true, openWorld: true, timeout: 2 * time.Minute}, adapter.containerRun)
	addTool(server, toolDefinition{name: "container_start", description: "Start one existing created or stopped container.", inputSchema: containerSchema, openWorld: true, timeout: 2 * time.Minute}, adapter.containerStart)
	addTool(server, toolDefinition{name: "container_exec", description: "Execute a non-interactive process with explicit argv; use an explicit shell argv when shell syntax is required.", inputSchema: containerExecSchema, destructive: true, openWorld: true, timeout: 10 * time.Minute}, adapter.containerExec)
	addTool(server, toolDefinition{name: "container_stop", description: "Stop one running container with a bounded grace period.", inputSchema: containerStopSchema, destructive: true, timeout: 2 * time.Minute}, adapter.containerStop)
	addTool(server, toolDefinition{name: "container_signal", description: "Send INT, TERM, or KILL to one running container.", inputSchema: containerSignalSchema, destructive: true, timeout: 30 * time.Second}, adapter.containerSignal)
	addTool(server, toolDefinition{name: "container_delete", description: "Delete one stopped container without deleting its named volumes.", inputSchema: containerSchema, destructive: true, timeout: 2 * time.Minute}, adapter.containerDelete)
	addTool(server, toolDefinition{name: "volume_create", description: "Create one named volume, optionally initializing its numeric owner and permission mode for a non-root image.", inputSchema: volumeSchema, timeout: 2 * time.Minute}, adapter.volumeCreate)
	addTool(server, toolDefinition{name: "volume_delete", description: "Delete one unmounted named volume.", inputSchema: volumeSchema, destructive: true, timeout: 2 * time.Minute}, adapter.volumeDelete)
}

func (adapter *Adapter) imagePull(ctx context.Context, request *mcp.CallToolRequest, input ImagePullInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 10*time.Minute, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		stream, err := adapter.runtime.PullImage(ctx, &hostv1.PullImageRequest{RequestId: requestID, Reference: input.Reference})
		if err != nil {
			return nil, err
		}
		result := ImageData{RequestedReference: input.Reference}
		progress := 0.0
		for {
			update, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			result.Status, result.Reference, result.Digest, result.LogicalBytes = strings.ToValidUTF8(update.GetStatus(), "�"), update.GetCanonicalReference(), update.GetDigest(), update.GetLogicalBytes()
			if token := request.Params.GetProgressToken(); token != nil {
				progress++
				_ = request.Session.NotifyProgress(ctx, &mcp.ProgressNotificationParams{ProgressToken: token, Progress: progress, Message: result.Status})
			}
		}
		return result, nil
	})
}

func (adapter *Adapter) containerRun(ctx context.Context, _ *mcp.CallToolRequest, input ContainerRunInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 2*time.Minute, true, func(ctx context.Context) (any, error) {
		logLimit := input.LogLimitBytes
		if logLimit == 0 {
			logLimit = 16 << 20
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		workdir, user := optionalString(input.Workdir), optionalString(input.User)
		mounts := make([]*hostv1.VolumeMount, 0, len(input.Volumes))
		for _, mount := range input.Volumes {
			mounts = append(mounts, &hostv1.VolumeMount{VolumeId: mount.VolumeID, Destination: mount.Destination, ReadOnly: mount.ReadOnly})
		}
		create := &hostv1.CreateContainerRequest{RequestId: requestID, ContainerId: input.ContainerID, Image: input.Image, Env: environmentList(input.Env), Workdir: workdir, User: user, Volumes: mounts, ReadOnlyRoot: input.ReadOnlyRoot, LogLimitBytes: logLimit}
		if input.Entrypoint != nil {
			create.EntrypointOverride = &hostv1.StringList{Values: append([]string(nil), (*input.Entrypoint)...)}
		}
		if input.Command != nil {
			create.CommandOverride = &hostv1.StringList{Values: append([]string(nil), (*input.Command)...)}
		}
		created, err := adapter.runtime.CreateContainer(ctx, create)
		if err != nil {
			return nil, err
		}
		requestID, err = newRequestID()
		if err != nil {
			return nil, err
		}
		started, err := adapter.runtime.StartContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: requestID, ContainerId: input.ContainerID})
		if err != nil {
			return nil, &toolFailure{data: RunData{Container: containerData(created.GetContainer()), Started: false}, err: err}
		}
		return RunData{Container: containerData(started.GetContainer()), Started: true}, nil
	})
}

func (adapter *Adapter) containerStart(ctx context.Context, _ *mcp.CallToolRequest, input ContainerInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.containerMutation(ctx, input.ContainerID, 2*time.Minute, func(ctx context.Context, requestID string) (*hostv1.ContainerResponse, error) {
		return adapter.runtime.StartContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: requestID, ContainerId: input.ContainerID})
	})
}

func (adapter *Adapter) containerStop(ctx context.Context, _ *mcp.CallToolRequest, input ContainerStopInput) (*mcp.CallToolResult, Envelope, error) {
	if input.TimeoutSeconds < 0 || input.TimeoutSeconds > 120 {
		return adapter.failure(status.Error(codes.InvalidArgument, "invalid timeout"))
	}
	return adapter.containerMutation(ctx, input.ContainerID, 2*time.Minute, func(ctx context.Context, requestID string) (*hostv1.ContainerResponse, error) {
		var timeout *uint32
		if input.TimeoutSeconds > 0 {
			value := uint32(input.TimeoutSeconds)
			timeout = &value
		}
		return adapter.runtime.StopContainer(ctx, &hostv1.StopContainerRequest{RequestId: requestID, ContainerId: input.ContainerID, TimeoutSeconds: timeout})
	})
}

func (adapter *Adapter) containerMutation(ctx context.Context, id string, timeout time.Duration, operation func(context.Context, string) (*hostv1.ContainerResponse, error)) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, timeout, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		response, err := operation(ctx, requestID)
		if err != nil {
			return nil, err
		}
		return containerData(response.GetContainer()), nil
	})
}

func (adapter *Adapter) containerSignal(ctx context.Context, _ *mcp.CallToolRequest, input ContainerSignalInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(ctx context.Context) (any, error) {
		signals := map[string]hostv1.ProcessSignal{"INT": hostv1.ProcessSignal_PROCESS_SIGNAL_INT, "TERM": hostv1.ProcessSignal_PROCESS_SIGNAL_TERM, "KILL": hostv1.ProcessSignal_PROCESS_SIGNAL_KILL}
		signal, ok := signals[input.Signal]
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid signal")
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		_, err = adapter.runtime.SignalContainer(ctx, &hostv1.SignalContainerRequest{RequestId: requestID, ContainerId: input.ContainerID, Signal: signal})
		return EmptyData{}, err
	})
}

func (adapter *Adapter) containerDelete(ctx context.Context, _ *mcp.CallToolRequest, input ContainerInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 2*time.Minute, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		_, err = adapter.runtime.DeleteContainer(ctx, &hostv1.ContainerMutationRequest{RequestId: requestID, ContainerId: input.ContainerID})
		return EmptyData{}, err
	})
}

func (adapter *Adapter) volumeCreate(ctx context.Context, _ *mcp.CallToolRequest, input VolumeInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 2*time.Minute, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		response, err := adapter.runtime.CreateVolume(ctx, &hostv1.CreateVolumeRequest{RequestId: requestID, VolumeId: input.VolumeID, OwnerUid: input.OwnerUID, OwnerGid: input.OwnerGID, Mode: input.Mode})
		if err != nil {
			return nil, err
		}
		return volumeData(response.GetVolume()), nil
	})
}

func (adapter *Adapter) volumeDelete(ctx context.Context, _ *mcp.CallToolRequest, input VolumeInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 2*time.Minute, true, func(ctx context.Context) (any, error) {
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		_, err = adapter.runtime.DeleteVolume(ctx, &hostv1.DeleteVolumeRequest{RequestId: requestID, VolumeId: input.VolumeID})
		return EmptyData{}, err
	})
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}
func environmentList(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+values[key])
	}
	return result
}

const imagePullSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["reference"],"properties":{"reference":{"type":"string","minLength":1,"maxLength":512}}}`
const containerRunSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id","image"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"image":{"type":"string","minLength":1,"maxLength":512},"entrypoint":{"type":"array","maxItems":256,"items":{"type":"string","maxLength":4096}},"command":{"type":"array","maxItems":256,"items":{"type":"string","maxLength":4096}},"env":{"type":"object","maxProperties":128,"additionalProperties":{"type":"string","maxLength":65536},"propertyNames":{"pattern":"^[A-Za-z_][A-Za-z0-9_]*$"}},"workdir":{"type":"string","maxLength":4096},"user":{"type":"string","pattern":"^[0-9]+(?::[0-9]+)?$"},"volumes":{"type":"array","maxItems":64,"items":{"type":"object","additionalProperties":false,"required":["volume_id","destination"],"properties":{"volume_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"destination":{"type":"string","pattern":"^/","maxLength":4096},"read_only":{"type":"boolean"}}}},"read_only_root":{"type":"boolean"},"log_limit_bytes":{"type":"integer","minimum":1048576,"maximum":67108864}}}`
const containerExecSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id","argv"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"argv":{"type":"array","minItems":1,"maxItems":256,"items":{"type":"string","maxLength":4096}},"stdin":{"type":"string","maxLength":1048576},"env":{"type":"object","maxProperties":128,"additionalProperties":{"type":"string","maxLength":65536},"propertyNames":{"pattern":"^[A-Za-z_][A-Za-z0-9_]*$"}},"workdir":{"type":"string","maxLength":4096},"user":{"type":"string","pattern":"^[0-9]+(?::[0-9]+)?$"},"timeout_seconds":{"type":"integer","minimum":1,"maximum":600},"max_output_bytes":{"type":"integer","minimum":1,"maximum":131072}}}`
const containerStopSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"timeout_seconds":{"type":"integer","minimum":1,"maximum":120}}}`
const containerSignalSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id","signal"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"signal":{"enum":["INT","TERM","KILL"]}}}`
const volumeSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["volume_id"],"dependentRequired":{"owner_uid":["owner_gid"],"owner_gid":["owner_uid"]},"properties":{"volume_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"owner_uid":{"type":"integer","minimum":0,"maximum":4294967295},"owner_gid":{"type":"integer","minimum":0,"maximum":4294967295},"mode":{"type":"integer","minimum":0,"maximum":511}}}`
