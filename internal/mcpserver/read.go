package mcpserver

import (
	"context"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (adapter *Adapter) registerReadTools(server *mcp.Server) {
	addTool(server, toolDefinition{name: "environment_get", description: "Inspect this session's environment health, limits, usage, and resource counts.", inputSchema: emptySchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.environmentGet)
	addTool(server, toolDefinition{name: "container_list", description: "List containers in this environment with bounded pagination.", inputSchema: containerListSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.containerList)
	addTool(server, toolDefinition{name: "container_get", description: "Inspect one container in this environment.", inputSchema: containerSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.containerGet)
	addTool(server, toolDefinition{name: "container_wait", description: "Wait for one container to stop, with a bounded timeout.", inputSchema: containerWaitSchema, readOnly: true, idempotent: true, timeout: 10 * time.Minute}, adapter.containerWait)
	addTool(server, toolDefinition{name: "container_logs", description: "Read a bounded snapshot of container logs using an opaque cursor.", inputSchema: containerLogsSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.containerLogs)
	addTool(server, toolDefinition{name: "volume_list", description: "List named volumes in this environment with bounded pagination.", inputSchema: pageSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.volumeList)
	addTool(server, toolDefinition{name: "image_list", description: "List canonical image references visible in this environment.", inputSchema: pageSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.imageList)
	addTool(server, toolDefinition{name: "cleanup_plan", description: "Plan an environment reset or irreversible discard without executing it.", inputSchema: cleanupPlanSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.cleanupPlan)
}

func (adapter *Adapter) imageList(ctx context.Context, _ *mcp.CallToolRequest, input PageInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.ListImages(ctx, &hostv1.ListImagesRequest{})
		if err != nil {
			return nil, err
		}
		items := make([]ImageData, 0, len(response.GetImages()))
		for _, image := range response.GetImages() {
			items = append(items, ImageData{Reference: image.GetReference(), Digest: image.GetDigest(), LogicalBytes: image.GetLogicalBytes()})
		}
		slices.SortFunc(items, func(a, b ImageData) int { return strings.Compare(a.Reference, b.Reference) })
		start, end, next, err := pageBounds(len(items), input.Limit, input.Cursor)
		if err != nil {
			return nil, err
		}
		return ImagePage{Items: items[start:end], NextCursor: next}, nil
	})
}

func (adapter *Adapter) environmentGet(ctx context.Context, _ *mcp.CallToolRequest, _ EmptyInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.GetEnvironment(ctx, &hostv1.GetOwnEnvironmentRequest{})
		if err != nil {
			return nil, err
		}
		environment := response.GetEnvironment()
		if environment == nil {
			return nil, status.Error(codes.Internal, "runtime session returned no environment")
		}
		return environmentData(environment), nil
	})
}

func (adapter *Adapter) containerList(ctx context.Context, _ *mcp.CallToolRequest, input ContainerListInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.ListContainers(ctx, &hostv1.ListContainersRequest{})
		if err != nil {
			return nil, err
		}
		items := make([]ContainerData, 0, len(response.GetContainers()))
		for _, container := range response.GetContainers() {
			item := containerData(container)
			if input.State == "" || item.State == input.State {
				items = append(items, item)
			}
		}
		slices.SortFunc(items, func(a, b ContainerData) int { return strings.Compare(a.ID, b.ID) })
		start, end, next, err := pageBounds(len(items), input.Limit, input.Cursor)
		if err != nil {
			return nil, err
		}
		return ContainerPage{Items: items[start:end], NextCursor: next}, nil
	})
}

func (adapter *Adapter) containerGet(ctx context.Context, _ *mcp.CallToolRequest, input ContainerInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.GetContainer(ctx, &hostv1.ContainerRequest{ContainerId: input.ContainerID})
		if err != nil {
			return nil, err
		}
		return containerData(response.GetContainer()), nil
	})
}

func (adapter *Adapter) containerWait(ctx context.Context, _ *mcp.CallToolRequest, input ContainerWaitInput) (*mcp.CallToolResult, Envelope, error) {
	timeout := input.TimeoutSeconds
	if timeout == 0 {
		timeout = 600
	}
	if timeout < 1 || timeout > 600 {
		return adapter.failure(status.Error(codes.InvalidArgument, "invalid timeout"))
	}
	return adapter.call(ctx, time.Duration(timeout)*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.WaitContainer(ctx, &hostv1.ContainerRequest{ContainerId: input.ContainerID})
		if err != nil {
			return nil, err
		}
		return WaitData{ExitCode: response.GetExitCode()}, nil
	})
}

func (adapter *Adapter) containerLogs(ctx context.Context, _ *mcp.CallToolRequest, input ContainerLogsInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		maxBytes := input.MaxBytes
		if maxBytes == 0 {
			maxBytes = 64 << 10
		}
		if maxBytes < 1 || maxBytes > 256<<10 || input.TailRecords < 0 || input.TailRecords > 1000 {
			return nil, status.Error(codes.InvalidArgument, "invalid log bounds")
		}
		var since *timestamppb.Timestamp
		if input.Since != "" {
			parsed, err := time.Parse(time.RFC3339, input.Since)
			if err != nil {
				return nil, status.Error(codes.InvalidArgument, "invalid since timestamp")
			}
			since = timestamppb.New(parsed)
		}
		stream, err := adapter.runtime.Logs(ctx, &hostv1.LogsRequest{ContainerId: input.ContainerID, TailRecords: uint64(input.TailRecords), Since: since, AfterCursor: input.AfterCursor})
		if err != nil {
			return nil, err
		}
		result := LogData{}
		used := 0
		for {
			record, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, err
			}
			text := strings.ToValidUTF8(string(record.GetData()), "�")
			remaining := maxBytes - used
			if len(text) > remaining {
				text = validPrefix(text, remaining)
				result.Truncated = true
			}
			result.Records = append(result.Records, LogRecordData{Cursor: record.GetCursor(), Timestamp: record.GetTimestamp().AsTime(), Stream: logStream(record.GetStream()), Text: text, Truncated: record.GetTruncated() || len(text) < len(record.GetData())})
			result.NextCursor = record.GetCursor()
			result.DroppedRecords += record.GetDroppedRecords()
			used += len(text)
			if used >= maxBytes {
				break
			}
		}
		return result, nil
	})
}

func (adapter *Adapter) volumeList(ctx context.Context, _ *mcp.CallToolRequest, input PageInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		response, err := adapter.runtime.ListVolumes(ctx, &hostv1.ListVolumesRequest{})
		if err != nil {
			return nil, err
		}
		items := make([]VolumeData, 0, len(response.GetVolumes()))
		for _, volume := range response.GetVolumes() {
			items = append(items, volumeData(volume))
		}
		slices.SortFunc(items, func(a, b VolumeData) int { return strings.Compare(a.ID, b.ID) })
		start, end, next, err := pageBounds(len(items), input.Limit, input.Cursor)
		if err != nil {
			return nil, err
		}
		return VolumePage{Items: items[start:end], NextCursor: next}, nil
	})
}

func (adapter *Adapter) cleanupPlan(ctx context.Context, _ *mcp.CallToolRequest, input CleanupPlanInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		if input.Kind != "reset" && input.Kind != "discard" {
			return nil, status.Error(codes.InvalidArgument, "invalid cleanup kind")
		}
		plan, err := adapter.cleanup.PlanCleanup(ctx, input.Kind)
		if err != nil {
			return nil, err
		}
		if plan.Kind != input.Kind || plan.Revision == 0 {
			return nil, status.Error(codes.Aborted, "cleanup plan kind changed")
		}
		result := CleanupPlanData{Kind: plan.Kind, Revision: strconv.FormatUint(plan.Revision, 10)}
		for _, impact := range plan.Impacts {
			result.Impacts = append(result.Impacts, CleanupImpactData{Resource: impact.Resource, Effect: impact.Effect, Irreversible: impact.Irreversible, Blocker: impact.Blocker})
		}
		return result, nil
	})
}

func environmentData(value *hostv1.AgentEnvironment) EnvironmentData {
	return EnvironmentData{Origin: strings.ToLower(strings.TrimPrefix(value.GetOrigin().String(), "ENVIRONMENT_ORIGIN_")), Limits: limitsData(value.GetLimits()), EgressMode: strings.ToLower(strings.TrimPrefix(value.GetEgressMode().String(), "EGRESS_MODE_")), Health: strings.ToLower(strings.TrimPrefix(value.GetHealth().GetState().String(), "HEALTH_STATE_")), Usage: usageData(value.GetUsage()), Containers: len(value.GetContainers()), Volumes: len(value.GetVolumes())}
}

func limitsData(value *hostv1.ResourceLimits) LimitsData {
	if value == nil {
		return LimitsData{}
	}
	return LimitsData{CPUMillis: value.GetCpuMillis(), MemoryByte: value.GetMemoryBytes(), PIDs: value.GetPids(), DiskBytes: value.GetDiskBytes()}
}
func usageData(value *hostv1.Usage) UsageData {
	if value == nil {
		return UsageData{}
	}
	return UsageData{CPUNanos: value.GetCpuNanos(), MemoryBytes: value.GetMemoryBytes(), PIDs: value.GetPids(), ReadBytes: value.GetReadBytes(), WriteBytes: value.GetWriteBytes(), DiskBytes: value.GetDiskBytes(), NetworkRXBytes: value.GetNetworkRxBytes(), NetworkTXBytes: value.GetNetworkTxBytes(), Stale: value.GetStale()}
}
func containerData(value *hostv1.Container) ContainerData {
	result := ContainerData{ID: value.GetId(), InstanceID: value.GetInstanceId(), Image: value.GetImage(), State: strings.ToLower(strings.TrimPrefix(value.GetState().String(), "CONTAINER_STATE_")), ExitCode: value.ExitCode, Argv: append([]string(nil), value.GetArgv()...), Workdir: value.GetWorkdir(), User: value.GetUser(), Usage: usageData(value.GetUsage())}
	for _, mount := range value.GetVolumes() {
		result.Volumes = append(result.Volumes, MountData{OwnedVolume: mount.GetOwnedVolume(), HostShare: mount.GetHostShare(), Destination: mount.GetDestination(), ReadOnly: mount.GetReadOnly()})
	}
	return result
}
func volumeData(value *hostv1.Volume) VolumeData {
	return VolumeData{ID: value.GetId(), Usage: usageData(value.GetUsage()), LimitBytes: value.GetLimitBytes(), MountedBy: append([]string(nil), value.GetMountedBy()...)}
}
func logStream(value hostv1.LogStream) string {
	if value == hostv1.LogStream_LOG_STREAM_STDERR {
		return "stderr"
	}
	return "stdout"
}
func validPrefix(value string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.RuneStart(value[limit]) {
		limit--
	}
	return value[:limit]
}

const emptySchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{}}`
const pageSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":100},"cursor":{"type":"string","maxLength":1024}}}`
const containerListSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"properties":{"limit":{"type":"integer","minimum":1,"maximum":100},"cursor":{"type":"string","maxLength":1024},"state":{"enum":["created","running","stopped","unknown"]}}}`
const containerSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"}}}`
const containerWaitSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"timeout_seconds":{"type":"integer","minimum":1,"maximum":600}}}`
const containerLogsSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"tail_records":{"type":"integer","minimum":0,"maximum":1000},"since":{"type":"string","format":"date-time"},"after_cursor":{"type":"string","maxLength":1024},"max_bytes":{"type":"integer","minimum":1,"maximum":262144}}}`
const cleanupPlanSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["kind"],"properties":{"kind":{"enum":["reset","discard"]}}}`
