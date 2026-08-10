package mcpserver

import "time"

const maxResultBytes = 2 << 20

type EmptyInput struct{}

type PageInput struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
}

type ContainerListInput struct {
	Limit  int    `json:"limit,omitempty"`
	Cursor string `json:"cursor,omitempty"`
	State  string `json:"state,omitempty"`
}

type ContainerInput struct {
	ContainerID string `json:"container_id"`
}

type ContainerWaitInput struct {
	ContainerID    string `json:"container_id"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}

type ContainerLogsInput struct {
	ContainerID string `json:"container_id"`
	TailRecords int    `json:"tail_records,omitempty"`
	Since       string `json:"since,omitempty"`
	AfterCursor string `json:"after_cursor,omitempty"`
	MaxBytes    int    `json:"max_bytes,omitempty"`
}

type CleanupPlanInput struct {
	Kind string `json:"kind"`
}

type ImagePullInput struct {
	Reference string `json:"reference"`
}

type ContainerRunInput struct {
	ContainerID   string            `json:"container_id"`
	Image         string            `json:"image"`
	Entrypoint    *[]string         `json:"entrypoint,omitempty"`
	Command       *[]string         `json:"command,omitempty"`
	Env           map[string]string `json:"env,omitempty"`
	Workdir       string            `json:"workdir,omitempty"`
	User          string            `json:"user,omitempty"`
	Volumes       []MountData       `json:"volumes,omitempty"`
	ReadOnlyRoot  bool              `json:"read_only_root,omitempty"`
	LogLimitBytes uint64            `json:"log_limit_bytes,omitempty"`
}

type ContainerExecInput struct {
	ContainerID    string            `json:"container_id"`
	Argv           []string          `json:"argv"`
	Stdin          string            `json:"stdin,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Workdir        string            `json:"workdir,omitempty"`
	User           string            `json:"user,omitempty"`
	TimeoutSeconds int               `json:"timeout_seconds,omitempty"`
	MaxOutputBytes int               `json:"max_output_bytes,omitempty"`
}

type ContainerStopInput struct {
	ContainerID    string `json:"container_id"`
	TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
}
type ContainerSignalInput struct {
	ContainerID string `json:"container_id"`
	Signal      string `json:"signal"`
}
type VolumeInput struct {
	VolumeID string  `json:"volume_id"`
	OwnerUID *uint32 `json:"owner_uid,omitempty"`
	OwnerGID *uint32 `json:"owner_gid,omitempty"`
	Mode     *uint32 `json:"mode,omitempty"`
}

type ImageData struct {
	Reference          string `json:"reference"`
	RequestedReference string `json:"requested_reference,omitempty"`
	Digest             string `json:"digest,omitempty"`
	LogicalBytes       uint64 `json:"logical_bytes"`
	Status             string `json:"status"`
}
type ImagePage struct {
	Items      []ImageData `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}
type RunData struct {
	Container ContainerData `json:"container"`
	Started   bool          `json:"started"`
}
type ExecData struct {
	ExitCode  uint32 `json:"exit_code"`
	Stdout    string `json:"stdout"`
	Stderr    string `json:"stderr"`
	Truncated bool   `json:"truncated"`
}
type EmptyData struct{}

type Envelope struct {
	OK    bool       `json:"ok"`
	Data  any        `json:"data,omitempty"`
	Error *ToolError `json:"error,omitempty"`
}

type ToolError struct {
	Code      string    `json:"code"`
	Message   string    `json:"message"`
	Retryable bool      `json:"retryable"`
	Resource  *Resource `json:"resource,omitempty"`
}

type Resource struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

type EnvironmentData struct {
	Origin     string     `json:"origin"`
	Limits     LimitsData `json:"limits"`
	EgressMode string     `json:"egress_mode"`
	Health     string     `json:"health"`
	Usage      UsageData  `json:"usage"`
	Containers int        `json:"containers"`
	Volumes    int        `json:"volumes"`
}

type LimitsData struct {
	CPUMillis  uint32 `json:"cpu_millis,omitempty"`
	MemoryByte uint64 `json:"memory_bytes,omitempty"`
	PIDs       uint32 `json:"pids,omitempty"`
	DiskBytes  uint64 `json:"disk_bytes,omitempty"`
}

type UsageData struct {
	CPUNanos       uint64 `json:"cpu_nanos"`
	MemoryBytes    uint64 `json:"memory_bytes"`
	PIDs           uint32 `json:"pids"`
	ReadBytes      uint64 `json:"read_bytes"`
	WriteBytes     uint64 `json:"write_bytes"`
	DiskBytes      uint64 `json:"disk_bytes"`
	NetworkRXBytes uint64 `json:"network_rx_bytes"`
	NetworkTXBytes uint64 `json:"network_tx_bytes"`
	Stale          bool   `json:"stale"`
}

type ContainerData struct {
	ID         string      `json:"id"`
	InstanceID string      `json:"instance_id,omitempty"`
	Image      string      `json:"image"`
	State      string      `json:"state"`
	ExitCode   *uint32     `json:"exit_code,omitempty"`
	Volumes    []MountData `json:"volumes,omitempty"`
	Argv       []string    `json:"argv,omitempty"`
	Workdir    string      `json:"workdir,omitempty"`
	User       string      `json:"user,omitempty"`
	Usage      UsageData   `json:"usage"`
}

type MountData struct {
	VolumeID    string `json:"volume_id"`
	Destination string `json:"destination"`
	ReadOnly    bool   `json:"read_only"`
}

type ContainerPage struct {
	Items      []ContainerData `json:"items"`
	NextCursor string          `json:"next_cursor,omitempty"`
}

type WaitData struct {
	ExitCode uint32 `json:"exit_code"`
}

type LogData struct {
	Records        []LogRecordData `json:"records"`
	NextCursor     string          `json:"next_cursor,omitempty"`
	Truncated      bool            `json:"truncated"`
	DroppedRecords uint64          `json:"dropped_records"`
}

type LogRecordData struct {
	Cursor    string    `json:"cursor"`
	Timestamp time.Time `json:"timestamp"`
	Stream    string    `json:"stream"`
	Text      string    `json:"text"`
	Truncated bool      `json:"truncated"`
}

type VolumeData struct {
	ID         string    `json:"id"`
	Usage      UsageData `json:"usage"`
	LimitBytes uint64    `json:"limit_bytes"`
	MountedBy  []string  `json:"mounted_by,omitempty"`
}

type VolumePage struct {
	Items      []VolumeData `json:"items"`
	NextCursor string       `json:"next_cursor,omitempty"`
}

type CleanupPlanData struct {
	Kind     string              `json:"kind"`
	Revision string              `json:"revision"`
	Impacts  []CleanupImpactData `json:"impacts"`
}

type CleanupImpactData struct {
	Resource     Resource `json:"resource"`
	Effect       string   `json:"effect"`
	Irreversible bool     `json:"irreversible"`
	Blocker      bool     `json:"blocker"`
}

type ForwardData struct {
	ID                  string    `json:"id"`
	ContainerID         string    `json:"container_id"`
	ContainerInstanceID string    `json:"container_instance_id"`
	RemotePort          uint32    `json:"remote_port"`
	LocalPort           uint32    `json:"local_port"`
	ExpiresAt           time.Time `json:"expires_at"`
}

type ForwardListData struct {
	Items []ForwardData `json:"items"`
}

type PortForwardStartInput struct {
	ContainerID string `json:"container_id"`
	Port        uint32 `json:"port"`
	LocalPort   uint32 `json:"local_port,omitempty"`
	TTLSeconds  int    `json:"ttl_seconds,omitempty"`
}
type PortForwardStopInput struct {
	ForwardID string `json:"forward_id"`
}
type CleanupExecuteInput struct {
	Kind                string   `json:"kind"`
	PlanRevision        string   `json:"plan_revision"`
	ConfirmReset        bool     `json:"confirm_reset,omitempty"`
	ConfirmIrreversible bool     `json:"confirm_irreversible,omitempty"`
	ConfirmedVolumeIDs  []string `json:"confirmed_volume_ids,omitempty"`
}
type OperationData struct {
	ID    string     `json:"id"`
	Kind  string     `json:"kind"`
	State string     `json:"state"`
	Stage string     `json:"stage,omitempty"`
	Error *ToolError `json:"error,omitempty"`
}
