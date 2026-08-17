package mcpserver

import "context"

type CleanupOwner interface {
	PlanCleanup(context.Context, string) (CleanupPlan, error)
	ExecuteCleanup(context.Context, CleanupExecution) error
}

type CleanupPlan struct {
	Kind      string
	Revision  uint64
	VolumeIDs []string
	Impacts   []CleanupImpact
}

type CleanupImpact struct {
	Resource     Resource
	Effect       string
	Irreversible bool
	Blocker      bool
}

type CleanupExecution struct {
	Kind               string
	Revision           uint64
	ConfirmedVolumeIDs []string
}
