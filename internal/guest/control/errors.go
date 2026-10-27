package control

import (
	"errors"
	"fmt"
	"strconv"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/grpc/codes"
)

var (
	errResourceExhausted = errors.New("resource exhausted")
	errPrecondition      = errors.New("failed precondition")
)

type runtimeStageError struct {
	stage string
	err   error
}

func (err runtimeStageError) Error() string { return err.err.Error() }
func (err runtimeStageError) Unwrap() error { return err.err }

func atRuntimeStage(stage string, err error) error {
	if err == nil {
		return nil
	}
	var existing runtimeStageError
	if errors.As(err, &existing) {
		return err
	}
	return runtimeStageError{stage: stage, err: err}
}

func resourceExhausted(message string) error {
	return fmt.Errorf("%w: %s", errResourceExhausted, message)
}

// resourceCapacityExhausted is emitted only by the Guest owner that observes
// the physical capacity boundary. The Host connector forwards this finite
// dimension; callers must not infer it from error prose.
func resourceCapacityExhausted(dimension resourcev2.CapacityDimension, stage, message string) error {
	return resourceCapacityExhaustedMetadata(dimension, stage, message, nil)
}

func resourceCapacityExhaustedWithBytes(dimension resourcev2.CapacityDimension, stage, message string, available, required uint64) error {
	return resourceCapacityExhaustedMetadata(dimension, stage, message, map[string]string{
		"available_bytes": strconv.FormatUint(available, 10),
		"required_bytes":  strconv.FormatUint(required, 10),
	})
}

func resourceCapacityExhaustedMetadata(dimension resourcev2.CapacityDimension, stage, message string, values map[string]string) error {
	metadata := map[string]string{"resource_dimension": string(dimension), "stage": stage}
	for key, value := range values {
		metadata[key] = value
	}
	return hostv1.NewError(codes.ResourceExhausted, hostv1.ErrorReason_ERROR_REASON_RESOURCE_CAPACITY_EXHAUSTED, message, hostv1.WithResource("guest_capacity", string(dimension)), hostv1.WithMetadata(metadata))
}

func failedPrecondition(message string) error {
	return fmt.Errorf("%w: %s", errPrecondition, message)
}
