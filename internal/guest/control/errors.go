package control

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/AFK-surf/agent-vmm/internal/resourcev2"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	return detailedErrorMetadata(codes.ResourceExhausted, "RESOURCE_CAPACITY_EXHAUSTED", message,
		"guest_capacity", string(dimension), metadata)
}

func detailedError(code codes.Code, reason, message, resourceType, resourceName string) error {
	return detailedErrorMetadata(code, reason, message, resourceType, resourceName, nil)
}

func detailedErrorMetadata(code codes.Code, reason, message, resourceType, resourceName string, metadata map[string]string) error {
	value := status.New(code, message)
	withDetails, err := value.WithDetails(&errdetails.ErrorInfo{Reason: reason, Domain: "agent-vmm", Metadata: metadata})
	if err != nil {
		return value.Err()
	}
	if resourceType == "" && resourceName == "" {
		return withDetails.Err()
	}
	withDetails, err = withDetails.WithDetails(&errdetails.ResourceInfo{ResourceType: resourceType, ResourceName: resourceName})
	if err != nil {
		return value.Err()
	}
	return withDetails.Err()
}

func imageError(code codes.Code, reason, message, reference string) error {
	return detailedError(code, reason, message, "image", reference)
}

func imageErrorMetadata(code codes.Code, reason, message, reference, failureClass string) error {
	return detailedErrorMetadata(code, reason, message, "image", reference, map[string]string{"failure_class": failureClass})
}

func failedPrecondition(message string) error {
	return fmt.Errorf("%w: %s", errPrecondition, message)
}
