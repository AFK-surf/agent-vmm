package control

import (
	"errors"
	"fmt"

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
