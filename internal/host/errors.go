package host

import (
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
)

func invalidField(field, description string) error {
	value := status.New(codes.InvalidArgument, "request validation failed")
	withDetails, err := value.WithDetails(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: field, Description: description}}})
	if err != nil {
		return value.Err()
	}
	return withDetails.Err()
}

func resourceError(code codes.Code, reason, kind, name, message string) error {
	value := status.New(code, message)
	withDetails, err := value.WithDetails(
		&errdetails.ErrorInfo{Reason: reason, Domain: "agentvmm.afk.surf"},
		&errdetails.ResourceInfo{ResourceType: kind, ResourceName: name},
	)
	if err != nil {
		return value.Err()
	}
	return withDetails.Err()
}

func retryable(reason, message string, delay time.Duration) error {
	value := status.New(codes.Unavailable, message)
	withDetails, err := value.WithDetails(
		&errdetails.ErrorInfo{Reason: reason, Domain: "agentvmm.afk.surf"},
		&errdetails.RetryInfo{RetryDelay: durationpb.New(delay)},
	)
	if err != nil {
		return value.Err()
	}
	return withDetails.Err()
}
