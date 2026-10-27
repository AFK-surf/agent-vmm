package hostv1

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func InvalidField(field, description string) error {
	value := status.New(codes.InvalidArgument, "request validation failed")
	withDetails, err := value.WithDetails(&errdetails.BadRequest{FieldViolations: []*errdetails.BadRequest_FieldViolation{{Field: field, Description: description}}})
	if err != nil {
		return value.Err()
	}
	return withDetails.Err()
}
