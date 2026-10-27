package hostv1

import (
	"strings"
	"time"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/protoadapt"
	"google.golang.org/protobuf/types/known/durationpb"
)

const ErrorDomain = "agent-vmm"

type errorOptions struct {
	metadata map[string]string
	retry    *errdetails.RetryInfo
	resource *errdetails.ResourceInfo
}
type ErrorOption func(*errorOptions)

func WithMetadata(metadata map[string]string) ErrorOption {
	return func(options *errorOptions) { options.metadata = metadata }
}
func WithRetry(delay time.Duration) ErrorOption {
	return func(options *errorOptions) { options.retry = &errdetails.RetryInfo{RetryDelay: durationpb.New(delay)} }
}
func WithResource(kind, name string) ErrorOption {
	return func(options *errorOptions) {
		options.resource = &errdetails.ResourceInfo{ResourceType: kind, ResourceName: name}
	}
}
func NewError(code codes.Code, reason ErrorReason, message string, opts ...ErrorOption) error {
	value := status.New(code, message)
	options := errorOptions{}
	for _, option := range opts {
		option(&options)
	}
	details := []protoadapt.MessageV1{&errdetails.ErrorInfo{Domain: ErrorDomain, Reason: strings.TrimPrefix(reason.String(), "ERROR_REASON_"), Metadata: options.metadata}}
	if options.retry != nil {
		details = append(details, options.retry)
	}
	if options.resource != nil {
		details = append(details, options.resource)
	}
	value, _ = value.WithDetails(details...)
	return value.Err()
}
func ErrorReasonOf(err error) (ErrorReason, bool) {
	reason, _, ok := ErrorInfoOf(err)
	return reason, ok
}

func ErrorInfoOf(err error) (ErrorReason, map[string]string, bool) {
	details := status.Convert(err).Details()
	for index := len(details) - 1; index >= 0; index-- {
		detail := details[index]
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != ErrorDomain {
			continue
		}
		value, ok := ErrorReason_value["ERROR_REASON_"+info.GetReason()]
		if ok && value != 0 {
			return ErrorReason(value), info.GetMetadata(), true
		}
	}
	return ErrorReason_ERROR_REASON_UNSPECIFIED, nil, false
}
