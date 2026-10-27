package guestconn

import hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"

func MutationAdmissionRejected(err error) bool {
	reason, ok := hostv1.ErrorReasonOf(err)
	return ok && (reason == hostv1.ErrorReason_ERROR_REASON_CONTAINER_CREATE_NOT_ADMITTED || reason == hostv1.ErrorReason_ERROR_REASON_CONTAINER_START_NOT_ADMITTED)
}
