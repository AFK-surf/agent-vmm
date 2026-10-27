enum VMState: Sendable { case loading, unavailable, stopped, starting, running, stopping, failed }
enum OperationState: Sendable { case unavailable, running, succeeded, failed, interrupted }
enum ApplianceState: Sendable { case unavailable, stopped, starting, running, stopping, failed, sealed }
enum AllocationState: Sendable { case unavailable, ready, draining, retained, discarded }
enum PairingState: String, Sendable {
    case invited, handshaking, awaitingConfirm = "awaiting_confirm", readyToCommit = "ready_to_commit"
    case committed, rejected, expired, unavailable
}
