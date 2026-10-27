extension VMState {
    var label: String {
        switch self {
        case .loading: "Loading"
        case .unavailable: "Unavailable"
        case .stopped: "Stopped"
        case .starting: "Starting"
        case .running: "Running"
        case .stopping: "Stopping"
        case .failed: "Failed"
        }
    }
}
extension OperationState {
    var label: String {
        switch self {
        case .unavailable: "Unavailable"
        case .running: "Running"
        case .succeeded: "Succeeded"
        case .failed: "Failed"
        case .interrupted: "Interrupted"
        }
    }
}
extension ApplianceState {
    var label: String {
        switch self {
        case .unavailable: "Unspecified"
        case .stopped: "Stopped"
        case .starting: "Starting"
        case .running: "Running"
        case .stopping: "Stopping"
        case .failed: "Failed"
        case .sealed: "Sealed"
        }
    }
}
extension AllocationState {
    var label: String {
        switch self {
        case .unavailable: "Unspecified"
        case .ready: "Ready"
        case .draining: "Draining"
        case .retained: "Retained"
        case .discarded: "Discarded"
        }
    }
}
extension PairingState {
    var label: String { rawValue }
}
