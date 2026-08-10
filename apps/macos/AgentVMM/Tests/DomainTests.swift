import Testing
import GRPCCore
import Foundation
@testable import AgentVMM

@Test func healthOrderingKeepsUnavailableWorst() {
    #expect(HealthLevel.unavailable < .degraded)
    #expect(HealthLevel.degraded < .healthy)
}

@Test func rpcErrorsExposeServerReason() {
    let error = RPCError(code: .failedPrecondition, message: "verify bundle: invalid manifest signature")
    #expect(hostErrorMessage(error) == "verify bundle: invalid manifest signature")
}

@Test func transportErrorsUseActionableMessage() {
    let error = RPCError(code: .unavailable, message: "transport: Error while dialing: VM is not available")
    #expect(hostErrorMessage(error) == "Agent VMM is temporarily unavailable. Try again.")
}

@Test func environmentUsesAgentLanguage() {
    let environment = AgentEnvironmentModel(
        id: "a", name: "Research", namespaceID: "hidden", health: .healthy, usage: nil, workloads: [], volumes: [],
        limits: EnvironmentLimits(cpuMillis: 1_000, memoryBytes: 1_073_741_824, pids: 128, diskBytes: 10_737_418_240),
        egressSummary: "Blocked by default", allowsPublicEgress: false, policyRevision: 1, origin: .local, allocationID: nil
    )
    #expect(environment.name == "Research")
    #expect(environment.egressSummary == "Blocked by default")
}

@Test func usageCountersBecomePerSecondRates() {
    let first = UsageSnapshot(
        cpuNanos: 1_000_000_000, memoryBytes: 100, pids: 1,
        readBytes: 1_000, writeBytes: 500, networkRXBytes: 2_000, networkTXBytes: 800,
        diskBytes: 0, sampledAt: Date(timeIntervalSinceReferenceDate: 10), isStale: false
    )
    let second = UsageSnapshot(
        cpuNanos: 2_000_000_000, memoryBytes: 200, pids: 1,
        readBytes: 5_000, writeBytes: 1_500, networkRXBytes: 8_000, networkTXBytes: 2_800,
        diskBytes: 0, sampledAt: Date(timeIntervalSinceReferenceDate: 12), isStale: false
    )
    #expect(second.rate(after: first, for: \UsageSnapshot.cpuNanos) == 500_000_000)
    #expect(second.rate(after: first, for: \UsageSnapshot.readBytes) == 2_000)
    #expect(second.rate(after: first, for: \UsageSnapshot.networkRXBytes) == 3_000)
    #expect(first.rate(after: second, for: \UsageSnapshot.cpuNanos) == nil)
    let stale = UsageSnapshot(
        cpuNanos: 9_000_000_000, memoryBytes: 0, pids: 0,
        readBytes: 9_000, writeBytes: 9_000, networkRXBytes: 9_000, networkTXBytes: 9_000,
        diskBytes: 0, sampledAt: Date(timeIntervalSinceReferenceDate: 14), isStale: true
    )
    #expect(stale.rate(after: second, for: \UsageSnapshot.networkRXBytes) == nil)
    #expect(second.rate(after: stale, for: \UsageSnapshot.networkRXBytes) == nil)
}

@MainActor
@Test func eventHistoryResetsOnlyWhenGuestBootChanges() {
    let model = AppModel()
    let first = ActivityEvent(id: 1, type: "created", environmentID: "a", resourceID: "one", bootID: "guest-a", timestamp: .now)
    let second = ActivityEvent(id: 2, type: "started", environmentID: "a", resourceID: "one", bootID: "guest-a", timestamp: .now)
    let rebooted = ActivityEvent(id: 1, type: "created", environmentID: "a", resourceID: "two", bootID: "guest-b", timestamp: .now)
    model.record(first)
    model.record(second)
    #expect(model.events == [first, second])
    let gap = ActivityEvent(id: 4, type: "updated", environmentID: "a", resourceID: "one", bootID: "guest-a", timestamp: .now)
    model.record(gap)
    #expect(model.events == [gap])
    model.record(rebooted)
    #expect(model.events == [rebooted])
}
