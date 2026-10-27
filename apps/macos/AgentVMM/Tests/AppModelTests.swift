import Foundation
import Testing
@testable import AgentVMM

@MainActor
private func eventually(_ condition: () -> Bool) async throws {
    let deadline = ContinuousClock.now.advanced(by: .seconds(3))
    while !condition() {
        try #require(ContinuousClock.now < deadline)
        try await Task.sleep(for: .milliseconds(10))
    }
}

@Test @MainActor func offlineStartupReconnectsWithoutAnotherStart() async throws {
    let host = FakeHost()
    host.state.withLock { $0.online = false }
    let model = AppModel(client: host)
    await model.start()
    defer { host.finishStreams(); model.observationTask?.cancel() }
    try await eventually { host.state.withLock { $0.snapshotCalls > 0 } }
    #expect(!model.isConnected)
    host.state.withLock { $0.online = true }
    try await eventually { model.isConnected }
    #expect(model.snapshot.bootID == "fake-boot")
    #expect(model.deviceIdentity?.id == "fake-device")
}

@Test @MainActor func eventBurstCoalescesRefreshes() async throws {
    let host = FakeHost()
    let model = AppModel(client: host)
    await model.start()
    defer { host.finishStreams(); model.observationTask?.cancel() }
    try await eventually { host.state.withLock { $0.events != nil } }
    let baseline = host.state.withLock { value in value.pauseSnapshots = true; return value.snapshotCalls }
    let refresh = Task { try await model.refresh() }
    try await eventually { host.state.withLock { $0.snapshotCalls > baseline } }
    for sequence in 1...100 {
        host.state.withLock { $0.events }?.yield(ActivityEvent(id: UInt64(sequence), type: "changed", environmentID: nil, resourceID: nil, bootID: "fake-boot", timestamp: .now))
    }
    try await eventually { model.events.count == 100 }
    host.resumeSnapshots()
    try await refresh.value
    try await eventually { host.state.withLock { $0.snapshotCalls >= baseline + 2 } }
    #expect(host.state.withLock { $0.snapshotCalls } <= baseline + 2)
}

@Test @MainActor func confirmedVMMutationKeepsDisplayedFence() async throws {
    let host = FakeHost()
    let model = AppModel(client: host)
    #expect(await model.prepareVMAction())
    #expect(model.vmImpactSummary?.overflow == true)
    host.state.withLock { $0.bootID = "replacement-boot" }
    await model.stopVM(force: false)
    #expect(host.state.withLock { $0.submittedFence?.bootID } == "fake-boot")
    #expect(model.errorMessage == "VM fence changed")
    #expect(model.vmImpactSummary?.vm.bootID == "replacement-boot")
}
