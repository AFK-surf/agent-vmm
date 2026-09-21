import Testing
import GRPCCore
import Foundation
@testable import AgentVMM

@Test func healthOrderingKeepsUnavailableWorst() {
    #expect(HealthLevel.unavailable < .degraded)
    #expect(HealthLevel.degraded < .healthy)
}

@Test func ansiParserKeepsSGRTextAndDropsControlSequences() {
    let input = "\u{001B}[32mgreen\u{001B}[0m \u{001B}]8;;https://example.com\u{0007}link\u{001B}]8;;\u{0007} \u{001B}[2Jdone"
    #expect(ANSIParser.plain(input) == "green link done")
}

@Test func ansiParserCarriesStyleAcrossRecords() {
    var parser = ANSIParser()
    _ = parser.consume("\u{001B}[3")
    let continued = parser.consume("1mred\u{001B}[0m")
    #expect(String(continued.characters) == "red")
}

@Test func ansiParserBoundsUnterminatedControlSequences() {
    var parser = ANSIParser()
    for _ in 0..<20 {
        _ = parser.consume("\u{001B}[" + String(repeating: "1", count: 1_024))
        _ = parser.consume("\u{001B}]" + String(repeating: "x", count: 1_024))
    }
    #expect(String(parser.consume("visible").characters) == "visible")
}

@Test @MainActor func completedLogStreamIsNotResubscribed() async {
    var subscriptions = 0
    var status = ""
    await followLogStream(
        subscribe: { _ in
            subscriptions += 1
            return AsyncThrowingStream { $0.finish() }
        },
        onStatus: { status = $0 },
        onRecords: { _ in }
    )
    #expect(subscriptions == 1)
    #expect(status == "Ended")
}

@Test @MainActor func logHistoryIsDeliveredInBatches() async {
    var batches: [[LogEntry]] = []
    await followLogStream(
        subscribe: { _ in
            AsyncThrowingStream { continuation in
                for index in 0..<250 {
                    continuation.yield(LogEntry(id: String(index), timestamp: .now, stream: "stdout", text: "line", truncated: false, droppedRecords: 0))
                }
                continuation.finish()
            }
        },
        onStatus: { _ in },
        onRecords: { batches.append($0) }
    )
    #expect(batches.flatMap { $0 }.count == 250)
    #expect(batches.count == 3)
}

@Test func rpcErrorsExposeServerReason() {
    let error = RPCError(code: .failedPrecondition, message: "verify bundle: invalid manifest signature")
    #expect(hostErrorMessage(error) == "verify bundle: invalid manifest signature")
}

@Test func transportErrorsUseActionableMessage() {
    let error = RPCError(code: .unavailable, message: "transport: Error while dialing: VM is not available")
    #expect(hostErrorMessage(error) == "Agent VMM is temporarily unavailable. Try again.")
}

@Test func vmMutationFenceAcknowledgesWholeVMDespitePartialSummary() throws {
    var summary = Agentvmm_Host_V1_GetVMImpactSummaryResponse()
    summary.complete = false
    summary.overflow = true
    summary.issue = "known workload summary exceeded the bound"
    summary.vm.state = .running
    summary.vm.bootID = "boot-a"
    var known = Agentvmm_Host_V1_ResourceReference()
    known.kind = "environment"
    known.environmentID = "env-a"
    known.generation = 7
    summary.knownWorkloads = [known]

    let fence = try GRPCHostClient.vmMutationFence(summary)
    #expect(fence.state == .running)
    #expect(fence.bootID == "boot-a")
    #expect(fence.observedWorkloads == [known])
    #expect(fence.wholeVmImpactAcknowledged)
}

@Test func environmentUsesAgentLanguage() {
    let environment = AgentEnvironmentModel(
        id: "a", name: "Research", namespaceID: "hidden", health: .healthy, usage: nil, workloads: [], volumes: [],
        limits: EnvironmentLimits(cpuMillis: 1_000, memoryBytes: 1_073_741_824, pids: 128, diskBytes: 10_737_418_240),
        egressSummary: "Blocked by default", allowsPublicEgress: false, policyRevision: 1, origin: .local, allocationID: nil
    )
    #expect(environment.name == "Research")
    #expect(environment.egressSummary == "Blocked by default")
    #expect(!environment.hasNoRetainedData)

    let emptyUsage = UsageSnapshot(
        cpuNanos: 0, memoryBytes: 0, pids: 0, readBytes: 0, writeBytes: 0,
        networkRXBytes: 0, networkTXBytes: 0, diskBytes: 0, sampledAt: .now, isStale: false
    )
    let empty = AgentEnvironmentModel(
        id: "empty", name: "Empty", namespaceID: "empty", health: .healthy, usage: emptyUsage, workloads: [], volumes: [],
        limits: environment.limits, egressSummary: environment.egressSummary, allowsPublicEgress: false,
        policyRevision: 1, origin: .local, allocationID: nil
    )
    #expect(empty.hasNoRetainedData)
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

@Test func imageRemovalRequiresFreshUnusedIdentity() {
    let unused = EnvironmentImageModel(reference: "docker.io/library/test:latest", digest: "sha256:test", logicalBytes: 42, platforms: [], usedByContainerIDs: [], generationID: "generation")
    #expect(imageRemovalAvailability(for: unused, inventory: .available, isRemoving: false) == .allowed)
    #expect(imageRemovalAvailability(for: unused, inventory: .stale, isRemoving: false) == .inventoryUnavailable)
    #expect(imageRemovalAvailability(for: unused, inventory: .available, isRemoving: true) == .inProgress)
    let used = EnvironmentImageModel(reference: unused.reference, digest: unused.digest, logicalBytes: unused.logicalBytes, platforms: [], usedByContainerIDs: ["one", "two"], generationID: unused.generationID)
    #expect(imageRemovalAvailability(for: used, inventory: .available, isRemoving: false) == .inUse(2))
}

@Test func remoteConnectionStatusHonorsFreshTransportStateAndTerminalFailures() {
    let now = Date(timeIntervalSinceReferenceDate: 1_000)
    #expect(RemoteConnectionStatus.derive(
        connected: false, connectorPhase: .retryWait, observedAt: now.addingTimeInterval(-10), now: now
    ) == .connecting)
    #expect(RemoteConnectionStatus.derive(
        connected: false, connectorPhase: .retryWait, observedAt: now.addingTimeInterval(-40), now: now
    ) == .offline)
    #expect(RemoteConnectionStatus.derive(
        connected: false, connectorPhase: .authFailed, observedAt: now, now: now
    ) == .offline)
    #expect(RemoteConnectionStatus.derive(
        connected: true, connectorPhase: .connected, observedAt: now, now: now
    ) == .connected)
    #expect(RemoteConnectionStatus.derive(
        connected: true, connectorPhase: .connected, observedAt: now.addingTimeInterval(-21), now: now
    ) == .offline)
}

@Test func lifecycleHealthRequiresHostService() {
    let healthy = LifecycleStatus(appInstalled: true, hostInstalled: true, hostLoaded: true)
    let stopped = LifecycleStatus(appInstalled: true, hostInstalled: true, hostLoaded: false)
    #expect(healthy.coreHealthy)
    #expect(!stopped.coreHealthy)
}

@Test func MCPClientBindingsAreOpaqueAndIndependent() {
    let first = MCPClientConfiguration.newBinding()
    let second = MCPClientConfiguration.newBinding()
    #expect(first.count == 64)
    #expect(first.allSatisfy { "0123456789abcdef".contains($0) })
    #expect(first != second)
}

@Test func applianceRemovalPreservesServerBlockers() {
    let plan = ApplianceRemovePlanModel(
        revision: 7,
        impacts: [
            ApplianceImpactModel(resourceType: "allocation", resourceID: "retained", effect: "discard-before-remove", irreversible: true, blocker: true),
            ApplianceImpactModel(resourceType: "encrypted-volume", resourceID: "volume", effect: "delete", irreversible: true, blocker: false)
        ]
    )
    #expect(plan.blockers.map(\.resourceID) == ["retained"])
    #expect(Set(plan.requiredConfirmations) == ["retained", "volume"])
}

@Test func mcpEndpointAvailabilityIsSeparateFromApplianceEnabledState() {
    let grant = ApplianceGrantModel.bundledMCPDefault
    let unavailable = ApplianceStatusModel(
        id: "mcp", productID: "mcp", revision: 1, artifactID: "artifact", state: "Running",
        channelState: "Connected", enabled: true, healthy: true, healthMessage: nil, grant: grant,
        frontends: [ApplianceFrontendModel(name: BundledMCPAppliance.frontendName, transport: "http", endpoint: "", available: false)]
    )
    #expect(!unavailable.endpointAvailable)
    #expect(unavailable.endpointStatus == "Starting or unavailable")

    let ready = ApplianceStatusModel(
        id: "mcp", productID: "mcp", revision: 1, artifactID: "artifact", state: "Running",
        channelState: "Connected", enabled: true, healthy: true, healthMessage: nil, grant: grant,
        frontends: [ApplianceFrontendModel(name: BundledMCPAppliance.frontendName, transport: "http", endpoint: "http://127.0.0.1:1/mcp", available: true)]
    )
    #expect(ready.endpointAvailable)
    #expect(ready.endpointStatus == "Available on this Mac")
}


@Test func remoteCapacityDraftPreservesUneditedPolicyExactly() {
    let policy = RemotePoolSettings(
        perEnvironmentPIDs: 222,
        perEnvironmentDiskBytes: 4_567_890_123,
        allowsPublicEgress: false,
        stopGraceSeconds: 13
    )
    let draft = RemotePoolDraft(policy)
    #expect(draft.settings == policy)
    #expect(draft.isValid)
}

@Test func remoteCapacityDefaultsFitTheDefaultHostAndDenyPublicEgress() {
    let defaults = RemotePoolSettings.defaults
    #expect(defaults.perEnvironmentPIDs == 256)
    #expect(defaults.perEnvironmentDiskBytes <= 8 << 30)
    #expect(!defaults.allowsPublicEgress)
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

@MainActor
@Test func healthSnapshotRefreshPreservesErrorsAndRejectsOlderSamples() {
    let model = AppModel()
    model.errorMessage = "Agent setup failed."
    let current = SystemSnapshotModel(
        vmState: "Running", bootID: "guest-a", health: .healthy, components: [], usage: nil,
        environments: [], remoteRegistrations: [], collectedAt: Date(timeIntervalSinceReferenceDate: 20)
    )
    model.applySnapshot(current)
    model.applySnapshot(SystemSnapshotModel(
        vmState: "Running", bootID: "guest-a", health: .unavailable, components: [], usage: nil,
        environments: [], remoteRegistrations: [], collectedAt: Date(timeIntervalSinceReferenceDate: 10)
    ))
    #expect(model.snapshot == current)
    #expect(model.errorMessage == "Agent setup failed.")
}

@Test func configurationCommandRunnerHandlesLargeOutputAndTimeout() async throws {
    let configuration = MCPClientConfiguration()
    let output = try await configuration.run(
        URL(filePath: "/bin/sh"),
        arguments: ["-c", "/usr/bin/yes x | /usr/bin/head -c 131072"],
        timeout: .seconds(3)
    )
    #expect(output.count == 131_072)

    do {
        _ = try await configuration.run(
            URL(filePath: "/bin/sh"), arguments: ["-c", "sleep 5"], timeout: .milliseconds(100)
        )
        Issue.record("Expected the command to time out")
    } catch MCPClientConfiguration.ConfigurationError.commandTimedOut {
    } catch {
        Issue.record("Unexpected error: \(error)")
    }
}
