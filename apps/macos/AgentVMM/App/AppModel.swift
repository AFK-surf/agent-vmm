import Foundation
import Observation

@MainActor
@Observable
final class AppModel {
    enum Section: String, CaseIterable, Identifiable {
        case system = "Overview"
        case access = "Agent Access"
        case environments = "Environments"
        case diagnostics = "Activity"
        var id: Self { self }
    }

    enum AccessTab: String, CaseIterable, Identifiable { case local = "Local Agents", remote = "Remote Controller"; var id: Self { self } }

    enum EnvironmentSection: String, CaseIterable, Identifiable {
        case overview = "Overview"
        case containers = "Containers"
        case storage = "Storage"
        case logs = "Logs"
        case network = "Network"
        case advanced = "Advanced"
        var id: Self { self }
    }

    enum VMAction {
        case starting
        case stopping
        case forceStopping
    }

    enum EnrollmentStage: String {
        case creating = "Creating registration"
        case storingCredential = "Storing credential"
        case connecting = "Connecting"
    }

    enum IntegrationStage: String {
        case starting = "Starting agent setup…"
        case writingConfiguration = "Writing Codex configuration…"
        case finalizing = "Finishing agent setup…"
    }

    var section: Section? = .system
    var selectedEnvironmentID: String?
    var environmentSection: EnvironmentSection = .overview
    var accessTab: AccessTab = .local
    var snapshot = SystemSnapshotModel.loading
    var integrations: [AgentIntegrationModel] = []
    var sessions: [AgentSessionModel] = []
    var events: [ActivityEvent] = []
    var metrics: [UsageSnapshot] = []
    var errorMessage: String?
    var isLoading = true
    var latestOperation: OperationModel?
    var gracefulStopFailed = false
    var vmAction: VMAction?
    var enrollmentStage: EnrollmentStage?
    var integrationStage: IntegrationStage?
    var lifecycleStatus: LifecycleStatus?
    var lifecycleOperationInProgress = false

    var client: (any HostServing)?
    var observationTask: Task<Void, Never>?
    private var metricsTask: Task<Void, Never>?
    private var snapshotRefreshTask: Task<Void, Never>?
    private var eventBootID = ""
    let integrationConfiguration = AgentIntegrationConfiguration()

    init(client: (any HostServing)? = nil) {
        self.client = client
    }

    func start(socketPath: String = AppModel.defaultSocketPath) async {
        guard observationTask == nil else { return }
        do {
            let client: any HostServing
            if let existing = self.client { client = existing }
            else { client = try GRPCHostClient(socketPath: socketPath) }
            self.client = client
            try await refresh()
            if let operationID = UserDefaults.standard.string(forKey: "latestOperationID") {
                latestOperation = try? await client.getOperation(id: operationID)
            }
            observationTask = Task { [weak self] in await self?.observe(client: client) }
            metricsTask = Task { [weak self] in await self?.observeMetrics(client: client) }
            snapshotRefreshTask = Task { [weak self] in await self?.observeSnapshots(client: client) }
        } catch {
            isLoading = false
            errorMessage = hostErrorMessage(error)
        }
        await refreshLifecycleStatus()
    }

    func refresh() async throws {
        guard let client else { throw HostClientError.disconnected }
        async let nextSnapshot = client.snapshot()
        async let nextIntegrations = client.listAgentIntegrations()
        async let nextSessions = client.listAgentSessions(integrationID: nil, includeTerminal: false)
        applySnapshot(try await nextSnapshot)
        integrations = try await nextIntegrations
        sessions = try await nextSessions
        isLoading = false
        errorMessage = nil
    }

    func applySnapshot(_ value: SystemSnapshotModel) {
        guard value.collectedAt >= snapshot.collectedAt else { return }
        let previousBootID = snapshot.bootID
        snapshot = value
        if !previousBootID.isEmpty && value.bootID != previousBootID {
            events.removeAll()
            metrics.removeAll()
            eventBootID = ""
        }
    }

    func stop(environmentID: String, containerID: String) async {
        do { try await client?.stopContainer(environmentID: environmentID, containerID: containerID); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func startVM() async {
        guard vmAction == nil else { return }
        vmAction = .starting
        errorMessage = nil
        defer { vmAction = nil }
        do { remember(try await client?.startVM()); gracefulStopFailed = false; try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func stopVM(force: Bool) async {
        guard vmAction == nil else { return }
        vmAction = force ? .forceStopping : .stopping
        errorMessage = nil
        defer { vmAction = nil }
        do { remember(try await client?.stopVM(force: force)); gracefulStopFailed = false; try await refresh() }
        catch {
            if !force { gracefulStopFailed = true }
            errorMessage = hostErrorMessage(error)
        }
    }

    func continueWorkload(environmentID: String, containerID: String) async {
        do { try await client?.continueContainer(environmentID: environmentID, containerID: containerID); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func signal(environmentID: String, containerID: String, signal: ContainerSignal) async {
        do { try await client?.signalContainer(environmentID: environmentID, containerID: containerID, signal: signal); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func deleteWorkload(environmentID: String, containerID: String, expectedGenerationID: String) async {
        do {
            try await client?.deleteContainer(environmentID: environmentID, containerID: containerID, expectedGenerationID: expectedGenerationID)
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func deleteImage(environmentID: String, image: EnvironmentImageModel) async throws {
        guard let client else { throw HostClientError.disconnected }
        try await client.deleteImage(
            environmentID: environmentID,
            reference: image.reference,
            expectedGenerationID: image.generationID,
            expectedDigest: image.digest
        )
    }

    func updatePolicy(environmentID: String, limits: EnvironmentLimits, allowsPublicEgress: Bool, expectedRevision: UInt64) async {
        do {
            try await client?.updatePolicy(environmentID: environmentID, limits: limits, allowsPublicEgress: allowsPublicEgress, expectedRevision: expectedRevision)
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func previewCleanup(environmentID: String, mode: CleanupMode) async throws -> CleanupPreview {
        guard let client else { throw HostClientError.disconnected }
        return try await client.cleanupPreview(environmentID: environmentID, mode: mode)
    }

    @discardableResult
    func executeCleanup(_ preview: CleanupPreview) async -> Bool {
        do {
            guard let operation = try await client?.executeCleanup(preview) else {
                throw HostClientError.disconnected
            }
            remember(operation)
            try await refresh()
            guard operation.state == "Succeeded" else {
                errorMessage = operation.message ?? operation.reason ?? "The environment could not be cleaned up."
                return false
            }
            return true
        } catch {
            errorMessage = hostErrorMessage(error)
            return false
        }
    }

    func runDiagnostics() async {
        do { remember(try await client?.runDiagnostics(environmentID: selectedEnvironmentID)) }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func supportPreview() async throws -> [SupportItem] {
        guard let client else { throw HostClientError.disconnected }
        return try await client.supportPreview(environmentID: selectedEnvironmentID)
    }

    func exportSupportBundle(sensitiveItems: [String]) async {
        do { remember(try await client?.exportSupportBundle(environmentID: selectedEnvironmentID, sensitiveItems: sensitiveItems)) }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func enrollRemote(endpoint: String, displayName: String, token: String, pool: RemotePoolSettings) async -> Bool {
        guard let client else { errorMessage = HostClientError.disconnected.localizedDescription; return false }
        var pendingRegistration: RemoteRegistrationModel?
        enrollmentStage = .creating
        defer { enrollmentStage = nil }
        do {
            let registration = try await client.beginRemoteEnrollment(endpoint: endpoint, displayName: displayName, pool: pool)
            pendingRegistration = registration
            enrollmentStage = .storingCredential
            try await runEnrollment(endpoint: endpoint, registrationID: registration.id, token: token)
            enrollmentStage = .connecting
            _ = try await client.completeRemoteEnrollment(registrationID: registration.id, expectedRevision: registration.revision)
            try await refresh()
            return true
        } catch {
            if let registration = pendingRegistration {
                _ = try? await client.failRemoteEnrollment(registrationID: registration.id, expectedRevision: registration.revision, code: "ENROLLMENT_FAILED", message: "Controller enrollment failed. Retry setup.")
            }
            errorMessage = hostErrorMessage(error)
            try? await refresh()
            return false
        }
    }

    func setRemoteAdmission(_ state: RemoteAdmissionState) async {
        guard let registration = snapshot.remoteRegistration else { return }
        do {
            _ = try await client?.setRemoteAdmission(state, expectedRevision: registration.revision)
            try await refresh()
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func updateRemotePool(_ pool: RemotePoolSettings) async -> Bool {
        guard let registration = snapshot.remoteRegistration else { return false }
        do { _ = try await client?.updateRemotePool(pool, expectedRevision: registration.revision); try await refresh() }
        catch { errorMessage = hostErrorMessage(error); return false }
        return true
    }

    func retryRemoteEnrollment(token: String) async -> Bool {
        guard let client, let registration = snapshot.remoteRegistration else { return false }
        var pendingRegistration: RemoteRegistrationModel?
        enrollmentStage = .creating
        defer { enrollmentStage = nil }
        do {
            let pending = try await client.retryRemoteEnrollment(registrationID: registration.id, expectedRevision: registration.revision)
            pendingRegistration = pending
            enrollmentStage = .storingCredential
            try await runEnrollment(endpoint: pending.endpoint, registrationID: pending.id, token: token)
            enrollmentStage = .connecting
            _ = try await client.completeRemoteEnrollment(registrationID: pending.id, expectedRevision: pending.revision)
            try await refresh()
            return true
        } catch {
            if let pending = pendingRegistration {
                _ = try? await client.failRemoteEnrollment(registrationID: pending.id, expectedRevision: pending.revision, code: "ENROLLMENT_FAILED", message: "Controller enrollment failed. Retry setup.")
            }
            errorMessage = hostErrorMessage(error)
            try? await refresh()
            return false
        }
    }

    func removeIncompleteRemoteRegistration() async {
        guard let client, let registration = snapshot.remoteRegistration else { return }
        do {
            try await revokeCredential(registrationID: registration.id)
            try await client.removeIncompleteRemoteRegistration(registrationID: registration.id, expectedRevision: registration.revision)
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func revokeRemoteAccess() async -> Bool {
        guard let client, let registration = snapshot.remoteRegistration else { return true }
        do {
            let revoking = try await client.beginRemoteRevocation(registrationID: registration.id, expectedRevision: registration.revision)
            try await revokeCredential(registrationID: registration.id)
            _ = try await client.completeRemoteRevocation(registrationID: registration.id, expectedRevision: revoking.revision)
            try await refresh()
            return true
        } catch {
            errorMessage = hostErrorMessage(error)
            try? await refresh()
            return false
        }
    }

    func finishRemoteRevocation() async {
        guard let client, let registration = snapshot.remoteRegistration else { return }
        do {
            try await revokeCredential(registrationID: registration.id)
            _ = try await client.completeRemoteRevocation(registrationID: registration.id, expectedRevision: registration.revision)
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    private func runEnrollment(endpoint: String, registrationID: String, token: String) async throws {
        guard let host = URL(string: "https://\(endpoint)")?.host, !host.isEmpty,
              let helper = BundledHelper.executable(named: "agent-vmm-connect") else {
            throw HostClientError.invalidResponse("invalid controller endpoint or missing connector")
        }
        let socketPath = AppModel.remoteComputeSocketPath
        try await Task.detached {
            let process = Process()
            let input = Pipe()
            process.executableURL = helper
            process.arguments = ["--enroll", "--endpoint", endpoint, "--endpoint-identity", host, "--registration", registrationID,
                                 "--remote-compute-socket", socketPath]
            process.standardInput = input
            process.standardOutput = FileHandle.nullDevice
            process.standardError = FileHandle.nullDevice
            try process.run()
            input.fileHandleForWriting.write(Data(token.utf8))
            try input.fileHandleForWriting.close()
            process.waitUntilExit()
            guard process.terminationStatus == 0 else { throw HostClientError.invalidResponse("controller enrollment failed") }
        }.value
    }

    private func revokeCredential(registrationID: String) async throws {
        guard let helper = BundledHelper.executable(named: "agent-vmm-connect") else {
            throw HostClientError.invalidResponse("missing connector")
        }
        try await Task.detached {
            let process = Process()
            process.executableURL = helper
            process.arguments = ["--revoke-credential", "--registration", registrationID]
            process.standardOutput = FileHandle.nullDevice
            process.standardError = FileHandle.nullDevice
            try process.run()
            process.waitUntilExit()
            guard process.terminationStatus == 0 else { throw HostClientError.invalidResponse("credential revocation failed") }
        }.value
    }

    private func remember(_ operation: OperationModel?) {
        guard let operation else { return }
        latestOperation = operation
        UserDefaults.standard.set(operation.id, forKey: "latestOperationID")
    }

    private func observeMetrics(client: any HostServing) async {
        while !Task.isCancelled {
            do {
                for try await sample in client.metrics(environmentID: nil) {
                    metrics.append(sample)
                    if metrics.count > 120 { metrics.removeFirst(metrics.count - 120) }
                }
            } catch is CancellationError {
                return
            } catch {
                try? await Task.sleep(for: .seconds(1))
            }
        }
    }

    private func observeSnapshots(client: any HostServing) async {
        while !Task.isCancelled {
            do {
                try await Task.sleep(for: .seconds(5))
                applySnapshot(try await client.snapshot())
            } catch is CancellationError {
                return
            } catch {
                // The event observer owns connection-level error reporting and retry backoff.
            }
        }
    }

    private func observe(client: any HostServing) async {
        var delay = Duration.milliseconds(250)
        while !Task.isCancelled {
            do {
                let sequence = events.last?.id ?? 0
                for try await event in client.events(after: sequence) {
                    record(event)
                    try await refresh()
                }
                throw HostClientError.disconnected
            } catch is CancellationError {
                return
            } catch {
                try? await Task.sleep(for: delay)
                delay = min(delay * 2, .seconds(8))
                do {
                    try await refresh()
                    delay = .milliseconds(250)
                } catch {}
            }
        }
    }

    func record(_ event: ActivityEvent) {
        if !eventBootID.isEmpty && (event.bootID != eventBootID || event.id != (events.last?.id ?? 0) + 1) {
            events.removeAll()
        }
        eventBootID = event.bootID
        events.append(event)
        if events.count > 500 { events.removeFirst(events.count - 500) }
    }

    static var defaultSocketPath: String {
        FileManager.default.homeDirectoryForCurrentUser
            .appending(path: "Library/Application Support/AgentVMM/operator.sock").path
    }

    static var remoteComputeSocketPath: String {
        FileManager.default.homeDirectoryForCurrentUser
            .appending(path: "Library/Application Support/AgentVMM/remote-compute.sock").path
    }
}
