import Foundation
import Observation

@MainActor
@Observable
final class AppModel {
    enum Section: String, CaseIterable, Identifiable {
        case system = "Overview"
        case environments = "Environments"
        case diagnostics = "Activity"
        case settings = "Settings"
        var id: Self { self }
    }

    enum VMAction {
        case starting
        case stopping
        case forceStopping
    }

    var section: Section? = .system
    var selectedEnvironmentID: String?
    var snapshot = SystemSnapshotModel.loading
    var events: [ActivityEvent] = []
    var metrics: [UsageSnapshot] = []
    var errorMessage: String?
    var isLoading = true
    var latestOperation: OperationModel?
    var gracefulStopFailed = false
    var vmAction: VMAction?

    private var client: (any HostServing)?
    private var observationTask: Task<Void, Never>?
    private var metricsTask: Task<Void, Never>?
    private var eventBootID = ""

    func start(socketPath: String = AppModel.defaultSocketPath) async {
        guard observationTask == nil else { return }
        do {
            let client = try GRPCHostClient(socketPath: socketPath)
            self.client = client
            try await refresh()
            if let operationID = UserDefaults.standard.string(forKey: "latestOperationID") {
                latestOperation = try? await client.getOperation(id: operationID)
            }
            observationTask = Task { [weak self] in await self?.observe(client: client) }
            metricsTask = Task { [weak self] in await self?.observeMetrics(client: client) }
        } catch {
            isLoading = false
            errorMessage = hostErrorMessage(error)
        }
    }

    func refresh() async throws {
        guard let client else { throw HostClientError.disconnected }
        let previousBootID = snapshot.bootID
        snapshot = try await client.snapshot()
        if !previousBootID.isEmpty && snapshot.bootID != previousBootID {
            events.removeAll()
            metrics.removeAll()
            eventBootID = ""
        }
        isLoading = false
        errorMessage = nil
    }

    func environment(id: String) -> AgentEnvironmentModel? {
        snapshot.environments.first { $0.id == id }
    }

    func logStream(environmentID: String, containerID: String, after cursor: String?) throws -> AsyncThrowingStream<LogEntry, Error> {
        guard let client else { throw HostClientError.disconnected }
        return client.logs(environmentID: environmentID, containerID: containerID, after: cursor)
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

    func executeCleanup(_ preview: CleanupPreview) async {
        do { remember(try await client?.executeCleanup(preview)); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
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

    func enrollRemote(endpoint: String, displayName: String, token: String, pool: RemotePoolSettings) async {
        guard let client else { errorMessage = HostClientError.disconnected.localizedDescription; return }
        do {
            let registration = try await client.createRemoteRegistration(endpoint: endpoint, displayName: displayName, pool: pool)
            try await runEnrollment(endpoint: endpoint, registrationID: registration.id, token: token)
            try await refresh()
        } catch { errorMessage = hostErrorMessage(error) }
    }

    func setRemoteState(_ state: Agentvmm_Host_V1_RemoteRegistrationState) async {
        guard let registration = snapshot.remoteRegistration else { return }
        do {
            _ = try await client?.setRemoteRegistrationState(state, expectedRevision: registration.revision)
            if state == .revoked { try revokeCredential(registrationID: registration.id) }
            try await refresh()
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func updateRemotePool(_ pool: RemotePoolSettings) async {
        guard let registration = snapshot.remoteRegistration else { return }
        do { _ = try await client?.updateRemotePool(pool, expectedRevision: registration.revision); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    private func runEnrollment(endpoint: String, registrationID: String, token: String) async throws {
        guard let host = URL(string: "https://\(endpoint)")?.host, !host.isEmpty,
              let helper = Bundle.main.url(forAuxiliaryExecutable: "agent-vmm-connect") else {
            throw HostClientError.invalidResponse("invalid controller endpoint or missing connector")
        }
        let process = Process()
        let input = Pipe()
        process.executableURL = helper
        process.arguments = ["--enroll", "--endpoint", endpoint, "--endpoint-identity", host, "--registration", registrationID,
                             "--remote-compute-socket", AppModel.remoteComputeSocketPath]
        process.standardInput = input
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        input.fileHandleForWriting.write(Data(token.utf8))
        try input.fileHandleForWriting.close()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { throw HostClientError.invalidResponse("controller enrollment failed") }
    }

    private func revokeCredential(registrationID: String) throws {
        guard let helper = Bundle.main.url(forAuxiliaryExecutable: "agent-vmm-connect") else {
            throw HostClientError.invalidResponse("missing connector")
        }
        let process = Process()
        process.executableURL = helper
        process.arguments = ["--revoke-credential", "--registration", registrationID]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = FileHandle.nullDevice
        try process.run()
        process.waitUntilExit()
        guard process.terminationStatus == 0 else { throw HostClientError.invalidResponse("credential revocation failed") }
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
