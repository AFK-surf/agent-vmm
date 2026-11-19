import Foundation
import Observation

@MainActor
@Observable
final class AppModel {
    enum Section: String, CaseIterable, Identifiable {
        case system = "Overview"
        case access = "MCP"
        case devices = "My Devices"
        case environments = "Environments"
        case diagnostics = "Activity"
        var id: Self { self }
    }


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

    var section: Section? = .system
    var selectedEnvironmentID: String?
    var environmentSection: EnvironmentSection = .overview
    var snapshot = SystemSnapshotModel.loading
    var appliances: [ApplianceStatusModel] = []
    var deviceIdentity: DeviceIdentityModel?
    var personalMeshes: [PersonalMeshModel] = []
    var personalMeshPairing: PersonalMeshPairingModel?
    var deviceActionInProgress = false
    var deviceRefreshInProgress = false
    var events: [ActivityEvent] = []
    var metrics: [UsageSnapshot] = []
    var errorMessage: String?
    var isLoading = true
    var latestOperation: OperationModel?
    var gracefulStopFailed = false
    var vmAction: VMAction?
    var vmImpactSummary: Agentvmm_Host_V1_GetVMImpactSummaryResponse?
    var cleanupActionInProgress = false
    var policyActionInProgress = false
    var diagnosticsActionInProgress = false
    var supportPreviewInProgress = false

    var client: (any HostServing)?
    @ObservationIgnored var observationTask: Task<Void, Never>?
    @ObservationIgnored private var metricsTask: Task<Void, Never>?
    @ObservationIgnored private var snapshotRefreshTask: Task<Void, Never>?
    private var eventBootID = ""
    var isConnected = false
    @ObservationIgnored private lazy var refreshScheduler = RefreshScheduler { [weak self] in
        guard let self else { throw CancellationError() }
        try await self.performRefresh()
    }

    init(client: (any HostServing)? = nil) {
        self.client = client
    }

    deinit {
        observationTask?.cancel()
        metricsTask?.cancel()
        snapshotRefreshTask?.cancel()
    }

    func start(socketPath: String = AppModel.defaultSocketPath) async {
        guard observationTask == nil else { return }
        do {
            let client: any HostServing
            if let existing = self.client { client = existing }
            else { client = SocketHostClient(socketPath: socketPath) }
            self.client = client
            observationTask = Task { [weak self] in await self?.observe(client: client) }
            metricsTask = Task { [weak self] in await self?.observeMetrics(client: client) }
            snapshotRefreshTask = Task { [weak self] in
                while !Task.isCancelled {
                    do { try await Task.sleep(for: .seconds(5)) }
                    catch { return }
                    self?.refreshScheduler.requestRefresh()
                }
            }
        } catch {
            isLoading = false
            errorMessage = hostErrorMessage(error)
        }
    }

    func refresh() async throws {
        try await refreshScheduler.refreshNow()
    }

    private func performRefresh() async throws {
        guard let client else { throw HostClientError.disconnected }
        do {
            async let nextSnapshot = client.snapshot()
            async let nextAppliances = client.listAppliances()
            let (value, next) = try await (nextSnapshot, nextAppliances)
            applySnapshot(value)
            appliances = next
            isConnected = true
            isLoading = false
        } catch {
            isConnected = false
            isLoading = false
            errorMessage = hostErrorMessage(error)
            throw error
        }
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

    func refreshDevices() async {
        guard let client, !deviceRefreshInProgress else { return }
        deviceRefreshInProgress = true
        defer { deviceRefreshInProgress = false }
        do {
            async let identity = client.deviceIdentity()
            async let meshes = client.listPersonalMeshes()
            deviceIdentity = try await identity
            personalMeshes = try await meshes
        } catch {
            errorMessage = hostErrorMessage(error)
        }
    }

    func createPersonalMesh(registryAudience: String) async {
        guard let client, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do {
            let mesh = try await client.createPersonalMesh(registryAudience: registryAudience)
            personalMeshes.removeAll { $0.id == mesh.id }
            personalMeshes.append(mesh)
            personalMeshes.sort { $0.id < $1.id }
        } catch {
            errorMessage = hostErrorMessage(error)
        }
    }

    func revokePersonalMeshMember(meshID: String, deviceID: String, expectedRevision: UInt64) async {
        guard let client, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do {
            let mesh = try await client.revokePersonalMeshMember(meshID: meshID, deviceID: deviceID, expectedRevision: expectedRevision)
            if let index = personalMeshes.firstIndex(where: { $0.id == mesh.id }) {
                personalMeshes[index] = mesh
            }
        } catch {
            errorMessage = hostErrorMessage(error)
        }
    }

    func repairDeviceIdentity(confirmMembershipReset: Bool) async {
        guard let client, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do {
            deviceIdentity = try await client.repairDeviceIdentity(confirmMembershipReset: confirmMembershipReset)
            await refreshDevices()
        } catch {
            errorMessage = hostErrorMessage(error)
        }
    }

    func beginPersonalMeshPairing(meshID: String, expectedRevision: UInt64, pin: String, canManageMembers: Bool) async {
        guard let client, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do {
            personalMeshPairing = try await client.beginPersonalMeshPairing(meshID: meshID, expectedRevision: expectedRevision, pin: pin, canManageMembers: canManageMembers)
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func joinPersonalMeshPairing(invitationCode: String, pin: String) async {
        guard let client, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do { personalMeshPairing = try await client.joinPersonalMeshPairing(invitationCode: invitationCode, pin: pin) }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func refreshPersonalMeshPairing() async {
        guard let client, let pairing = personalMeshPairing else { return }
        do { personalMeshPairing = try await client.getPersonalMeshPairing(sessionID: pairing.sessionID) }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func confirmPersonalMeshPairing() async {
        guard let client, let pairing = personalMeshPairing, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do { personalMeshPairing = try await client.confirmPersonalMeshPairing(sessionID: pairing.sessionID) }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func rejectPersonalMeshPairing() async {
        guard let client, let pairing = personalMeshPairing, !deviceActionInProgress else { return }
        deviceActionInProgress = true
        defer { deviceActionInProgress = false }
        do {
            personalMeshPairing = try await client.rejectPersonalMeshPairing(sessionID: pairing.sessionID)
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func stop(environmentID: String, containerID: String) async {
        do { try await client?.stopContainer(environmentID: environmentID, containerID: containerID); try await refresh() }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func prepareVMAction() async -> Bool {
        do {
            guard let client else { throw HostClientError.disconnected }
            vmImpactSummary = try await client.getVMImpactSummary()
            return true
        } catch {
            errorMessage = hostErrorMessage(error)
            return false
        }
    }

    func startVM() async {
        guard vmAction == nil, let summary = vmImpactSummary else { return }
        vmAction = .starting
        errorMessage = nil
        defer { vmAction = nil }
        do { remember(try await client?.startVM(fence: SocketHostClient.vmMutationFence(summary))); gracefulStopFailed = false; try await refresh() }
        catch {
            errorMessage = hostErrorMessage(error)
            if (error as? RPCError)?.code == .aborted { _ = await prepareVMAction() }
        }
    }

    func stopVM(force: Bool) async {
        guard vmAction == nil, let summary = vmImpactSummary else { return }
        vmAction = force ? .forceStopping : .stopping
        errorMessage = nil
        defer { vmAction = nil }
        do { remember(try await client?.stopVM(force: force, fence: SocketHostClient.vmMutationFence(summary))); gracefulStopFailed = false; try await refresh() }
        catch {
            if !force { gracefulStopFailed = true }
            errorMessage = hostErrorMessage(error)
            if (error as? RPCError)?.code == .aborted { _ = await prepareVMAction() }
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
        guard !policyActionInProgress else { return }
        policyActionInProgress = true
        defer { policyActionInProgress = false }
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
        guard !cleanupActionInProgress else { return false }
        cleanupActionInProgress = true
        defer { cleanupActionInProgress = false }
        do {
            guard let operation = try await client?.executeCleanup(preview) else {
                throw HostClientError.disconnected
            }
            let completed = try await waitForOperation(operation)
            try await refresh()
            guard completed.state == .succeeded else {
                errorMessage = completed.message ?? completed.reason ?? "The environment could not be cleaned up."
                return false
            }
            return true
        } catch {
            errorMessage = hostErrorMessage(error)
            return false
        }
    }

    func runDiagnostics() async {
        guard !diagnosticsActionInProgress else { return }
        diagnosticsActionInProgress = true
        defer { diagnosticsActionInProgress = false }
        do {
            guard let operation = try await client?.runDiagnostics(environmentID: selectedEnvironmentID) else {
                throw HostClientError.disconnected
            }
            _ = try await waitForOperation(operation)
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    func supportPreview() async throws -> [SupportItem] {
        guard let client else { throw HostClientError.disconnected }
        guard !supportPreviewInProgress else { throw HostClientError.invalidResponse("A support bundle request is already in progress.") }
        supportPreviewInProgress = true
        defer { supportPreviewInProgress = false }
        return try await client.supportPreview(environmentID: selectedEnvironmentID)
    }

    func exportSupportBundle(sensitiveItems: [String]) async {
        guard !diagnosticsActionInProgress else { return }
        diagnosticsActionInProgress = true
        defer { diagnosticsActionInProgress = false }
        do {
            guard let operation = try await client?.exportSupportBundle(environmentID: selectedEnvironmentID, sensitiveItems: sensitiveItems) else {
                throw HostClientError.disconnected
            }
            _ = try await waitForOperation(operation)
        }
        catch { errorMessage = hostErrorMessage(error) }
    }

    private func waitForOperation(_ operation: OperationModel) async throws -> OperationModel {
        var current = operation
        remember(current)
        guard current.state == .running else { return current }
        guard let client else { throw HostClientError.disconnected }
        for _ in 0..<600 {
            try await Task.sleep(for: .milliseconds(500))
            current = try await client.getOperation(id: current.id)
            remember(current)
            if current.state != .running { return current }
        }
        throw HostClientError.invalidResponse("The operation did not finish in time. Check Activity for its current state.")
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
        var recoveredInitialState = false
        while !Task.isCancelled {
            do {
                try await refresh()
                delay = .milliseconds(250)
                if !recoveredInitialState {
                    await refreshDevices()
                    if let operationID = UserDefaults.standard.string(forKey: "latestOperationID") {
                        latestOperation = try? await client.getOperation(id: operationID)
                    }
                    recoveredInitialState = true
                }
                let sequence = events.last?.id ?? 0
                for try await event in client.events(after: sequence) {
                    record(event)
                    refreshScheduler.requestRefresh()
                }
                throw HostClientError.disconnected
            } catch is CancellationError {
                return
            } catch {
                isConnected = false
                errorMessage = hostErrorMessage(error)
                try? await Task.sleep(for: delay)
                delay = min(delay * 2, .seconds(8))
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
