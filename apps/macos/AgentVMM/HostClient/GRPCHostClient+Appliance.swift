import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2

extension GRPCHostClient {
    func listAppliances() async throws -> [ApplianceStatusModel] {
        try await applianceService.list(.init()).appliances.map(applianceStatus)
    }

    func installBundledMCP(artifact: BundledApplianceArtifact, grant: ApplianceGrantModel) async throws {
        let manifest = try Agentvmm_Appliance_V1_ControllerApplianceManifest(serializedBytes: artifact.manifest)
        let approved = approvedGrant(grant, manifest: manifest)
        let deadline = deadlineMillis()
        var stage = Agentvmm_Appliance_V1_StageArtifactRequest()
        stage.requestID = UUID().uuidString
        stage.deadlineUnixMillis = deadline
        stage.manifest = manifest
        stage.publisherSignature = artifact.signature
        let artifactID = try await applianceService.stageArtifact(stage).artifactID
        guard !artifactID.isEmpty else { throw HostClientError.invalidResponse("staged MCP artifact is missing") }
        var planRequest = Agentvmm_Appliance_V1_PlanInstallRequest()
        planRequest.installationID = BundledMCPAppliance.installationID
        planRequest.requestID = UUID().uuidString
        planRequest.expectedRevision = 0
        planRequest.artifactID = artifactID
        planRequest.deadlineUnixMillis = deadline
        planRequest.approvedGrant = approved
        _ = try await applianceService.planInstall(planRequest)
        var install = Agentvmm_Appliance_V1_InstallRequest()
        install.installationID = BundledMCPAppliance.installationID
        install.requestID = UUID().uuidString
        install.expectedRevision = 0
        install.artifactID = artifactID
        install.deadlineUnixMillis = deadline
        install.approvedGrant = approved
        install.opaqueBootstrapSeed = randomBytes(count: 32)
        _ = try await applianceService.install(install)
    }

    func activateAppliance(_ appliance: ApplianceStatusModel) async throws {
        let deadline = deadlineMillis()
        var planRequest = Agentvmm_Appliance_V1_PlanActivationRequest()
        applyIdentity(appliance, deadline: deadline, to: &planRequest)
        _ = try await applianceService.planActivation(planRequest)
        var request = Agentvmm_Appliance_V1_ActivateRequest()
        applyIdentity(appliance, deadline: deadline, to: &request)
        _ = try await applianceService.activate(request)
    }

    func stopAppliance(_ appliance: ApplianceStatusModel) async throws {
        var request = Agentvmm_Appliance_V1_StopApplianceRequest()
        applyIdentity(appliance, deadline: deadlineMillis(), to: &request)
        _ = try await applianceService.stop(request)
    }

    func restartAppliance(_ appliance: ApplianceStatusModel) async throws {
        var request = Agentvmm_Appliance_V1_RestartApplianceRequest()
        applyIdentity(appliance, deadline: deadlineMillis(), to: &request)
        _ = try await applianceService.restart(request)
    }

    func planRemoveAppliance(_ appliance: ApplianceStatusModel) async throws -> ApplianceRemovePlanModel {
        var request = Agentvmm_Appliance_V1_PlanRemoveRequest()
        applyIdentity(appliance, deadline: deadlineMillis(), to: &request)
        let plan = try await applianceService.planRemove(request).plan
        return ApplianceRemovePlanModel(
            revision: plan.revision,
            impacts: plan.impacts.map { ApplianceImpactModel(resourceType: $0.resourceType, resourceID: $0.resourceID, effect: $0.effect, irreversible: $0.irreversible, blocker: $0.blocker) }
        )
    }

    func executeRemoveAppliance(_ appliance: ApplianceStatusModel, plan: ApplianceRemovePlanModel) async throws {
        var request = Agentvmm_Appliance_V1_ExecuteRemoveRequest()
        applyIdentity(appliance, deadline: deadlineMillis(), to: &request)
        request.expectedRevision = plan.revision
        request.confirmedResourceIds = plan.requiredConfirmations
        _ = try await applianceService.executeRemove(request)
    }
}

private extension GRPCHostClient {
    nonisolated func deadlineMillis() -> Int64 {
        Int64(Date().addingTimeInterval(120).timeIntervalSince1970 * 1_000)
    }

    nonisolated func randomBytes(count: Int) -> Data {
        var generator = SystemRandomNumberGenerator()
        return Data((0..<count).map { _ in UInt8.random(in: .min ... .max, using: &generator) })
    }

    nonisolated func approvedGrant(
        _ value: ApplianceGrantModel,
        manifest: Agentvmm_Appliance_V1_ControllerApplianceManifest
    ) -> Agentvmm_Appliance_V1_ApprovedGrant {
        var grant = Agentvmm_Appliance_V1_ApprovedGrant()
        grant.systemResources = manifest.systemResources
        grant.capacity.maxEnvironments = value.maxEnvironments
        grant.capacity.aggregateLimits = resourceLimits(value.aggregateLimits)
        grant.capacity.perEnvironmentLimits = resourceLimits(value.perEnvironmentLimits)
        grant.capacity.maxEgressMode = value.allowsPublicEgress ? .publicInternet : .denyAll
        grant.capacity.maxLeaseSeconds = value.maxLeaseSeconds
        grant.capacity.disconnectGraceSeconds = value.disconnectGraceSeconds
        grant.egressMode = manifest.requestedEgressMode
        grant.frontends = manifest.requestedFrontends.filter { value.enabledFrontendNames.contains($0.name) }
        return grant
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_PlanActivationRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_ActivateRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_StopApplianceRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_RestartApplianceRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_PlanRemoveRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }

    nonisolated func applyIdentity(_ value: ApplianceStatusModel, deadline: Int64, to request: inout Agentvmm_Appliance_V1_ExecuteRemoveRequest) {
        request.installationID = value.id; request.requestID = UUID().uuidString; request.expectedRevision = value.revision
        request.deadlineUnixMillis = deadline
    }
}
