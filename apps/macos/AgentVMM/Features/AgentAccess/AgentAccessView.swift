import AppKit
import SwiftUI

struct AgentAccessView: View {
    @Bindable var model: AppModel

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 20) {
                Picker("Access type", selection: $model.accessTab) {
                    ForEach(AppModel.AccessTab.allCases) { Text($0.rawValue).tag($0) }
                }
                .pickerStyle(.segmented).frame(maxWidth: 420)
                switch model.accessTab {
                case .local: BundledMCPView(model: model)
                case .remote: RemoteControllerView(model: model)
                }
            }
            .frame(maxWidth: 980, alignment: .leading).padding(28)
        }
        .navigationTitle("MCP")
    }
}

private struct RemoteControllerView: View {
    @Bindable var model: AppModel
    @State private var showingEnrollment = false
    @State private var showingRetry = false
    @State private var showingCapacity = false
    @State private var showingRevoke = false
    @State private var showingDrain = false
    @State private var showingIncompleteRemoval = false
    @State private var showingReconnectConfirmation = false
    @State private var selectedRegistrationID: String?

    private var externalRegistrations: [RemoteRegistrationModel] {
        model.snapshot.remoteRegistrations.filter { $0.owner == .external }
    }

    private var selectedRegistration: RemoteRegistrationModel? {
        externalRegistrations.first { $0.id == selectedRegistrationID }
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            if externalRegistrations.isEmpty {
                ContentUnavailableView {
                    Label("No remote controller", systemImage: "network")
                } description: {
                    Text("Connect one approved controller to expose a bounded capacity pool over an outbound connection.")
                } actions: {
                    Button("Connect Controller…") { showingEnrollment = true }.buttonStyle(.borderedProminent)
                }
                .frame(maxWidth: .infinity, minHeight: 320)
            } else { ForEach(externalRegistrations, id: \.id) { controller($0) } }
        }
        .sheet(isPresented: $showingEnrollment) { RemoteEnrollmentSheet(model: model) }
        .sheet(isPresented: $showingRetry) { if let registration = selectedRegistration { RemoteRetrySheet(model: model, registration: registration) } }
        .sheet(isPresented: $showingCapacity) {
            if let registration = selectedRegistration { RemoteCapacitySheet(model: model, registration: registration) }
        }
        .alert("Revoke controller access?", isPresented: $showingRevoke) {
            Button("Cancel", role: .cancel) {}
            Button("Revoke Access", role: .destructive) { if let id = selectedRegistrationID { Task { _ = await model.revokeRemoteAccess(registrationID: id) } } }
        } message: {
            Text("The connector will disconnect, its protected credential will be deleted, and active remote sessions will be revoked. Retained environments remain available for local inspection.")
        }
        .alert("Drain remote access?", isPresented: $showingDrain) {
            Button("Cancel", role: .cancel) {}
            Button("Drain") { if let id = selectedRegistrationID { Task { await model.setRemoteAdmission(.draining, registrationID: id) } } }
        } message: {
            Text("New allocations and leases will be rejected. Existing leases continue until they finish or expire.")
        }
        .alert("Remove incomplete registration?", isPresented: $showingIncompleteRemoval) {
            Button("Cancel", role: .cancel) {}
            Button("Remove Registration", role: .destructive) { if let id = selectedRegistrationID { Task { await model.removeIncompleteRemoteRegistration(registrationID: id) } } }
        } message: {
            Text("The incomplete controller setup and any stored credential will be removed. Existing environments are retained.")
        }
        .alert("Reconnect with a new credential?", isPresented: $showingReconnectConfirmation) {
            Button("Cancel", role: .cancel) {}
            Button("Revoke and Reconnect", role: .destructive) {
                Task {
                    if let id = selectedRegistrationID { _ = await model.revokeRemoteAccess(registrationID: id) }
                    if externalRegistrations.isEmpty { showingEnrollment = true }
                }
            }
        } message: {
            Text("The current credential and active remote sessions will be revoked before new enrollment begins. Remote environments and retained data remain available.")
        }
    }

    @ViewBuilder private func controller(_ registration: RemoteRegistrationModel) -> some View {
        HStack(alignment: .top, spacing: 16) {
            Image(systemName: "network").font(.title).foregroundStyle(.blue).frame(width: 42)
            VStack(alignment: .leading, spacing: 7) {
                Text(registration.displayName).font(.title2.weight(.semibold))
                Text(registration.endpoint).foregroundStyle(.secondary)
                HStack { transportStatus(registration); admissionStatus(registration) }
                if let failure = registration.connector.failureMessage { Text(failure).font(.caption).foregroundStyle(.red) }
                if registration.connectionState != .connected {
                    HStack(spacing: 12) {
                        if let lastConnected = registration.connector.lastConnectedAt {
                            Text("Last connected \(lastConnected.formatted(.relative(presentation: .named)))")
                        }
                        if registration.connector.reconnectAttempt > 0 {
                            Text("Attempt \(registration.connector.reconnectAttempt)")
                        }
                        if let nextRetry = registration.connector.nextRetryAt {
                            Text("Next retry \(nextRetry.formatted(.relative(presentation: .named)))")
                        }
                    }.font(.caption).foregroundStyle(.secondary)
                }
            }
            Spacer()
            controllerActions(registration)
        }
        .padding(18)
        .background(.background, in: RoundedRectangle(cornerRadius: 10))
        .overlay(RoundedRectangle(cornerRadius: 10).stroke(.quaternary))

        if registration.connectionState == .offline && registration.enrollmentState == .ready {
            HStack {
                switch registration.connector.phase {
                case .authFailed:
                    Button("Revoke and Reconnect…") { selectedRegistrationID = registration.id; showingReconnectConfirmation = true }.buttonStyle(.borderedProminent)
                case .configError, .retryWait, .stopped:
                    Button("Repair Connector") { Task { await model.repairServices() } }.buttonStyle(.borderedProminent)
                default:
                    EmptyView()
                }
                Button("View Details") { model.showActivity() }
                Spacer()
            }
        }

        if registration.enrollmentState == .incomplete || registration.enrollmentState == .pending {
            GroupBox(registration.enrollmentState.rawValue) {
                VStack(alignment: .leading, spacing: 10) {
                    Text(registration.enrollmentError ?? "Credential setup did not finish. Retry with a new one-time token or remove this registration.")
                    HStack {
                        if registration.enrollmentState == .incomplete || registration.enrollmentState == .pending {
                            Button("Retry Enrollment…") { selectedRegistrationID = registration.id; showingRetry = true }.buttonStyle(.borderedProminent)
                        }
                        Button("Remove Registration", role: .destructive) { selectedRegistrationID = registration.id; showingIncompleteRemoval = true }
                    }
                }.frame(maxWidth: .infinity, alignment: .leading).padding(6)
            }
        } else if registration.enrollmentState == .revoking {
            GroupBox("Revocation incomplete") {
                HStack { Text("Delete the protected credential and finish revocation."); Spacer(); Button("Retry Revocation") { Task { await model.finishRemoteRevocation(registrationID: registration.id) } } }
                    .padding(6)
            }
        } else {
            capacitySummary(registration)
            allocations(registration)
        }
    }

    @ViewBuilder private func controllerActions(_ registration: RemoteRegistrationModel) -> some View {
        HStack {
            if registration.enrollmentState == .ready {
                Button("Edit Capacity…") { selectedRegistrationID = registration.id; showingCapacity = true }
                if registration.state == .accepting {
                    Button("Drain…") { selectedRegistrationID = registration.id; showingDrain = true }
                } else if registration.state == .draining {
                    Button("Resume Accepting Work") { Task { await model.setRemoteAdmission(.accepting, registrationID: registration.id) } }.buttonStyle(.borderedProminent)
                }
                Menu { Button("Revoke Access…", role: .destructive) { selectedRegistrationID = registration.id; showingRevoke = true } } label: { Image(systemName: "ellipsis.circle") }
            }
        }
    }

    private func capacitySummary(_ registration: RemoteRegistrationModel) -> some View {
        LazyVGrid(columns: [GridItem(.adaptive(minimum: 180), spacing: 12)], spacing: 12) {
            metric("Environments", "\(registration.allocations.count) / \(registration.pool.maxEnvironments)", "Maximum capacity")
            metric("Active leases", String(registration.activeLeases), registration.activeLeases == 0 ? "No active work" : "Existing work continues while draining")
            metric("Retained", String(registration.allocations.filter { $0.lease == nil }.count), "No active lease")
        }
    }

    private func metric(_ label: String, _ value: String, _ caption: String) -> some View {
        GroupBox {
            VStack(alignment: .leading, spacing: 5) { Text(label).foregroundStyle(.secondary); Text(value).font(.title2.weight(.semibold)); Text(caption).font(.caption).foregroundStyle(.secondary) }
                .frame(maxWidth: .infinity, alignment: .leading).padding(4)
        }
    }

    private func allocations(_ registration: RemoteRegistrationModel) -> some View {
        VStack(alignment: .leading, spacing: 10) {
            Text("Allocations").font(.headline)
            if registration.allocations.isEmpty { Text("No remote environments yet.").foregroundStyle(.secondary) }
            else {
                GroupBox {
                    VStack(spacing: 0) {
                        ForEach(Array(registration.allocations.enumerated()), id: \.element.id) { index, allocation in
                            HStack {
                                VStack(alignment: .leading) { Text(allocation.displayName.isEmpty ? allocation.allocationID : allocation.displayName).font(.headline); Text(allocation.allocationID).font(.caption).foregroundStyle(.secondary) }
                                Spacer()
                                Text(allocation.state).foregroundStyle(.secondary)
                                VStack(alignment: .trailing) {
                                    Text(allocation.lease?.state.rawValue ?? "Retained").foregroundStyle(.secondary)
                                    if let expiry = allocation.lease?.expiresAt {
                                        Text("Expires \(expiry.formatted(.relative(presentation: .named)))").font(.caption).foregroundStyle(.secondary)
                                    } else {
                                        Text("Data retained locally").font(.caption).foregroundStyle(.secondary)
                                    }
                                }
                                Button("Open") { model.showEnvironment(allocation.environmentID) }.disabled(allocation.environmentID.isEmpty)
                            }.padding(.vertical, 10)
                            if index != registration.allocations.count - 1 { Divider() }
                        }
                    }.padding(.horizontal, 6)
                }
            }
        }
    }

    private func transportStatus(_ registration: RemoteRegistrationModel) -> AccessStatusView {
        switch registration.connectionState {
        case .connected: AccessStatusView(label: "Connected", symbol: "checkmark.circle.fill", kind: .success)
        case .connecting: AccessStatusView(label: "Connecting", symbol: "arrow.triangle.2.circlepath", kind: .neutral)
        case .offline: AccessStatusView(label: "Offline", symbol: "xmark.circle", kind: registration.enrollmentState == .ready ? .failure : .neutral)
        }
    }

    private func admissionStatus(_ registration: RemoteRegistrationModel) -> AccessStatusView {
        switch registration.state {
        case .accepting: AccessStatusView(label: "Accepting new work", symbol: "play.circle", kind: .neutral)
        case .draining: AccessStatusView(label: "Draining", symbol: "pause.circle", kind: .attention)
        case .revoked: AccessStatusView(label: "Revoked", symbol: "nosign", kind: .neutral)
        case .unknown: AccessStatusView(label: "Unavailable", symbol: "questionmark.circle", kind: .failure)
        }
    }
}

private struct RemoteEnrollmentSheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    @State private var endpoint = ""
    @State private var name = ""
    @State private var token = ""
    @State private var pool = RemotePoolDraft()
    @State private var isWorking = false
    @State private var localError: String?
    @State private var reviewing = false

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Connect Remote Controller").font(.title2.weight(.semibold))
            Form {
                if reviewing {
                    LabeledContent("Controller", value: name)
                    LabeledContent("TLS endpoint", value: endpoint)
                    LabeledContent("Maximum environments", value: String(pool.maxEnvironments))
                    LabeledContent("Per-environment CPU", value: "\(pool.perEnvironmentCPU) millicores")
                    LabeledContent("Per-environment memory", value: "\(pool.perEnvironmentMemoryMiB) MiB")
                } else {
                    TextField("TLS endpoint", text: $endpoint, prompt: Text("controller.example:443"))
                    TextField("Controller name", text: $name)
                    SecureField("One-time enrollment token", text: $token)
                    RemoteCapacityEditor(draft: $pool)
                }
                Text("The connector opens an outbound connection. The returned credential is stored by the signed build's protected credential provider and is never included in snapshots, logs, or support bundles.")
                    .font(.caption).foregroundStyle(.secondary)
                if let localError { Label(localError, systemImage: "exclamationmark.triangle.fill").foregroundStyle(.red).accessibilityAddTraits(.isHeader) }
            }
            HStack {
                if reviewing { Button("Back") { reviewing = false } }
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                if reviewing {
                    Button("Connect") { connect() }.buttonStyle(.borderedProminent).disabled(isWorking)
                } else {
                    Button("Review") { reviewing = true }.buttonStyle(.borderedProminent).disabled(endpoint.isEmpty || name.isEmpty || token.isEmpty || !pool.isValid || isWorking)
                }
            }
        }
        .padding(24).frame(width: 590)
        .overlay { if isWorking { ProgressView(model.enrollmentStage?.rawValue ?? "Preparing…").padding().background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10)) } }
        .interactiveDismissDisabled(isWorking)
    }

    private func connect() {
        let oneTimeToken = token
        token = ""
        isWorking = true
        Task {
            let succeeded = await model.enrollRemote(endpoint: endpoint, displayName: name, token: oneTimeToken, pool: pool.settings)
            isWorking = false
            if succeeded { dismiss() } else { localError = model.errorMessage ?? "Enrollment failed. Enter a new one-time token and retry." }
        }
    }
}

private struct RemoteRetrySheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    let registration: RemoteRegistrationModel
    @State private var token = ""
    @State private var isWorking = false
    @State private var localError: String?
    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            Text("Retry Enrollment").font(.title2.weight(.semibold))
            SecureField("New one-time enrollment token", text: $token)
            if let localError { Label(localError, systemImage: "exclamationmark.triangle.fill").foregroundStyle(.red).accessibilityAddTraits(.isHeader) }
            HStack { Spacer(); Button("Cancel", role: .cancel) { dismiss() }; Button("Retry") { retry() }.buttonStyle(.borderedProminent).disabled(token.isEmpty || isWorking) }
        }
        .padding(24).frame(width: 500)
        .overlay { if isWorking { ProgressView(model.enrollmentStage?.rawValue ?? "Preparing…").padding().background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10)) } }
        .interactiveDismissDisabled(isWorking)
    }

    private func retry() {
        let value = token
        token = ""
        isWorking = true
        Task {
            let succeeded = await model.retryRemoteEnrollment(token: value, registrationID: registration.id)
            isWorking = false
            if succeeded { dismiss() } else { localError = model.errorMessage ?? "Enrollment retry failed. Enter a new one-time token and try again." }
        }
    }
}

private struct RemoteCapacitySheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    let registration: RemoteRegistrationModel
    @State private var pool: RemotePoolDraft
    @State private var localError: String?
    @State private var isWorking = false

    init(model: AppModel, registration: RemoteRegistrationModel) {
        self.model = model
        self.registration = registration
        _pool = State(initialValue: RemotePoolDraft(registration.pool))
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Edit Remote Capacity").font(.title2.weight(.semibold))
            Form {
                RemoteCapacityEditor(draft: $pool)
                Text("This update changes admission capacity only. Existing leases continue under their current limits.").font(.caption).foregroundStyle(.secondary)
                if let localError { Label(localError, systemImage: "exclamationmark.triangle.fill").foregroundStyle(.red).accessibilityAddTraits(.isHeader) }
            }
            HStack { Spacer(); Button("Cancel", role: .cancel) { dismiss() }; Button("Update") { update() }.buttonStyle(.borderedProminent).disabled(!pool.isValid || isWorking) }
        }.padding(24).frame(width: 540)
    }

    private func update() {
        isWorking = true
        Task {
            if await model.updateRemotePool(pool.settings, registrationID: registration.id) { dismiss() }
            else { localError = model.errorMessage ?? "Capacity could not be updated."; isWorking = false }
        }
    }
}
