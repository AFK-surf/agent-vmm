import SwiftUI

struct EnvironmentListView: View {
    @Bindable var model: AppModel
    @State private var pendingCleanup: PendingEnvironmentCleanup?
    @State private var typedEnvironmentName = ""

    var body: some View {
        Group {
            if model.snapshot.environments.isEmpty {
                ContentUnavailableView {
                    Label("No agent environments", systemImage: "shippingbox")
                } description: {
                    Text(emptyStateDescription)
                } actions: {
                    if model.snapshot.vmState == "Stopped" {
                        Button("Start Virtual Machine") { Task { await model.startVM() } }
                            .buttonStyle(.borderedProminent)
                    }
                }
            } else {
                HStack(spacing: 0) {
                    environmentList
                        .frame(minWidth: 250, idealWidth: 280, maxWidth: 300)
                    Divider()
                    if let id = model.selectedEnvironmentID, let environment = model.environment(id: id) {
                        EnvironmentDetailView(
                            model: model,
                            environment: environment,
                            onReset: { prepareCleanup(environment, mode: .reset) },
                            onDiscard: { prepareCleanup(environment, mode: .discard) },
                            resetDisabledReason: cleanupDisabledReason(for: environment, mode: .reset),
                            discardDisabledReason: cleanupDisabledReason(for: environment, mode: .discard)
                        )
                            .frame(minWidth: 640, maxWidth: .infinity, maxHeight: .infinity)
                            .clipped()
                    } else {
                        ContentUnavailableView("Choose an agent environment", systemImage: "shippingbox")
                            .frame(maxWidth: .infinity, maxHeight: .infinity)
                    }
                }
            }
        }
        .navigationTitle("Environments")
        .onAppear { selectFirstEnvironmentIfNeeded() }
        .onChange(of: model.snapshot.environments) { _, _ in selectFirstEnvironmentIfNeeded() }
        .alert(cleanupTitle, isPresented: Binding(
            get: { pendingCleanup != nil },
            set: { if !$0 { pendingCleanup = nil } }
        )) {
            if pendingCleanup?.preview.mode == .discard {
                TextField("Type \(pendingCleanup?.environmentName ?? "")", text: $typedEnvironmentName)
            }
            Button("Cancel", role: .cancel) { pendingCleanup = nil }
            Button(pendingCleanup?.preview.mode == .discard ? "Delete Environment" : "Reset Environment", role: .destructive) {
                executePendingCleanup()
            }
            .disabled(pendingCleanup?.preview.mode == .discard && typedEnvironmentName != pendingCleanup?.environmentName)
        } message: {
            Text(cleanupMessage)
        }
    }

    private var environmentList: some View {
        VStack(alignment: .leading, spacing: 0) {
            Text("Environments")
                .font(.title2.weight(.semibold))
                .padding(.horizontal, 18)
                .padding(.vertical, 16)
            List(model.snapshot.environments, selection: $model.selectedEnvironmentID) { environment in
                VStack(alignment: .leading, spacing: 7) {
                    HStack {
                        Text(environment.name).font(.headline)
                        Spacer()
                        Text("\(environment.workloads.filter { $0.state == .running }.count) running")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    HStack(spacing: 8) {
                        Label(environment.origin.rawValue, systemImage: environment.origin == .local ? "desktopcomputer" : "network")
                            .foregroundStyle(.secondary)
                        HealthLabel(level: environment.health)
                        if isRetained(environment) {
                            Text("Retained")
                                .foregroundStyle(.secondary)
                        }
                    }
                    .font(.caption)
                }
                .tag(environment.id)
                .padding(.vertical, 6)
                .contextMenu {
                    Button("Open") { model.showEnvironment(environment.id) }
                    if environment.origin == .remote {
                        Button("Open Remote Controller") { model.showAccess(.remote) }
                    }
                    if model.snapshot.vmState == "Stopped" {
                        Button("Start Virtual Machine") { Task { await model.startVM() } }
                    }
                    Divider()
                    Button("Reset Environment…") { prepareCleanup(environment, mode: .reset) }
                        .disabled(cleanupDisabledReason(for: environment, mode: .reset) != nil)
                    Button("Delete Environment…", role: .destructive) { prepareCleanup(environment, mode: .discard) }
                        .disabled(cleanupDisabledReason(for: environment, mode: .discard) != nil)
                        .help(cleanupDisabledReason(for: environment, mode: .discard) ?? "Permanently delete this environment")
                }
            }
            .listStyle(.sidebar)
        }
        .background(.background.secondary)
    }

    private var emptyStateDescription: String {
        switch model.snapshot.vmState {
        case "Stopped": "Start the virtual machine to make environments available to agents."
        case "Starting": "Environments will appear after the virtual machine finishes starting."
        default: "Environments appear when agent access is added or a remote allocation is retained."
        }
    }

    private func selectFirstEnvironmentIfNeeded() {
        if let selected = model.selectedEnvironmentID,
           model.snapshot.environments.contains(where: { $0.id == selected }) { return }
        model.selectedEnvironmentID = model.snapshot.environments.first?.id
        model.environmentSection = .overview
    }

    private func isRetained(_ environment: AgentEnvironmentModel) -> Bool {
        if environment.origin == .remote {
            return remoteAllocation(for: environment)?.state == "Retained"
        }
        return true
    }

    private func cleanupDisabledReason(for environment: AgentEnvironmentModel, mode: CleanupMode) -> String? {
        if model.snapshot.vmState != "Running" { return "Start the virtual machine first." }
        if model.vmAction != nil { return "Wait for the virtual machine operation to finish." }
        guard mode == .discard else { return nil }
        if environment.origin == .remote {
            guard let allocation = remoteAllocation(for: environment), allocation.state == "Retained" else {
                return "Release this allocation from Remote Controller before deleting it."
            }
            if allocation.lease?.state == .active {
                return "Release the active remote lease first."
            }
        }
        if environment.workloads.contains(where: { $0.state == .running }) {
            return "Stop running workspaces and services first."
        }
        return nil
    }

    private func remoteAllocation(for environment: AgentEnvironmentModel) -> RemoteAllocationModel? {
        guard let allocationID = environment.allocationID else { return nil }
        return model.snapshot.remoteRegistrations.lazy.flatMap(\.allocations).first { $0.allocationID == allocationID }
    }

    private func prepareCleanup(_ environment: AgentEnvironmentModel, mode: CleanupMode) {
        if let reason = cleanupDisabledReason(for: environment, mode: mode) {
            model.errorMessage = reason
            return
        }
        Task {
            do {
                let preview = try await model.previewCleanup(environmentID: environment.id, mode: mode)
                typedEnvironmentName = ""
                pendingCleanup = PendingEnvironmentCleanup(
                    environmentName: environment.name,
                    health: environment.health.label,
                    preview: preview
                )
            } catch {
                model.errorMessage = hostErrorMessage(error)
            }
        }
    }

    private func executePendingCleanup() {
        guard let cleanup = pendingCleanup else { return }
        pendingCleanup = nil
        Task {
            let succeeded = await model.executeCleanup(cleanup.preview)
            if succeeded { selectFirstEnvironmentIfNeeded() }
        }
    }

    private var cleanupTitle: String {
        pendingCleanup?.preview.mode == .discard ? "Delete environment?" : "Reset environment?"
    }

    private var cleanupMessage: String {
        guard let cleanup = pendingCleanup else { return "" }
        let action = cleanup.preview.mode == .discard
            ? "This permanently deletes \(cleanup.environmentName) and its confirmed retained data."
            : "This removes writable workspace layers and logs from \(cleanup.environmentName)."
        return ([action, "Current status: \(cleanup.health)."] + cleanup.preview.impacts).joined(separator: "\n")
    }
}

private struct PendingEnvironmentCleanup {
    let environmentName: String
    let health: String
    let preview: CleanupPreview
}

private struct EnvironmentDetailView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    let onReset: () -> Void
    let onDiscard: () -> Void
    let resetDisabledReason: String?
    let discardDisabledReason: String?

    var body: some View {
        VStack(spacing: 0) {
            header
            Divider()
            Picker("Environment section", selection: $model.environmentSection) {
                ForEach(AppModel.EnvironmentSection.allCases) { section in
                    Text(section.rawValue).tag(section)
                }
            }
            .pickerStyle(.segmented)
            .labelsHidden()
            .frame(maxWidth: 680)
            .padding(.horizontal, 24)
            .padding(.vertical, 12)
            Divider()
            sectionContent
                .frame(maxWidth: .infinity, maxHeight: .infinity, alignment: .topLeading)
        }
        .background(Color(nsColor: .windowBackgroundColor))
    }

    private var header: some View {
        HStack(spacing: 18) {
            Image(systemName: "shippingbox.fill")
                .font(.title2)
                .foregroundStyle(.blue)
                .frame(width: 44, height: 44)
                .background(.blue.opacity(0.1), in: RoundedRectangle(cornerRadius: 10))
            VStack(alignment: .leading, spacing: 5) {
                HStack(spacing: 10) {
                    Text(environment.name).font(.title2.weight(.semibold))
                    HealthLabel(level: environment.health).font(.callout)
                }
                HStack(spacing: 12) {
                    Label(environment.origin.rawValue, systemImage: environment.origin == .local ? "desktopcomputer" : "network")
                    Text("\(environment.workloads.filter { $0.state == .running }.count) running")
                    if environment.origin == .remote {
                        Text("Remote allocation")
                    } else {
                        Text("Local environment")
                    }
                }
                .font(.caption).foregroundStyle(.secondary)
            }
            Spacer()
            if model.snapshot.vmState == "Stopped" {
                Button("Start Virtual Machine") { Task { await model.startVM() } }
                    .buttonStyle(.borderedProminent)
                    .disabled(model.vmAction != nil)
            }
        }
        .padding(.horizontal, 24)
        .padding(.vertical, 18)
    }

    @ViewBuilder private var sectionContent: some View {
        switch model.environmentSection {
        case .overview: EnvironmentOverviewView(model: model, environment: environment)
        case .containers: EnvironmentContainersView(model: model, environment: environment)
        case .storage: EnvironmentStorageView(model: model, environment: environment)
        case .logs:
            LogsActivityView(model: model, environment: environment)
                .padding(20)
        case .network: EnvironmentNetworkView(model: model, environment: environment)
        case .advanced:
            EnvironmentActionsView(
                model: model,
                environment: environment,
                onReset: onReset,
                onDiscard: onDiscard,
                resetDisabledReason: resetDisabledReason,
                discardDisabledReason: discardDisabledReason
            )
        }
    }

}

private struct EnvironmentNetworkView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var allowsPublicEgress = false

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                GroupBox("Outbound access") {
                    VStack(alignment: .leading, spacing: 10) {
                        Label(
                            environment.allowsPublicEgress ? "Public internet is allowed" : "Public internet is blocked",
                            systemImage: environment.allowsPublicEgress ? "network.badge.shield.half.filled" : "lock.shield"
                        )
                        .font(.headline)
                        Text("Private networks and the local LAN remain inaccessible. Environment-to-environment connections require explicit product-managed links.")
                            .foregroundStyle(.secondary)
                    }
                    .padding(.top, 6)
                }

                GroupBox("Access policy") {
                    VStack(alignment: .leading, spacing: 10) {
                        Picker("Outbound policy", selection: $allowsPublicEgress) {
                            Text("Block public internet").tag(false)
                            Text("Allow public internet").tag(true)
                        }
                        .pickerStyle(.radioGroup)
                        HStack {
                            Button("Save Policy") { save() }.disabled(allowsPublicEgress == environment.allowsPublicEgress)
                            Button("Revert") { allowsPublicEgress = environment.allowsPublicEgress }
                                .disabled(allowsPublicEgress == environment.allowsPublicEgress)
                        }
                        Text("This changes new outbound connections. Existing connections may close when policy is tightened.")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    .padding(.top, 6)
                }

                GroupBox("Traffic") {
                    HStack {
                        Text("Live receive and transmit rates are shown with CPU, memory, and disk I/O.").foregroundStyle(.secondary)
                        Spacer()
                        Button("Open Metrics") { model.environmentSection = .overview }
                    }
                    .padding(.top, 6)
                }
            }
            .frame(maxWidth: 920, alignment: .leading)
            .padding(24)
        }
        .onAppear { allowsPublicEgress = environment.allowsPublicEgress }
        .onChange(of: environment.policyRevision) { _, _ in allowsPublicEgress = environment.allowsPublicEgress }
    }

    private func save() {
        Task {
            await model.updatePolicy(
                environmentID: environment.id,
                limits: environment.limits,
                allowsPublicEgress: allowsPublicEgress,
                expectedRevision: environment.policyRevision
            )
        }
    }
}
