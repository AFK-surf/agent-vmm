import SwiftUI

struct RemoveAgentAccessSheet: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    let integration: AgentIntegrationModel
    @State private var deleteEmptyEnvironment = false
    @State private var isWorking = false
    @State private var localError: String?
    @State private var hasFreshSnapshot = false
    @State private var isCheckingEnvironment = false
    @State private var accessWasRemoved = false

    private var environment: AgentEnvironmentModel? { model.environment(id: integration.environmentID) }
    private var activeConnections: UInt32 {
        model.sessions.filter { $0.environmentID == integration.environmentID }.reduce(0) { $0 + $1.activeConnections }
    }
    private var canOfferEnvironmentDeletion: Bool {
        guard hasFreshSnapshot, model.snapshot.vmState == "Running", let environment, let usage = environment.usage else { return false }
        let accessRelationships = model.integrations.filter { $0.environmentID == integration.environmentID }
        return accessRelationships.count == 1 && accessRelationships.first?.id == integration.id &&
            !usage.isStale && environment.hasNoRetainedData && activeConnections == 0
    }

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Remove Agent Access").font(.title2.weight(.semibold))
            Text("\(integration.displayName) will no longer be able to start Agent VMM sessions.")

            GroupBox("Environment") {
                VStack(alignment: .leading, spacing: 9) {
                    LabeledContent("Name", value: environment?.name ?? integration.environmentID)
                    LabeledContent("Virtual machine", value: model.snapshot.vmState)
                    LabeledContent("Active connections", value: String(activeConnections))
                    LabeledContent("Workspaces and services", value: environment.map { String($0.workloads.count) } ?? "Unknown")
                    LabeledContent("Retained volumes", value: environment.map { String($0.volumes.count) } ?? "Unknown")
                    LabeledContent("Retained data", value: retainedDataSummary)
                }
                .padding(.top, 6)
            }

            if activeConnections > 0 {
                Label("Removing access disconnects the active agent sessions.", systemImage: "exclamationmark.triangle")
                    .foregroundStyle(.orange)
            }
            if !hasFreshSnapshot {
                if isCheckingEnvironment {
                    ProgressView("Checking environment data…")
                } else {
                    HStack {
                        Label("Environment data could not be verified.", systemImage: "exclamationmark.triangle")
                            .foregroundStyle(.orange)
                        Spacer()
                        Button("Retry Check") { checkEnvironment() }
                    }
                }
            } else if canOfferEnvironmentDeletion {
                Toggle("Also delete this empty environment", isOn: $deleteEmptyEnvironment)
                Text("The environment is verified empty. This option is off by default.")
                    .font(.caption).foregroundStyle(.secondary)
            } else {
                Text("The environment will be retained. Review and delete it separately from Environments after access is removed.")
                    .font(.caption).foregroundStyle(.secondary)
            }
            if let localError { Text(localError).foregroundStyle(.red) }

            HStack {
                if accessWasRemoved {
                    Button("Open Environment") { dismiss(); model.showEnvironment(integration.environmentID) }
                } else if !canOfferEnvironmentDeletion {
                    Button("Review Environment") { dismiss(); model.showEnvironment(integration.environmentID) }
                }
                Spacer()
                if accessWasRemoved {
                    Button("Done") { dismiss() }.keyboardShortcut(.defaultAction)
                } else {
                    Button("Cancel", role: .cancel) { dismiss() }
                    Button("Remove Access", role: .destructive) { removeAccess() }
                        .disabled(isWorking || !hasFreshSnapshot)
                }
            }
        }
        .padding(24)
        .frame(width: 540)
        .interactiveDismissDisabled(isWorking)
        .task {
            checkEnvironment()
        }
        .overlay {
            if isWorking {
                ProgressView("Removing agent access…")
                    .padding()
                    .background(.regularMaterial, in: RoundedRectangle(cornerRadius: 10))
            }
        }
    }

    private var retainedDataSummary: String {
        guard let environment, let usage = environment.usage, !usage.isStale else { return "Unknown" }
        return DisplayFormatting.bytes.string(fromByteCount: Int64(usage.diskBytes))
    }

    private func removeAccess() {
        isWorking = true
        localError = nil
        let shouldDeleteEnvironment = deleteEmptyEnvironment && canOfferEnvironmentDeletion
        Task {
            guard await model.removeIntegration(integration) else {
                localError = model.errorMessage
                isWorking = false
                return
            }
            if shouldDeleteEnvironment {
                do {
                    let preview = try await model.previewCleanup(environmentID: integration.environmentID, mode: .discard)
                    if await model.executeCleanup(preview) {
                        dismiss()
                        return
                    }
                    localError = "Access was removed, but the environment was retained. \(model.errorMessage ?? "Review it in Environments and retry deletion.")"
                    accessWasRemoved = true
                    isWorking = false
                    return
                } catch {
                    localError = "Access was removed, but the environment was retained. \(hostErrorMessage(error))"
                    accessWasRemoved = true
                    isWorking = false
                    return
                }
            }
            dismiss()
        }
    }

    private func checkEnvironment() {
        isCheckingEnvironment = true
        hasFreshSnapshot = false
        localError = nil
        Task {
            do {
                try await model.refresh()
                hasFreshSnapshot = true
            } catch {
                localError = hostErrorMessage(error)
            }
            isCheckingEnvironment = false
        }
    }
}
