import SwiftUI

struct EnvironmentActionsView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    let onReset: () -> Void
    let onDiscard: () -> Void
    let resetDisabledReason: String?
    let discardDisabledReason: String?

    @State private var pendingWorkloadAction: WorkloadAction?
    @State private var cpuMillis = ""
    @State private var memoryMiB = ""
    @State private var pids = ""
    @State private var diskGiB = ""
    @State private var allowsPublicEgress = false

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                GroupBox("Runtime details") {
                    VStack(alignment: .leading, spacing: 10) {
                        LabeledContent("Namespace ID", value: environment.namespaceID)
                        LabeledContent("Policy revision", value: String(environment.policyRevision))
                    }
                    .padding(.top, 6)
                }

                GroupBox("Workspace controls") {
                    VStack(alignment: .leading, spacing: 10) {
                        if environment.workloads.isEmpty {
                            Text("No workspaces or services are present.").foregroundStyle(.secondary)
                        }
                        ForEach(environment.workloads) { workload in
                            DisclosureGroup(workload.id) {
                                VStack(alignment: .leading, spacing: 8) {
                                    LabeledContent("Image", value: workload.image)
                                    LabeledContent("Command", value: workload.argv.joined(separator: " "))
                                    LabeledContent("Working directory", value: workload.workdir ?? "Image default")
                                    LabeledContent("User", value: workload.user ?? "Image default")
                                    LabeledContent("Environment keys", value: workload.environmentKeys.joined(separator: ", "))
                                    LabeledContent("Mounts", value: workload.mounts.joined(separator: ", "))
                                    HStack {
                                        Spacer()
                                        if workload.state == .running {
                                            Button("Interrupt…") { pendingWorkloadAction = .interrupt(workload.id) }
                                            Button("Stop…") { pendingWorkloadAction = .stop(workload.id) }
                                            Button("KILL…", role: .destructive) { pendingWorkloadAction = .kill(workload.id) }
                                        } else if workload.state == .stopped {
                                            Button("Continue…") { pendingWorkloadAction = .continueWorkload(workload.id) }
                                        }
                                    }
                                }
                                .padding(.top, 6)
                            }
                        }
                    }
                    .padding(.top, 6)
                }

                GroupBox("Limits and egress") {
                    Grid(alignment: .leading, horizontalSpacing: 14, verticalSpacing: 10) {
                        policyField("CPU millicores", text: $cpuMillis)
                        policyField("Memory MiB", text: $memoryMiB)
                        policyField("Process limit", text: $pids)
                        policyField("Retained disk GiB", text: $diskGiB)
                        GridRow {
                            Text("")
                            Toggle("Allow public internet egress", isOn: $allowsPublicEgress)
                        }
                        GridRow {
                            Text("")
                            Button("Apply Policy") { applyPolicy() }.disabled(parsedLimits == nil)
                        }
                    }
                    .padding(.top, 6)
                    Text("Policy updates use revision \(environment.policyRevision). Concurrent changes are rejected and appear after refresh.")
                        .font(.caption).foregroundStyle(.secondary)
                        .padding(.top, 8)
                }

                GroupBox("Environment data") {
                    VStack(alignment: .leading, spacing: 10) {
                        HStack {
                            Button("Reset Environment…", action: onReset)
                                .disabled(resetDisabledReason != nil)
                                .help(resetDisabledReason ?? "Remove writable workspace layers and logs")
                            Button("Delete Environment…", role: .destructive, action: onDiscard)
                                .disabled(discardDisabledReason != nil)
                                .help(discardDisabledReason ?? "Permanently delete this environment")
                        }
                        Text("Reset removes writable workspace layers and logs but keeps named retained data. Delete permanently removes the environment and confirmed retained data.")
                            .font(.caption).foregroundStyle(.secondary)
                    }
                    .padding(.top, 6)
                }

                if let operation = model.latestOperation {
                    GroupBox("Latest operation") {
                        VStack(alignment: .leading, spacing: 10) {
                            LabeledContent("State", value: operation.state)
                            LabeledContent("Stage", value: operation.stage)
                            if let resource = operation.resource { LabeledContent("Resource", value: resource) }
                            if let reason = operation.reason { LabeledContent("Reason", value: reason) }
                            if let message = operation.message { Text(message).foregroundStyle(.secondary) }
                            Text(operation.nextStep).foregroundStyle(.secondary)
                        }
                        .padding(.top, 6)
                    }
                }
            }
            .frame(maxWidth: 760, alignment: .leading)
            .padding(24)
        }
        .confirmationDialog(workloadActionTitle, isPresented: Binding(
            get: { pendingWorkloadAction != nil }, set: { if !$0 { pendingWorkloadAction = nil } }
        ), titleVisibility: .visible) {
            Button(workloadActionButton, role: pendingWorkloadAction?.isDestructive == true ? .destructive : nil) { executeWorkloadAction() }
            Button("Cancel", role: .cancel) { pendingWorkloadAction = nil }
        } message: {
            Text(workloadActionImpact)
        }
        .onAppear { loadPolicy() }
        .onChange(of: environment.policyRevision) { _, _ in loadPolicy() }
    }

    @ViewBuilder
    private func policyField(_ label: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).foregroundStyle(.secondary)
            TextField(label, text: text)
                .labelsHidden()
                .textFieldStyle(.roundedBorder)
                .frame(maxWidth: 240)
        }
    }

    private var parsedLimits: EnvironmentLimits? {
        guard let cpu = UInt32(cpuMillis), let processCount = UInt32(pids),
              let memory = scaledBytes(memoryMiB, multiplier: 1_048_576),
              let disk = scaledBytes(diskGiB, multiplier: 1_073_741_824),
              cpu > 0, processCount > 0, memory > 0, disk > 0 else { return nil }
        return EnvironmentLimits(cpuMillis: cpu, memoryBytes: memory, pids: processCount, diskBytes: disk)
    }

    private func scaledBytes(_ value: String, multiplier: UInt64) -> UInt64? {
        guard let number = UInt64(value) else { return nil }
        let result = number.multipliedReportingOverflow(by: multiplier)
        return result.overflow ? nil : result.partialValue
    }

    private func loadPolicy() {
        cpuMillis = String(environment.limits.cpuMillis)
        memoryMiB = String(environment.limits.memoryBytes / 1_048_576)
        pids = String(environment.limits.pids)
        diskGiB = String(environment.limits.diskBytes / 1_073_741_824)
        allowsPublicEgress = environment.allowsPublicEgress
    }

    private func applyPolicy() {
        guard let limits = parsedLimits else { return }
        Task { await model.updatePolicy(environmentID: environment.id, limits: limits, allowsPublicEgress: allowsPublicEgress, expectedRevision: environment.policyRevision) }
    }

    private func executeWorkloadAction() {
        guard let action = pendingWorkloadAction else { return }
        pendingWorkloadAction = nil
        Task {
            switch action {
            case .stop(let id): await model.stop(environmentID: environment.id, containerID: id)
            case .continueWorkload(let id): await model.continueWorkload(environmentID: environment.id, containerID: id)
            case .interrupt(let id): await model.signal(environmentID: environment.id, containerID: id, signal: .interrupt)
            case .kill(let id): await model.signal(environmentID: environment.id, containerID: id, signal: .kill)
            }
        }
    }

    private var workloadActionTitle: String {
        switch pendingWorkloadAction {
        case .stop: "Stop container?"
        case .continueWorkload: "Continue container?"
        case .interrupt: "Interrupt container?"
        case .kill: "Kill container immediately?"
        case nil: "Container action"
        }
    }

    private var workloadActionButton: String {
        switch pendingWorkloadAction {
        case .stop: "Stop"
        case .continueWorkload: "Continue"
        case .interrupt: "Send Interrupt"
        case .kill: "KILL"
        case nil: "Continue"
        }
    }

    private var workloadActionImpact: String {
        switch pendingWorkloadAction {
        case .stop: "Requests an orderly stop. The writable snapshot and named volumes are retained."
        case .continueWorkload: "Resumes the stopped container using its existing writable snapshot. Process state is not restored."
        case .interrupt: "Sends SIGINT to the container's initial process."
        case .kill: "Sends SIGKILL immediately. The process cannot flush in-memory state; retained files are not deleted."
        case nil: ""
        }
    }
}

private enum WorkloadAction {
    case stop(String)
    case continueWorkload(String)
    case interrupt(String)
    case kill(String)

    var isDestructive: Bool {
        if case .kill = self { true } else { false }
    }
}
