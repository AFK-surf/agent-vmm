import SwiftUI

struct EnvironmentActionsView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel

    @State private var preview: CleanupPreview?
    @State private var typedName = ""
    @State private var showingCleanup = false
    @State private var pendingWorkloadAction: WorkloadAction?
    @State private var cpuMillis = ""
    @State private var memoryMiB = ""
    @State private var pids = ""
    @State private var diskGiB = ""
    @State private var allowsPublicEgress = false

    var body: some View {
        Form {
            Section("Runtime details") {
                LabeledContent("Namespace ID", value: environment.namespaceID)
                LabeledContent("Policy revision", value: String(environment.policyRevision))
                ForEach(environment.workloads) { workload in
                    DisclosureGroup(workload.id) {
                        LabeledContent("Image", value: workload.image)
                        LabeledContent("Command", value: workload.argv.joined(separator: " "))
                        LabeledContent("Working directory", value: workload.workdir ?? "Image default")
                        LabeledContent("User", value: workload.user ?? "Image default")
                        LabeledContent("Environment keys", value: workload.environmentKeys.joined(separator: ", "))
                        LabeledContent("Mounts", value: workload.mounts.joined(separator: ", "))
                    }
                }
            }
            Section("Workspace controls") {
                ForEach(environment.workloads) { workload in
                    HStack {
                        Text(workload.id)
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
            }
            Section("Limits and egress") {
                TextField("CPU millicores", text: $cpuMillis)
                TextField("Memory MiB", text: $memoryMiB)
                TextField("Process limit", text: $pids)
                TextField("Retained disk GiB", text: $diskGiB)
                Toggle("Allow public internet egress", isOn: $allowsPublicEgress)
                Button("Apply policy") { applyPolicy() }.disabled(parsedLimits == nil)
                Text("Policy updates use revision \(environment.policyRevision). A concurrent update is rejected and the desired/observed difference remains visible after refresh.")
                    .foregroundStyle(.secondary)
            }
            Section("Environment data") {
                Button("Reset environment…") { prepare(.reset) }
                Button("Discard environment…", role: .destructive) { prepare(.discard) }
                Text("Reset removes writable workspace layers and logs but retains named data. Discard permanently removes the environment and confirmed named data.")
                    .foregroundStyle(.secondary)
            }
            if let operation = model.latestOperation {
                Section("Latest operation") {
                    LabeledContent("State", value: operation.state)
                    LabeledContent("Stage", value: operation.stage)
                    if let resource = operation.resource { LabeledContent("Resource", value: resource) }
                    if let reason = operation.reason { LabeledContent("Reason", value: reason) }
                    Text(operation.nextStep).foregroundStyle(.secondary)
                }
            }
        }
        .alert(preview?.mode == .discard ? "Discard environment?" : "Reset environment?", isPresented: $showingCleanup) {
            if preview?.mode == .discard { TextField("Type \(environment.name)", text: $typedName) }
            Button("Cancel", role: .cancel) {}
            Button(preview?.mode == .discard ? "Discard" : "Reset", role: .destructive) {
                if let preview { Task { await model.executeCleanup(preview) } }
            }
            .disabled(preview?.mode == .discard && typedName != environment.name)
        } message: {
            Text(preview?.impacts.joined(separator: "\n") ?? "Loading impact…")
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

    private func prepare(_ mode: CleanupMode) {
        Task {
            do {
                preview = try await model.previewCleanup(environmentID: environment.id, mode: mode)
                typedName = ""
                showingCleanup = true
            } catch {
                model.errorMessage = hostErrorMessage(error)
            }
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
