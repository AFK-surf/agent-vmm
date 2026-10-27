import AppKit
import Charts
import SwiftUI

struct EnvironmentContainersView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var selection: Workload.ID?
    @State private var mutatingIDs: Set<Workload.ID> = []
    @State private var pendingConfirmation: ContainerConfirmation?

    var body: some View {
        VStack(alignment: .leading, spacing: 12) {
            HStack {
                Text("Containers").font(.headline)
                Spacer()
                Text("\(environment.workloads.filter { $0.state == .running }.count) running · \(environment.workloads.count) total").foregroundStyle(.secondary)
            }
            if environment.inventoryAvailability == .unknown {
                ProgressView("Loading container inventory…").frame(maxWidth: .infinity, maxHeight: .infinity)
            } else if environment.inventoryAvailability != .available && environment.workloads.isEmpty {
                ContentUnavailableView("Container inventory unavailable", systemImage: "shippingbox", description: Text(environment.inventoryMessage ?? "Try again after the runtime is available."))
            } else {
                Table(environment.workloads, selection: $selection) {
                    TableColumn("Name", value: \.id)
                    TableColumn("State") { workload in Label(workload.state.rawValue.capitalized, systemImage: workload.state == .running ? "play.fill" : "stop.fill") }
                    TableColumn("Image", value: \.image)
                    TableColumn("Started") { workload in Text(workload.startedAt?.formatted(date: .abbreviated, time: .shortened) ?? "—") }
                    TableColumn("Exit") { Text($0.exitCode.map(String.init) ?? "—") }
                    TableColumn("Actions") { workload in
                        HStack(spacing: 6) {
                            primaryAction(for: workload)
                            Menu { actionMenu(for: workload) } label: { Image(systemName: "ellipsis") }
                                .menuStyle(.borderlessButton)
                                .frame(width: 28)
                                .accessibilityLabel("More actions for \(workload.id)")
                        }
                    }
                    .width(min: 115, ideal: 130)
                }
                .contextMenu(forSelectionType: Workload.ID.self) { ids in
                    if let id = ids.first, let workload = environment.workloads.first(where: { $0.id == id }) {
                        actionMenu(for: workload)
                    }
                } primaryAction: { _ in }
                .overlay {
                    if environment.workloads.isEmpty {
                        ContentUnavailableView("No containers", systemImage: "shippingbox", description: Text("Containers created in this environment will appear here."))
                    }
                }
                if environment.inventoryAvailability == .stale {
                    Label(environment.inventoryMessage ?? "Showing the last available container inventory.", systemImage: "clock.arrow.circlepath")
                        .font(.caption).foregroundStyle(.secondary)
                }
                if let selected = selectedWorkload { workloadDetails(selected) }
            }
        }
        .padding(20)
        .confirmationDialog(confirmationTitle, isPresented: confirmationPresented, titleVisibility: .visible) {
            if let confirmation = pendingConfirmation {
                Button(confirmation.buttonTitle, role: .destructive) {
                    confirm(confirmation)
                }
                Button("Cancel", role: .cancel) { pendingConfirmation = nil }
            }
        } message: { Text(confirmationMessage) }
    }

    private var selectedWorkload: Workload? {
        selection.flatMap { id in environment.workloads.first { $0.id == id } }
    }

    @ViewBuilder private func primaryAction(for workload: Workload) -> some View {
        if mutatingIDs.contains(workload.id) {
            ProgressView().controlSize(.small).frame(width: 58)
        } else if workload.state == .running {
            Button("Stop") { mutate(workload, start: false) }
        } else if workload.state == .stopped || workload.state == .created {
            Button("Start") { mutate(workload, start: true) }
        }
    }

    @ViewBuilder private func actionMenu(for workload: Workload) -> some View {
        Button("View Details") { selection = workload.id }
        if mutatingIDs.contains(workload.id) {
            Text("Operation in progress…")
        } else {
        Divider()
        if workload.state == .running {
            Button("Stop") { mutate(workload, start: false) }
            Button("Interrupt") { signal(workload, .interrupt) }
            Button("Force Stop…") { pendingConfirmation = .forceStop(workload) }
        } else if workload.state == .stopped || workload.state == .created {
            Button("Start") { mutate(workload, start: true) }
            Divider()
            Button("Delete Container…", role: .destructive) { pendingConfirmation = .delete(workload) }
                .disabled(!canDelete(workload))
        }
        }
    }

    private func workloadDetails(_ workload: Workload) -> some View {
        GroupBox("Container details") {
            Grid(alignment: .leading, horizontalSpacing: 14, verticalSpacing: 8) {
                detailRow("Name", workload.id)
                detailRow("Image", workload.image)
                detailRow("Command", workload.argv.isEmpty ? "Image default" : workload.argv.joined(separator: " "))
                detailRow("Working directory", workload.workdir ?? "Image default")
                detailRow("User", workload.user ?? "Image default")
                detailRow("Environment", workload.environmentKeys.isEmpty ? "None" : workload.environmentKeys.joined(separator: ", "))
                detailRow("Volumes", workload.mounts.isEmpty ? "None" : workload.mounts.joined(separator: ", "))
            }
            .frame(maxWidth: .infinity, alignment: .leading)
            .padding(.top, 4)
        }
    }

    private func detailRow(_ label: String, _ value: String) -> some View {
        GridRow { Text(label).foregroundStyle(.secondary); Text(value).textSelection(.enabled) }
    }

    private func canDelete(_ workload: Workload) -> Bool {
        environment.inventoryAvailability == .available && !workload.generationID.isEmpty && (workload.state == .stopped || workload.state == .created)
    }

    private func mutate(_ workload: Workload, start: Bool) {
        mutatingIDs.insert(workload.id)
        Task {
            if start { await model.continueWorkload(environmentID: environment.id, containerID: workload.id) }
            else { await model.stop(environmentID: environment.id, containerID: workload.id) }
            mutatingIDs.remove(workload.id)
        }
    }

    private func signal(_ workload: Workload, _ signal: ContainerSignal) {
        mutatingIDs.insert(workload.id)
        Task {
            await model.signal(environmentID: environment.id, containerID: workload.id, signal: signal)
            mutatingIDs.remove(workload.id)
        }
    }

    private var confirmationPresented: Binding<Bool> {
        Binding(get: { pendingConfirmation != nil }, set: { if !$0 { pendingConfirmation = nil } })
    }
    private var confirmationTitle: String { pendingConfirmation?.title ?? "Confirm container action" }
    private var confirmationMessage: String { pendingConfirmation?.message ?? "" }
    private func confirm(_ confirmation: ContainerConfirmation) {
        pendingConfirmation = nil
        switch confirmation {
        case .forceStop(let workload): signal(workload, .kill)
        case .delete(let workload):
            guard canDelete(workload) else { return }
            mutatingIDs.insert(workload.id)
            Task {
                await model.deleteWorkload(environmentID: environment.id, containerID: workload.id, expectedGenerationID: workload.generationID)
                mutatingIDs.remove(workload.id)
                if selection == workload.id { selection = nil }
            }
        }
    }
}

private enum ContainerConfirmation {
    case forceStop(Workload)
    case delete(Workload)

    var title: String { switch self { case .forceStop(let value): "Force stop \(value.id)?"; case .delete(let value): "Delete \(value.id)?" } }
    var message: String { switch self {
    case .forceStop: "The container process will be killed immediately and may not finish writing data."
    case .delete: "This permanently deletes the container's writable snapshot and logs. Named volumes and image content are retained."
    } }
    var buttonTitle: String { switch self { case .forceStop: "Force Stop"; case .delete: "Delete Container" } }
}
