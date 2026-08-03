import SwiftUI

struct EnvironmentListView: View {
    @Bindable var model: AppModel

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
                NavigationSplitView {
                    List(model.snapshot.environments, selection: $model.selectedEnvironmentID) { environment in
                        VStack(alignment: .leading, spacing: 5) {
                            Text(environment.name).font(.headline)
                            HStack {
                                HealthLabel(level: environment.health)
                                Text("\(environment.workloads.filter { $0.state == .running }.count) running")
                            }
                            .font(.caption)
                        }
                        .tag(environment.id)
                        .padding(.vertical, 3)
                    }
                    .navigationTitle("Environments")
                    .navigationSplitViewColumnWidth(min: 220, ideal: 260, max: 320)
                } detail: {
                    if let id = model.selectedEnvironmentID, let environment = model.environment(id: id) {
                        EnvironmentDetailView(model: model, environment: environment)
                    } else {
                        ContentUnavailableView("Choose an agent environment", systemImage: "shippingbox")
                    }
                }
            }
        }
        .navigationTitle("Environments")
        .onAppear { selectFirstEnvironmentIfNeeded() }
        .onChange(of: model.snapshot.environments) { _, _ in selectFirstEnvironmentIfNeeded() }
    }

    private var emptyStateDescription: String {
        switch model.snapshot.vmState {
        case "Stopped": "Start the virtual machine to make environments available to agents."
        case "Starting": "Environments will appear after the virtual machine finishes starting."
        default: "Environments appear here when a local agent creates a session."
        }
    }

    private func selectFirstEnvironmentIfNeeded() {
        guard model.selectedEnvironmentID == nil else { return }
        model.selectedEnvironmentID = model.snapshot.environments.first?.id
    }
}

struct EnvironmentDetailView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    var body: some View {
        TabView {
            overview.tabItem { Label("Overview", systemImage: "rectangle.grid.2x2") }
            workloads.tabItem { Label("Workspaces & Services", systemImage: "shippingbox") }
            LogsActivityView(model: model, environment: environment).tabItem { Label("Logs & Activity", systemImage: "text.alignleft") }
            data.tabItem { Label("Data", systemImage: "externaldrive") }
            network.tabItem { Label("Network & Access", systemImage: "network") }
            EnvironmentActionsView(model: model, environment: environment).tabItem { Label("Advanced", systemImage: "gearshape.2") }
        }.padding()
    }

    private var overview: some View {
        Form {
            LabeledContent("Status") { HealthLabel(level: environment.health) }
            LabeledContent("Memory", value: environment.usage.map { DisplayFormatting.bytes.string(fromByteCount: Int64($0.memoryBytes)) } ?? "Unavailable")
            LabeledContent("Retained data", value: environment.usage.map { DisplayFormatting.bytes.string(fromByteCount: Int64($0.diskBytes)) } ?? "Unavailable")
            Text("Stopping preserves each workspace writable layer. Deleting a workspace removes that layer. Named retained data is independent.")
        }
    }

    private var workloads: some View {
        Table(environment.workloads) {
            TableColumn("Workspace or service", value: \.id)
            TableColumn("State") { Text($0.state.rawValue.capitalized) }
            TableColumn("Exit") { Text($0.exitCode.map(String.init) ?? "—") }
            TableColumn("Image", value: \.image)
        }
    }

    private var data: some View {
        Table(environment.volumes) {
            TableColumn("Retained data", value: \.id)
            TableColumn("Used") { Text(DisplayFormatting.bytes.string(fromByteCount: Int64($0.bytesUsed))) }
            TableColumn("Mounted by") { Text($0.mountedBy.joined(separator: ", ")) }
        }
    }

    private var network: some View { Form { LabeledContent("Outbound access", value: environment.egressSummary) } }
}
