import SwiftUI

@main
struct AgentVMMApp: App {
    @State private var model = AppModel()
    @State private var showingUninstall = false

    var body: some Scene {
        WindowGroup {
            RootView(model: model)
                .frame(minWidth: 1120, minHeight: 680)
                .task { await model.start() }
                .onOpenURL { url in
                    guard url.scheme == "agent-vmm", url.host == "pair",
                          let components = URLComponents(url: url, resolvingAgainstBaseURL: false),
                          let invitation = components.queryItems?.first(where: { $0.name == "invite" })?.value,
                          Data(base64Encoded: invitation) != nil else { return }
                    model.incomingPairingInvitation = invitation
                    model.section = .devices
                }
                .sheet(isPresented: $showingUninstall) { UninstallView(model: model) }
        }
        .defaultSize(width: 1280, height: 800)
        .commands {
            CommandGroup(after: .appSettings) {
                Button("Uninstall Agent VMM…") { showingUninstall = true }
            }
        }

        MenuBarExtra("Agent VMM", systemImage: model.snapshot.health.symbolName) {
            Text(model.snapshot.health.label)
            Text(DisplayFormatting.age(of: model.snapshot.collectedAt))
        }
    }
}

private struct UninstallView: View {
    @Environment(\.dismiss) private var dismiss
    @Bindable var model: AppModel
    @State private var purge = false
    @State private var showingPurgeConfirmation = false

    var body: some View {
        VStack(alignment: .leading, spacing: 18) {
            Text("Uninstall Agent VMM").font(.title2.weight(.semibold))
            Text("The app, MCP helper, connector, host service, and LaunchAgents will be removed.")
            Toggle("Also delete environments, images, logs, and other retained data", isOn: $purge)
            if purge {
                Label("This permanently deletes all Agent VMM data.", systemImage: "exclamationmark.triangle.fill")
                    .foregroundStyle(.red)
            } else {
                Text("Retained data remains in ~/Library/Application Support/AgentVMM.").font(.caption).foregroundStyle(.secondary)
            }
            HStack {
                Spacer()
                Button("Cancel", role: .cancel) { dismiss() }
                Button("Uninstall", role: .destructive) {
                    if purge { showingPurgeConfirmation = true }
                    else { uninstall() }
                }.disabled(model.lifecycleOperationInProgress)
            }
        }
        .padding(24).frame(width: 520)
        .interactiveDismissDisabled(model.lifecycleOperationInProgress)
        .alert("Permanently delete all Agent VMM data?", isPresented: $showingPurgeConfirmation) {
            Button("Cancel", role: .cancel) {}
            Button("Delete All Data and Uninstall", role: .destructive) { uninstall() }
        } message: {
            Text("All environments, images, volumes, logs, and credentials will be permanently deleted. This cannot be undone.")
        }
    }

    private func uninstall() {
        Task { if await model.uninstall(purge: purge) { NSApplication.shared.terminate(nil) } }
    }
}
