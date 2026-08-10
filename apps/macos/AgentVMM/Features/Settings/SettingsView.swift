import SwiftUI

struct SettingsView: View {
    @Bindable var model: AppModel
    @State private var endpoint = ""
    @State private var displayName = ""
    @State private var token = ""
    @State private var maxEnvironments: UInt32 = 2
    var body: some View {
        ScrollView {
            Form {
                Section("Connection") {
                    LabeledContent("Operator socket", value: AppModel.defaultSocketPath)
                    LabeledContent("VM boot ID", value: model.snapshot.bootID.isEmpty ? "Unavailable" : model.snapshot.bootID)
                }
                Section("Advanced access") {
                    Text("Interactive terminal access is not enabled in this release. Use an authenticated agent session for controlled exec access.")
                    Text("The app never opens the guest runtime socket or state disk directly.")
                }
                Section("Remote controller") {
                    if let registration = model.snapshot.remoteRegistration {
                        LabeledContent("Controller", value: registration.displayName)
                        LabeledContent("Endpoint", value: registration.endpoint)
                        LabeledContent("Connection", value: registration.connectionState)
                        LabeledContent("Active leases", value: String(registration.activeLeases))
                        Stepper("Pool environments: \(maxEnvironments)", value: $maxEnvironments, in: 1...128)
                        HStack {
                            Button("Update pool") { Task { await model.updateRemotePool(pool) } }
                            Button("Enable") { Task { await model.setRemoteState(.enabled) } }
                            Button("Drain") { Task { await model.setRemoteState(.draining) } }
                            Button("Revoke", role: .destructive) { Task { await model.setRemoteState(.revoked) } }
                        }
                    } else {
                        TextField("controller.example:443", text: $endpoint)
                        TextField("Controller name", text: $displayName)
                        SecureField("One-time enrollment token", text: $token)
                        Stepper("Pool environments: \(maxEnvironments)", value: $maxEnvironments, in: 1...128)
                        Button("Enroll") {
                            let oneTimeToken = token
                            token = ""
                            Task { await model.enrollRemote(endpoint: endpoint, displayName: displayName, token: oneTimeToken, pool: pool) }
                        }
                        .disabled(endpoint.isEmpty || displayName.isEmpty || token.isEmpty)
                    }
                    Text("Credentials are stored by the connector in Keychain and never appear in snapshots.")
                        .font(.caption).foregroundStyle(.secondary)
                }
            }
            .formStyle(.grouped)
            .frame(maxWidth: 760, alignment: .leading)
            .padding(28)
        }
        .navigationTitle("Settings")
    }

    private var pool: RemotePoolSettings {
        RemotePoolSettings(maxEnvironments: maxEnvironments, cpuMillis: 1000, memoryBytes: 1 << 30, pids: 256, diskBytes: 2 << 30)
    }
}
