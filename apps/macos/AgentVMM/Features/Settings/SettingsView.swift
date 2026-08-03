import SwiftUI

struct SettingsView: View {
    @Bindable var model: AppModel
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
            }
            .formStyle(.grouped)
            .frame(maxWidth: 760, alignment: .leading)
            .padding(28)
        }
        .navigationTitle("Settings")
    }
}
