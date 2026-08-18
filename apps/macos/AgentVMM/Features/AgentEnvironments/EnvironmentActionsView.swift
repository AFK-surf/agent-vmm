import AppKit
import SwiftUI

struct EnvironmentActionsView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    let onReset: () -> Void
    let onDiscard: () -> Void
    let resetDisabledReason: String?
    let discardDisabledReason: String?

    @State private var cpuMillis = ""
    @State private var memoryMiB = ""
    @State private var pids = ""
    @State private var diskGiB = ""

    var body: some View {
        ScrollView {
            VStack(alignment: .leading, spacing: 18) {
                GroupBox("Resource limits") {
                    Grid(alignment: .leading, horizontalSpacing: 14, verticalSpacing: 10) {
                        policyField("CPU millicores", text: $cpuMillis)
                        policyField("Memory MiB", text: $memoryMiB)
                        policyField("Process limit", text: $pids)
                        policyField("Retained disk GiB", text: $diskGiB)
                        GridRow {
                            Text("")
                            HStack {
                                if model.policyActionInProgress {
                                    ProgressView("Saving limits…")
                                        .controlSize(.small)
                                }
                                Button("Save Limits") { applyPolicy() }.disabled(parsedLimits == nil || !limitsChanged || model.policyActionInProgress)
                                Button("Revert") { loadPolicy() }.disabled(!limitsChanged || model.policyActionInProgress)
                            }
                        }
                    }
                    .padding(.top, 6)
                    Text("Concurrent changes are rejected. Refresh before retrying if policy revision \(environment.policyRevision) changed.")
                        .font(.caption).foregroundStyle(.secondary).padding(.top, 8)
                }

                DisclosureGroup("Technical details") {
                    VStack(alignment: .leading, spacing: 10) {
                        copyableRow("Environment ID", environment.id)
                        copyableRow("Namespace ID", environment.namespaceID)
                        LabeledContent("Policy revision", value: String(environment.policyRevision))
                        if let allocationID = environment.allocationID { copyableRow("Allocation ID", allocationID) }
                    }
                    .padding(.top, 8)
                }
                .padding(12)
                .background(.quaternary.opacity(0.45), in: RoundedRectangle(cornerRadius: 10))

                GroupBox("Operations") {
                    HStack {
                        Text("Cleanup history and failure details are shown in Activity.").foregroundStyle(.secondary)
                        Spacer()
                        Button("Open Activity") { model.section = .diagnostics }
                    }
                    .padding(.top, 6)
                }

                GroupBox("Danger Zone") {
                    VStack(alignment: .leading, spacing: 12) {
                        dangerAction(
                            title: "Reset environment",
                            detail: "Deletes container writable layers and logs, but keeps named retained volumes.",
                            button: "Reset Environment…",
                            disabledReason: resetDisabledReason,
                            action: onReset
                        )
                        Divider()
                        dangerAction(
                            title: "Delete environment",
                            detail: "Permanently deletes the environment and the retained data listed in the confirmation.",
                            button: "Delete Environment…",
                            disabledReason: discardDisabledReason,
                            role: .destructive,
                            action: onDiscard
                        )
                    }
                    .padding(.top, 6)
                }
            }
            .frame(maxWidth: 760, alignment: .leading)
            .padding(24)
        }
        .onAppear { loadPolicy() }
        .onChange(of: environment.policyRevision) { _, _ in loadPolicy() }
    }

    @ViewBuilder private func policyField(_ label: String, text: Binding<String>) -> some View {
        GridRow {
            Text(label).foregroundStyle(.secondary)
            TextField(label, text: text).labelsHidden().textFieldStyle(.roundedBorder).frame(maxWidth: 240)
        }
    }

    private func copyableRow(_ label: String, _ value: String) -> some View {
        LabeledContent(label) {
            HStack {
                Text(value).font(.system(.body, design: .monospaced)).textSelection(.enabled)
                Button { NSPasteboard.general.clearContents(); NSPasteboard.general.setString(value, forType: .string) } label: {
                    Image(systemName: "doc.on.doc")
                }
                .buttonStyle(.plain).accessibilityLabel("Copy \(label)")
            }
        }
    }

    private func dangerAction(title: String, detail: String, button: String, disabledReason: String?, role: ButtonRole? = nil, action: @escaping () -> Void) -> some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack {
                VStack(alignment: .leading, spacing: 3) {
                    Text(title).font(.headline)
                    Text(disabledReason ?? detail).font(.caption).foregroundStyle(disabledReason == nil ? Color.secondary : Color.orange)
                }
                Spacer()
                Button(button, role: role, action: action).disabled(disabledReason != nil).help(disabledReason ?? detail)
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

    private var limitsChanged: Bool { parsedLimits.map { $0 != environment.limits } ?? false }

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
    }

    private func applyPolicy() {
        guard let limits = parsedLimits else { return }
        Task {
            await model.updatePolicy(
                environmentID: environment.id,
                limits: limits,
                allowsPublicEgress: environment.allowsPublicEgress,
                expectedRevision: environment.policyRevision
            )
        }
    }
}
