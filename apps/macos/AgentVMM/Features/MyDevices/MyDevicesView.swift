import SwiftUI
import CoreImage.CIFilterBuiltins

struct MyDevicesView: View {
    @Bindable var model: AppModel
    @State private var registryAudience = "registry.agent-vmm.local"
    @State private var memberToRevoke: (mesh: PersonalMeshModel, member: PersonalMeshMemberModel)?
    @State private var confirmingRepair = false
    @State private var pairingPIN = ""
    @State private var invitationCode = ""
    @State private var grantManagerPermission = false

    var body: some View {
        Form {
            if model.deviceActionInProgress || model.deviceRefreshInProgress {
                Section {
                    ProgressView(model.deviceRefreshInProgress ? "Refreshing device state…" : "Updating device…")
                        .controlSize(.small)
                }
            }
            Section("This device") {
                if let identity = model.deviceIdentity {
                    LabeledContent("Device ID", value: identity.id)
                    LabeledContent("Root key revision", value: String(identity.rootKeyRevision))
                    LabeledContent("Identity state", value: identity.state == .ready ? "Ready" : "Needs repair")
                    if identity.state == .needsRepair {
                        Button("Repair identity", role: .destructive) { confirmingRepair = true }
                            .disabled(model.deviceActionInProgress)
                    }
                } else {
                    ProgressView("Reading device identity…")
                }
            }

            Section("Personal meshes") {
                if model.personalMeshes.isEmpty {
                    ContentUnavailableView("No personal mesh", systemImage: "network", description: Text("Create a mesh to authorize services between your devices."))
                }
                ForEach(model.personalMeshes) { mesh in
                    DisclosureGroup {
                        LabeledContent("Registry", value: mesh.registryAudience)
                        LabeledContent("Revision", value: String(mesh.revision))
                        LabeledContent("Policy epoch", value: String(mesh.policyEpoch))
                        ForEach(mesh.members) { member in
                            HStack {
                                VStack(alignment: .leading) {
                                    Text(member.deviceID).font(.body.monospaced())
                                    Text(member.permissions.joined(separator: ", ")).font(.caption).foregroundStyle(.secondary)
                                }
                                Spacer()
                                if member.deviceID != model.deviceIdentity?.id {
                                    Button("Revoke", role: .destructive) { memberToRevoke = (mesh, member) }
                                        .disabled(model.deviceActionInProgress)
                                }
                            }
                        }
                        SecureField("8-digit pairing PIN", text: $pairingPIN)
                        Toggle("Allow the invited device to manage members", isOn: $grantManagerPermission)
                        Button("Create pairing invitation") {
                            let pin = pairingPIN
                            pairingPIN = ""
                            Task { await model.beginPersonalMeshPairing(meshID: mesh.id, expectedRevision: mesh.revision, pin: pin, canManageMembers: grantManagerPermission) }
                        }
                        .disabled(model.deviceActionInProgress || pairingPIN.count != 8)
                    } label: {
                        Text(mesh.id).font(.body.monospaced())
                    }
                }
            }

            Section("Create a mesh") {
                TextField("Registry audience", text: $registryAudience)
                Button("Create personal mesh") {
                    Task { await model.createPersonalMesh(registryAudience: registryAudience.trimmingCharacters(in: .whitespacesAndNewlines)) }
                }
                .disabled(model.deviceActionInProgress || registryAudience.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
                Text("Pairing uses RFC 9382 SPAKE2-P256 with a short-lived PIN. The secret remains in the local data worker and is never sent to the registry or MCP.")
                    .font(.caption)
                    .foregroundStyle(.secondary)
            }

            Section("Join another device") {
                TextEditor(text: $invitationCode)
                    .frame(minHeight: 64)
                    .font(.caption.monospaced())
                SecureField("8-digit pairing PIN", text: $pairingPIN)
                Button("Start secure pairing") {
                    let pin = pairingPIN
                    pairingPIN = ""
                    Task {
                        await model.joinPersonalMeshPairing(
                            invitationCode: invitationCode.trimmingCharacters(in: .whitespacesAndNewlines),
                            pin: pin
                        )
                    }
                }
                .disabled(model.deviceActionInProgress || pairingPIN.count != 8 || invitationCode.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty)
            }

            if let pairing = model.personalMeshPairing {
                Section("Pairing verification") {
                    if pairing.role == "manager" {
                        PairingQRCodeView(value: pairing.invitationCode)
                            .frame(width: 180, height: 180)
                            .accessibilityLabel("Pairing invitation QR code")
                        LabeledContent("Invitation") {
                            Text(pairing.invitationCode).font(.caption.monospaced()).textSelection(.enabled)
                        }
                    }
                    LabeledContent("State", value: pairing.state.label)
                    LabeledContent("Attempts remaining", value: String(pairing.attemptsRemaining))
                    if let failure = pairing.lastFailure { Text(failure).foregroundStyle(.secondary) }
                    if !pairing.sas.isEmpty {
                        LabeledContent("Verify on both devices") {
                            Text(pairing.sas).font(.title2.monospaced().bold())
                        }
                        Text("Confirm only when this six-digit code is identical on both devices.")
                            .font(.caption)
                            .foregroundStyle(.secondary)
                    }
                    if pairing.registryCommitPending {
                        Text("Both devices confirmed. Waiting for the registry CAS snapshot before membership becomes active.")
                            .font(.caption)
                    }
                    HStack {
                        Button("Refresh") { Task { await model.refreshPersonalMeshPairing() } }
                            .disabled(model.deviceActionInProgress || model.deviceRefreshInProgress)
                        Button("Confirm codes match") { Task { await model.confirmPersonalMeshPairing() } }
                            .disabled(model.deviceActionInProgress || pairing.sas.isEmpty || pairing.state == .committed)
                        Button("Reject", role: .destructive) { Task { await model.rejectPersonalMeshPairing() } }
                            .disabled(model.deviceActionInProgress || pairing.state == .committed || pairing.state == .rejected)
                    }
                }
            }
        }
        .formStyle(.grouped)
        .navigationTitle("My Devices")
        .toolbar {
            Button("Refresh", systemImage: "arrow.clockwise") { Task { await model.refreshDevices() } }
                .disabled(model.deviceActionInProgress || model.deviceRefreshInProgress)
        }
        .confirmationDialog("Revoke this device?", isPresented: Binding(
            get: { memberToRevoke != nil },
            set: { if !$0 { memberToRevoke = nil } }
        ), titleVisibility: .visible) {
            if let pending = memberToRevoke {
                Button("Revoke \(pending.member.deviceID)", role: .destructive) {
                    memberToRevoke = nil
                    Task { await model.revokePersonalMeshMember(meshID: pending.mesh.id, deviceID: pending.member.deviceID, expectedRevision: pending.mesh.revision) }
                }
            }
        } message: {
            Text("The local deny is recorded immediately. The member becomes globally revoked only after the registry accepts the signed CAS operation.")
        }
        .confirmationDialog("Reset device membership?", isPresented: $confirmingRepair, titleVisibility: .visible) {
            Button("Repair and reset membership", role: .destructive) {
                Task { await model.repairDeviceIdentity(confirmMembershipReset: true) }
            }
        } message: {
            Text("Use this only when the protected root key is unavailable. Existing mesh membership cannot silently transfer to a new key.")
        }
    }
}

private struct PairingQRCodeView: View {
    let value: String

    var body: some View {
        if let image = qrImage(value) {
            Image(nsImage: image).interpolation(.none).resizable()
        } else {
            ContentUnavailableView("QR unavailable", systemImage: "qrcode")
        }
    }

    private func qrImage(_ value: String) -> NSImage? {
        let filter = CIFilter.qrCodeGenerator()
        filter.message = Data(value.utf8)
        filter.correctionLevel = "M"
        guard let output = filter.outputImage else { return nil }
        let scaled = output.transformed(by: CGAffineTransform(scaleX: 8, y: 8))
        let representation = NSCIImageRep(ciImage: scaled)
        let image = NSImage(size: representation.size)
        image.addRepresentation(representation)
        return image
    }
}
