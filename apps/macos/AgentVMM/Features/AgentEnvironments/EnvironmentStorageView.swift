import AppKit
import Charts
import SwiftUI

struct EnvironmentStorageView: View {
    @Bindable var model: AppModel
    let environment: AgentEnvironmentModel
    @State private var images: [EnvironmentImageModel] = []
    @State private var imageError: String?
    @State private var loadingImages = false
    @State private var imageReloadID = UUID()
    @State private var imagePendingRemoval: EnvironmentImageModel?
    @State private var removingImageIDs: Set<String> = []
    @State private var failedImageRemoval: EnvironmentImageModel?
    @State private var imageRemovalError: String?
    @State private var imageInventoryFresh = false

    var body: some View {
        VStack(alignment: .leading, spacing: 16) {
            HStack {
                Text("Images available to this environment").font(.headline)
                Text("Environment namespace").font(.caption).padding(.horizontal, 8).padding(.vertical, 3)
                    .background(.quaternary, in: Capsule())
                Spacer()
                Button { imageReloadID = UUID() } label: { Label("Refresh", systemImage: "arrow.clockwise") }
                    .disabled(loadingImages)
            }
            Text("Image records belong to this environment. Content blobs are stored and deduplicated across the virtual machine, so removing a record may not free disk space.")
                .font(.caption).foregroundStyle(.secondary)
            if let imageError, !images.isEmpty {
                Label(imageError, systemImage: "exclamationmark.triangle")
                    .font(.caption).foregroundStyle(.red)
            }
            if let imageRemovalError {
                HStack {
                    Label(imageRemovalError, systemImage: "exclamationmark.triangle")
                        .font(.caption).foregroundStyle(.red)
                    Spacer()
                    if let failedImageRemoval, !imageRemovalDisabled(failedImageRemoval) {
                        Button("Retry") { remove(failedImageRemoval) }
                    }
                    Button("Dismiss") {
                        self.imageRemovalError = nil
                        failedImageRemoval = nil
                    }
                }
            }
            Table(images) {
                TableColumn("Reference", value: \.reference)
                TableColumn("Content") { Text(shortDigest($0.digest)).font(.system(.body, design: .monospaced)) }
                TableColumn("Logical size") { Text(DisplayFormatting.bytes.string(fromByteCount: Int64($0.logicalBytes))) }
                TableColumn("Used by") { Text($0.usedByContainerIDs.isEmpty ? "Not in use" : $0.usedByContainerIDs.joined(separator: ", ")) }
                TableColumn("") { image in
                    Menu {
                        Button("Copy Reference") { copy(image.reference) }
                        Button("Copy Digest") { copy(image.digest) }
                        Divider()
                        Button("Remove from Environment…", role: .destructive) { imagePendingRemoval = image }
                            .disabled(imageRemovalDisabled(image))
                    } label: {
                        if removingImageIDs.contains(image.id) { ProgressView().controlSize(.small) }
                        else { Image(systemName: "ellipsis.circle") }
                    }
                    .menuStyle(.borderlessButton)
                    .help(imageRemovalHelp(image))
                }.width(38)
            }
            .overlay {
                if loadingImages || environment.inventoryAvailability == .unknown { ProgressView("Loading images…") }
                else if let imageError, images.isEmpty {
                    ContentUnavailableView {
                        Label("Image inventory unavailable", systemImage: "photo.stack")
                    } description: { Text(imageError) } actions: { Button("Retry") { imageReloadID = UUID() } }
                }
                else if environment.inventoryAvailability == .stale && images.isEmpty {
                    ContentUnavailableView("Last image inventory unavailable", systemImage: "clock.arrow.circlepath", description: Text(environment.inventoryMessage ?? "Refresh after the runtime is available."))
                }
                else if environment.inventoryAvailability == .available && images.isEmpty && imageError == nil { ContentUnavailableView("No images", systemImage: "photo.stack") }
            }
            Text("Retained volumes").font(.headline)
            Table(environment.volumes) {
                TableColumn("Name", value: \.id)
                TableColumn("Used") { volume in
                    Text(volume.usageIsStale ? (volume.usageUnavailableReason ?? "Sampling…") : DisplayFormatting.bytes.string(fromByteCount: Int64(volume.bytesUsed)))
                }
                TableColumn("Mounted by") { Text($0.mountedBy.isEmpty ? "Not mounted" : $0.mountedBy.joined(separator: ", ")) }
            }
            .frame(minHeight: 150, idealHeight: 210)
            .overlay {
                if environment.inventoryAvailability == .unknown {
                    ProgressView("Loading retained volumes…")
                } else if environment.inventoryAvailability != .available && environment.volumes.isEmpty {
                    ContentUnavailableView("Volume inventory unavailable", systemImage: "externaldrive", description: Text(environment.inventoryMessage ?? "Try again after the runtime is available."))
                } else if environment.volumes.isEmpty {
                    ContentUnavailableView("No retained volumes", systemImage: "externaldrive")
                }
            }
            if environment.inventoryAvailability == .stale {
                HStack {
                    Label(staleInventoryMessage, systemImage: "clock.arrow.circlepath")
                        .font(.caption).foregroundStyle(.secondary)
                    Spacer()
                    Button("Retry") { Task { await loadImages(force: true) } }
                }
            }
        }
        .padding(20)
        .task(id: "\(environment.id):\(environment.inventoryAvailability):\(model.snapshot.bootID):\(imageReloadID)") { await loadImages(force: false) }
        .alert("Remove image record from this environment?", isPresented: removalAlertPresented, presenting: imagePendingRemoval) { image in
            Button("Cancel", role: .cancel) { imagePendingRemoval = nil }
            Button("Remove Image Record", role: .destructive) { remove(image) }
        } message: { _ in
            Text("This removes the reference from this environment’s namespace. Containers and volumes are not deleted. The underlying content may remain because it can be shared elsewhere in the virtual machine. Reclaimed disk space is unknown.")
        }
    }

    private var staleInventoryMessage: String {
        let observed = environment.inventoryObservedAt.map { " Last updated \($0.formatted(date: .abbreviated, time: .shortened))." } ?? ""
        return (environment.inventoryMessage ?? "Showing the last available storage inventory.") + observed
    }

    private func loadImages(force: Bool) async {
        switch imageInventoryPlan(availability: environment.inventoryAvailability, force: force) {
        case .wait:
            loadingImages = true
            imageInventoryFresh = false
            imageError = nil
            return
        case .retain:
            loadingImages = false
            imageInventoryFresh = false
            return
        case .unavailable:
            loadingImages = false
            imageInventoryFresh = false
            images.removeAll()
            imageError = environment.inventoryMessage ?? (environment.inventoryAvailability == .vmStopped ? "Start the virtual machine to inspect images." : "The runtime image inventory is unavailable.")
            return
        case .fetch:
            break
        }
        guard let client = model.client else {
            loadingImages = false
            imageError = hostErrorMessage(HostClientError.disconnected)
            return
        }
        loadingImages = true
        imageInventoryFresh = false
        defer { loadingImages = false }
        do {
            images = try await client.images(environmentID: environment.id)
            imageError = nil
            imageInventoryFresh = true
            if let failedImageRemoval { self.failedImageRemoval = images.first { $0.id == failedImageRemoval.id } }
        } catch {
            imageError = hostErrorMessage(error)
            imageInventoryFresh = false
        }
    }
    private var removalAlertPresented: Binding<Bool> {
        Binding(get: { imagePendingRemoval != nil }, set: { if !$0 { imagePendingRemoval = nil } })
    }
    private func imageRemovalDisabled(_ image: EnvironmentImageModel) -> Bool {
        imageRemovalAvailability(for: image, inventory: effectiveImageInventory, isRemoving: removingImageIDs.contains(image.id)) != .allowed
    }
    private func imageRemovalHelp(_ image: EnvironmentImageModel) -> String {
        switch imageRemovalAvailability(for: image, inventory: effectiveImageInventory, isRemoving: removingImageIDs.contains(image.id)) {
        case .inUse(let count):
            return "Used by \(count) container\(count == 1 ? "" : "s"). Delete or replace those containers first."
        case .inventoryUnavailable:
            return "Refresh the image inventory before removing this record."
        case .inProgress:
            return "This image record is being removed."
        case .allowed:
            return "Copy image identity or remove its record from this environment."
        }
    }
    private var effectiveImageInventory: InventoryAvailability {
        environment.inventoryAvailability == .available && imageInventoryFresh && !loadingImages ? .available : .stale
    }
    private func remove(_ image: EnvironmentImageModel) {
        imagePendingRemoval = nil
        removingImageIDs.insert(image.id)
        Task {
            defer { removingImageIDs.remove(image.id) }
            do {
                try await model.deleteImage(environmentID: environment.id, image: image)
                images.removeAll { $0.id == image.id }
                imageError = nil
                imageRemovalError = nil
                failedImageRemoval = nil
            } catch {
                imageRemovalError = hostErrorMessage(error)
                failedImageRemoval = image
                imageReloadID = UUID()
            }
        }
    }
    private func copy(_ value: String) {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(value, forType: .string)
    }
    private func shortDigest(_ value: String) -> String { value.count > 20 ? String(value.prefix(20)) + "…" : value }
}
