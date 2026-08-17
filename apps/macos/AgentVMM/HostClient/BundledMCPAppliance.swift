import Foundation

enum BundledMCPAppliance {
    static let installationID = "agent-vmm-mcp"
    static let frontendName = "mcp-http"

    enum ArtifactError: LocalizedError {
        case missing(String)
        var errorDescription: String? {
            switch self {
            case .missing(let name): "The bundled MCP appliance is missing \(name). Reinstall Agent VMM."
            }
        }
    }

    static func loadArtifact(bundle: Bundle = .main) throws -> BundledApplianceArtifact {
        let directory = bundle.resourceURL?.appending(path: "Guest/mcp-appliance")
        guard let directory else { throw ArtifactError.missing("Guest/mcp-appliance") }
        let manifest = try requiredData(directory.appending(path: "manifest.pb"))
        let signature = try requiredData(directory.appending(path: "manifest.sig"))
        let decoded = try Agentvmm_Appliance_V1_ControllerApplianceManifest(serializedBytes: manifest)
        return BundledApplianceArtifact(
            manifest: manifest,
            signature: signature,
            requestedFrontends: decoded.requestedFrontends.map {
                ApplianceFrontendGrantModel(
                    name: $0.name,
                    transport: String(describing: $0.transport).replacingOccurrences(of: "_", with: " ").capitalized,
                    exposure: String(describing: $0.exposure).replacingOccurrences(of: "_", with: " ").capitalized
                )
            },
            requestsDynamicHostLoopback: false
        )
    }

    static func endpointFile(supportDirectory: URL = defaultSupportDirectory) -> URL {
        supportDirectory.appending(path: "appliance-frontends/\(installationID)/\(frontendName).endpoint")
    }

    static var defaultSupportDirectory: URL {
        FileManager.default.homeDirectoryForCurrentUser.appending(path: "Library/Application Support/AgentVMM")
    }

    private static func requiredData(_ url: URL) throws -> Data {
        guard let data = try? Data(contentsOf: url), !data.isEmpty else { throw ArtifactError.missing(url.lastPathComponent) }
        return data
    }
}
