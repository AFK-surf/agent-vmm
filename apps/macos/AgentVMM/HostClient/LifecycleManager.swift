import Foundation

struct LifecycleStatus: Sendable, Equatable, Decodable {
    let appInstalled: Bool
    let hostInstalled: Bool
	let hostLoaded: Bool

    var coreHealthy: Bool { appInstalled && hostInstalled && hostLoaded }
}

enum LifecycleManager {
    enum LifecycleError: LocalizedError {
        case helperUnavailable
        case failed(String)

        var errorDescription: String? {
            switch self {
            case .helperUnavailable: "The lifecycle helper is unavailable. Reinstall Agent VMM from its signed package."
            case .failed(let message): message.isEmpty ? "The lifecycle operation failed." : message
            }
        }
    }

    static func status() throws -> LifecycleStatus { try JSONDecoder().decode(LifecycleStatus.self, from: run(["status"])) }

    static func repair() throws {
        _ = try run(["repair"])
    }

    static func uninstall(purge: Bool) throws {
        _ = try run(purge ? ["uninstall", "--purge"] : ["uninstall"])
    }

    private static func run(_ arguments: [String]) throws -> Data {
        guard let helper = BundledHelper.executable(named: "agent-vmm-lifecycle") else {
            throw LifecycleError.helperUnavailable
        }
        let process = Process()
        let output = Pipe()
        let errors = Pipe()
        process.executableURL = helper
        process.arguments = arguments
        process.standardOutput = output
        process.standardError = errors
        try process.run()
        process.waitUntilExit()
        let outputData = output.fileHandleForReading.readDataToEndOfFile()
        let errorData = errors.fileHandleForReading.readDataToEndOfFile()
        guard process.terminationStatus == 0 else {
            throw LifecycleError.failed(String(decoding: errorData, as: UTF8.self).trimmingCharacters(in: .whitespacesAndNewlines))
        }
        return outputData
    }
}
