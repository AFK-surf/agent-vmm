import CryptoKit
import Foundation

struct AgentIntegrationConfiguration: Sendable {
    enum ConfigurationError: LocalizedError {
        case codexUnavailable
        case helperUnavailable
        case commandFailed
        case commandTimedOut
        case configurationChanged

        var errorDescription: String? {
            switch self {
            case .codexUnavailable: "Codex CLI was not found in a standard installation location."
            case .helperUnavailable: "The Agent VMM MCP helper is missing from the application bundle."
            case .commandFailed: "The MCP client configuration command failed."
            case .commandTimedOut: "The MCP client configuration command did not finish in time. Try again."
            case .configurationChanged: "The MCP configuration changed outside Agent VMM. Review it before removing access."
            }
        }
    }

    func installCodex(integrationID: String) async throws -> String {
        let executable = try codexExecutable()
        let helper = try mcpHelper()
        try backupCodexConfiguration()
        _ = try await run(executable, arguments: ["mcp", "add", serverName(integrationID), "--", helper.path, "--integration", integrationID])
        return try await fingerprint(executable: executable, integrationID: integrationID)
    }

    func removeCodex(integrationID: String, expectedFingerprint: String) async throws {
        let executable = try codexExecutable()
        if expectedFingerprint.isEmpty {
            throw ConfigurationError.configurationChanged
        } else if try await fingerprint(executable: executable, integrationID: integrationID) != expectedFingerprint {
            throw ConfigurationError.configurationChanged
        }
        try backupCodexConfiguration()
        _ = try await run(executable, arguments: ["mcp", "remove", serverName(integrationID)])
    }

    func currentCodexConfiguration(integrationID: String) async throws -> String {
        let output = try await run(try codexExecutable(), arguments: ["mcp", "get", serverName(integrationID)])
        return String(decoding: output, as: UTF8.self)
    }

    func codexFingerprint(integrationID: String) async throws -> String {
        try await fingerprint(executable: codexExecutable(), integrationID: integrationID)
    }

    func repairCodex(integrationID: String) async throws -> String {
        let executable = try codexExecutable()
        let helper = try mcpHelper()
        try backupCodexConfiguration()
        _ = try? await run(executable, arguments: ["mcp", "remove", serverName(integrationID)])
        _ = try await run(executable, arguments: ["mcp", "add", serverName(integrationID), "--", helper.path, "--integration", integrationID])
        return try await fingerprint(executable: executable, integrationID: integrationID)
    }

    func manualCommand(integrationID: String) throws -> String {
        let path = try mcpHelper().path.replacingOccurrences(of: "'", with: "'\\''")
        return "'\(path)' --integration \(integrationID)"
    }

    func codexConfigTarget() -> String {
        FileManager.default.homeDirectoryForCurrentUser.appending(path: ".codex/config.toml").path
    }

    func canConfigureCodex() -> Bool {
        (try? codexExecutable()) != nil
    }

    func backupConfiguration(at target: URL) throws {
        guard FileManager.default.fileExists(atPath: target.path) else { return }
        let backup = target.appendingPathExtension("agent-vmm-backup")
        try Data(contentsOf: target).write(to: backup, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: backup.path)
    }

    private func backupCodexConfiguration() throws {
        try backupConfiguration(at: FileManager.default.homeDirectoryForCurrentUser.appending(path: ".codex/config.toml"))
    }

    private func fingerprint(executable: URL, integrationID: String) async throws -> String {
        let output = try await run(executable, arguments: ["mcp", "get", serverName(integrationID)])
        return SHA256.hash(data: output).map { String(format: "%02x", $0) }.joined()
    }

    private func serverName(_ integrationID: String) -> String {
        "agent-vmm-\(integrationID.prefix(8))"
    }

    private func mcpHelper() throws -> URL {
        guard let helper = BundledHelper.executable(named: "agent-vmm-mcp") else {
            throw ConfigurationError.helperUnavailable
        }
        return helper
    }

    private func codexExecutable() throws -> URL {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let candidates = [
            URL(filePath: "/opt/homebrew/bin/codex"),
            URL(filePath: "/usr/local/bin/codex"),
            home.appending(path: ".local/bin/codex")
        ]
        guard let executable = candidates.first(where: { FileManager.default.isExecutableFile(atPath: $0.path) }) else {
            throw ConfigurationError.codexUnavailable
        }
        return executable
    }

    func run(_ executable: URL, arguments: [String], timeout: Duration = .seconds(30)) async throws -> Data {
        try await Task.detached {
            let directory = FileManager.default.temporaryDirectory.appending(path: "agent-vmm-command-\(UUID().uuidString)")
            try FileManager.default.createDirectory(
                at: directory, withIntermediateDirectories: false,
                attributes: [.posixPermissions: 0o700]
            )
            defer { try? FileManager.default.removeItem(at: directory) }
            let outputURL = directory.appending(path: "stdout")
            let errorURL = directory.appending(path: "stderr")
            FileManager.default.createFile(atPath: outputURL.path, contents: nil, attributes: [.posixPermissions: 0o600])
            FileManager.default.createFile(atPath: errorURL.path, contents: nil, attributes: [.posixPermissions: 0o600])
            let output = try FileHandle(forWritingTo: outputURL)
            let errors = try FileHandle(forWritingTo: errorURL)
            defer {
                try? output.close()
                try? errors.close()
            }
            let process = Process()
            defer { if process.isRunning { process.terminate() } }
            process.executableURL = executable
            process.arguments = arguments
            process.standardOutput = output
            process.standardError = errors
            try process.run()
            let deadline = ContinuousClock.now.advanced(by: timeout)
            while process.isRunning && ContinuousClock.now < deadline {
                try await Task.sleep(for: .milliseconds(50))
            }
            if process.isRunning {
                process.terminate()
                process.waitUntilExit()
                throw ConfigurationError.commandTimedOut
            }
            process.waitUntilExit()
            try output.close()
            try errors.close()
            let data = try Data(contentsOf: outputURL)
            guard process.terminationStatus == 0 else {
                throw ConfigurationError.commandFailed
            }
            return data
        }.value
    }
}
