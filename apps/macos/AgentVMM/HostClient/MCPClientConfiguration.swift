import Foundation

struct MCPClientBindingModel: Identifiable, Codable, Sendable, Equatable {
    enum Kind: String, Codable, Sendable { case codex, manual }
    let id: UUID
    let kind: Kind
    var displayName: String
    let binding: String
}

struct MCPClientConfiguration: Sendable {
    enum ConfigurationError: LocalizedError {
        case codexUnavailable, endpointUnavailable, commandFailed, commandTimedOut

        var errorDescription: String? {
            switch self {
            case .codexUnavailable: "Codex CLI was not found in a standard installation location."
            case .endpointUnavailable: "The Agent VMM HTTP MCP endpoint is not available. Enable MCP and try again."
            case .commandFailed: "The MCP client configuration command failed."
            case .commandTimedOut: "The MCP client configuration command did not finish in time. Try again."
            }
        }
    }

    func bindings() throws -> [MCPClientBindingModel] {
        guard FileManager.default.fileExists(atPath: storeURL.path) else { return [] }
        return try JSONDecoder().decode([MCPClientBindingModel].self, from: Data(contentsOf: storeURL))
    }

    func installCodex() async throws {
        let executable = try codexExecutable()
        var values = try bindings()
        let client: MCPClientBindingModel
        if let existing = values.first(where: { $0.kind == .codex }) { client = existing }
        else {
            client = MCPClientBindingModel(id: UUID(), kind: .codex, displayName: "Codex", binding: Self.newBinding())
            values.append(client)
            try save(values)
        }
        _ = try? await run(executable, arguments: ["mcp", "remove", "agent-vmm"])
        _ = try await run(executable, arguments: ["mcp", "add", "agent-vmm", "--url", try endpointURL(binding: client.binding).absoluteString])
    }

    func removeCodex() async throws {
        _ = try await run(try codexExecutable(), arguments: ["mcp", "remove", "agent-vmm"])
    }

    func createManual(displayName: String) throws -> MCPClientBindingModel {
        var values = try bindings()
        let value = MCPClientBindingModel(id: UUID(), kind: .manual, displayName: displayName, binding: Self.newBinding())
        values.append(value)
        try save(values)
        return value
    }

    func manualCommand(binding: String) throws -> String {
        "codex mcp add agent-vmm --url \(shellQuote(try endpointURL(binding: binding).absoluteString))"
    }

    func canConfigureCodex() -> Bool { (try? codexExecutable()) != nil }

    static func newBinding() -> String {
        var generator = SystemRandomNumberGenerator()
        return (0..<32).map { _ in String(format: "%02x", UInt8.random(in: .min ... .max, using: &generator)) }.joined()
    }

    func removeAllBindings() throws {
        guard FileManager.default.fileExists(atPath: storeURL.path) else { return }
        try FileManager.default.removeItem(at: storeURL)
    }

    private func save(_ values: [MCPClientBindingModel]) throws {
        try FileManager.default.createDirectory(at: storeURL.deletingLastPathComponent(), withIntermediateDirectories: true, attributes: [.posixPermissions: 0o700])
        try JSONEncoder().encode(values).write(to: storeURL, options: .atomic)
        try FileManager.default.setAttributes([.posixPermissions: 0o600], ofItemAtPath: storeURL.path)
    }

    private var storeURL: URL { BundledMCPAppliance.defaultSupportDirectory.appending(path: "mcp-client-bindings.json") }
    private func shellQuote(_ value: String) -> String { "'" + value.replacingOccurrences(of: "'", with: "'\\''") + "'" }

    private func endpointURL(binding: String) throws -> URL {
        let endpoint = try String(contentsOf: BundledMCPAppliance.endpointFile(), encoding: .utf8).trimmingCharacters(in: .whitespacesAndNewlines)
        guard !endpoint.isEmpty,
              var components = URLComponents(string: "http://\(endpoint)/mcp") else { throw ConfigurationError.endpointUnavailable }
        components.queryItems = [URLQueryItem(name: "access_token", value: binding)]
        guard let url = components.url else { throw ConfigurationError.endpointUnavailable }
        return url
    }

    private func codexExecutable() throws -> URL {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let candidates = [URL(filePath: "/opt/homebrew/bin/codex"), URL(filePath: "/usr/local/bin/codex"), home.appending(path: ".local/bin/codex")]
        guard let executable = candidates.first(where: { FileManager.default.isExecutableFile(atPath: $0.path) }) else { throw ConfigurationError.codexUnavailable }
        return executable
    }

    func run(_ executable: URL, arguments: [String], timeout: Duration = .seconds(30)) async throws -> Data {
        try await Task.detached {
            let directory = FileManager.default.temporaryDirectory.appending(path: "agent-vmm-command-\(UUID().uuidString)")
            try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
            defer { try? FileManager.default.removeItem(at: directory) }
            let outputURL = directory.appending(path: "stdout"), errorURL = directory.appending(path: "stderr")
            FileManager.default.createFile(atPath: outputURL.path, contents: nil, attributes: [.posixPermissions: 0o600])
            FileManager.default.createFile(atPath: errorURL.path, contents: nil, attributes: [.posixPermissions: 0o600])
            let output = try FileHandle(forWritingTo: outputURL), errors = try FileHandle(forWritingTo: errorURL)
            defer { try? output.close(); try? errors.close() }
            let process = Process()
            defer { if process.isRunning { process.terminate() } }
            process.executableURL = executable; process.arguments = arguments
            process.standardOutput = output; process.standardError = errors
            try process.run()
            let deadline = ContinuousClock.now.advanced(by: timeout)
            while process.isRunning && ContinuousClock.now < deadline { try await Task.sleep(for: .milliseconds(50)) }
            if process.isRunning { process.terminate(); process.waitUntilExit(); throw ConfigurationError.commandTimedOut }
            process.waitUntilExit(); try output.close(); try errors.close()
            let data = try Data(contentsOf: outputURL)
            guard process.terminationStatus == 0 else { throw ConfigurationError.commandFailed }
            return data
        }.value
    }
}
