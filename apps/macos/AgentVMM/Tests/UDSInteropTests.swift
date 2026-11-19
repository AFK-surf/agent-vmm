import Foundation
import Testing
@testable import AgentVMM

private final class TestBundleMarker {}

@Test func swiftReadsRustOperatorHealthAndTerminalStatus() async throws {
    let bundle = Bundle(for: TestBundleMarker.self).bundleURL
    // XCTest places the test bundle in the app's Contents/PlugIns directory.
    var products = bundle.deletingLastPathComponent()
    while products.path != "/" && !FileManager.default.isExecutableFile(atPath: products.appendingPathComponent("agent-vmm-interop-host").path) {
        products.deleteLastPathComponent()
    }
    let executable = products.appendingPathComponent("agent-vmm-interop-host")
    try #require(FileManager.default.isExecutableFile(atPath: executable.path))
    let root = URL(fileURLWithPath: "/tmp").appendingPathComponent(UUID().uuidString)
    try FileManager.default.createDirectory(at: root, withIntermediateDirectories: false, attributes: [.posixPermissions: 0o700])
    defer { try? FileManager.default.removeItem(at: root) }
    let host = Process()
    host.executableURL = executable
    host.arguments = ["--database", root.appendingPathComponent("host.sqlite").path,
                      "--carrier-socket", root.appendingPathComponent("carrier.sock").path]
    try host.run()
    defer {
        if host.isRunning { host.terminate() }
        host.waitUntilExit()
    }
    let socket = root.appendingPathComponent("operator.sock").path
    let deadline = ContinuousClock.now + .seconds(10)
    while !FileManager.default.fileExists(atPath: socket) {
        try #require(host.isRunning && ContinuousClock.now < deadline)
        try await Task.sleep(for: .milliseconds(20))
    }
    let rpc = VmmRpcClient(path: socket, service: "agentvmm.host.v1.OperatorService")
    let query = Agentvmm_Host_V1_GetSystemHealthSummaryRequest()
    let health = try await rpc.unary("GetSystemHealthSummary", query, as: Agentvmm_Host_V1_GetSystemHealthSummaryResponse.self)
    #expect(health.hasCollectedAt)
    #expect(health.health.components.contains { $0.component == "dom0" })
    do {
        _ = try await rpc.upload("GetSystemHealthSummary", as: Agentvmm_Host_V1_GetSystemHealthSummaryResponse.self) { send in
            try send(query)
            try send(query)
        }
        Issue.record("Unary method accepted two streamed requests")
    } catch let status as RPCError {
        #expect(status.code == .invalidArgument)
    }
    do {
        _ = try await rpc.unary("MissingMethod", query, as: Agentvmm_Host_V1_GetSystemHealthSummaryResponse.self)
        Issue.record("Operator accepted an unknown method")
    } catch let status as RPCError {
        #expect(status.code == .unimplemented)
    }
}
