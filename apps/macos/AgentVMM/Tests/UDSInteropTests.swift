import Foundation
import GRPCCore
import GRPCNIOTransportHTTP2
import Testing
@testable import AgentVMM

@Test func grpcSwiftInteroperatesWithGRPCGoOverUnixSockets() async throws {
    let repository = URL(fileURLWithPath: #filePath)
        .deletingLastPathComponent().deletingLastPathComponent()
        .deletingLastPathComponent().deletingLastPathComponent()
        .deletingLastPathComponent()
    let directory = URL(fileURLWithPath: "/tmp").appending(path: "agent-vmm-swift-\(UUID().uuidString)")
    try FileManager.default.createDirectory(at: directory, withIntermediateDirectories: true)
    defer { try? FileManager.default.removeItem(at: directory) }
    let operatorSocket = directory.appending(path: "operator.sock").path
    let agentSocket = directory.appending(path: "agent.sock").path

    func launchFixture() async throws -> Process {
        let process = Process()
        process.executableURL = URL(fileURLWithPath: "/opt/homebrew/bin/go")
        process.arguments = ["run", "./test/transportfixture", "--operator-socket", operatorSocket, "--agent-socket", agentSocket]
        process.currentDirectoryURL = repository
        try process.run()
        let deadline = Date().addingTimeInterval(20)
        while (!FileManager.default.fileExists(atPath: operatorSocket) || !FileManager.default.fileExists(atPath: agentSocket)) && Date() < deadline {
            guard process.isRunning else { throw HostClientError.invalidResponse("transport fixture exited with status \(process.terminationStatus)") }
            try await Task.sleep(for: .milliseconds(20))
        }
        guard FileManager.default.fileExists(atPath: operatorSocket), FileManager.default.fileExists(atPath: agentSocket) else {
            process.terminate()
            throw HostClientError.invalidResponse("transport fixture did not create its sockets")
        }
        return process
    }

    var fixture = try await launchFixture()
    defer { if fixture.isRunning { fixture.terminate(); fixture.waitUntilExit() } }

    let operatorClient = try GRPCHostClient(socketPath: operatorSocket)
    let snapshot = try await operatorClient.snapshot()
    #expect(snapshot.bootID == "transport-fixture")
    #expect(snapshot.remoteRegistrations.first?.pool.perEnvironmentLimits.memoryBytes == 1 << 30)
    #expect(snapshot.remoteRegistrations.first?.pool.disconnectGraceSeconds == 45)
    #expect(snapshot.remoteRegistrations.first?.allocations.first?.environmentID == "environment-a")
    #expect(snapshot.remoteRegistrations.first?.allocations.first?.lease?.state == .active)
    let event = try await operatorClient.events(after: 0).first { _ in true }
    #expect(event?.type == "fixture")

    try await withGRPCClient(transport: .http2NIOPosix(target: .unixDomainSocket(path: agentSocket), transportSecurity: .plaintext)) { client in
        let agent = Agentvmm_Host_V1_AgentRuntimeService.Client(wrapping: client)
        #expect(try await agent.getEnvironment(.init()).environment.id == "fixture")
        let received = try await agent.exec { writer in
            var start = Agentvmm_Host_V1_ExecRequest()
            start.start.requestID = "interop"
            start.start.containerID = "fixture"
            try await writer.write(start)
            for _ in 0..<128 {
                var input = Agentvmm_Host_V1_ExecRequest()
                input.stdin = Data(repeating: 7, count: 32 << 10)
                try await writer.write(input)
            }
        } onResponse: { response in
            var bytes = 0
            for try await message in response.messages { bytes += message.stdout.count }
            return bytes
        }
        #expect(received == 128 * (32 << 10))

        var rejectedOversizedMessage = false
        do {
            try await agent.exec { writer in
                var oversized = Agentvmm_Host_V1_ExecRequest()
                oversized.stdin = Data(repeating: 1, count: 2 << 20)
                try await writer.write(oversized)
            } onResponse: { response in
                for try await _ in response.messages {}
            }
        } catch {
            rejectedOversizedMessage = true
        }
        #expect(rejectedOversizedMessage)
    }

    fixture.terminate()
    fixture.waitUntilExit()
    try? FileManager.default.removeItem(atPath: operatorSocket)
    try? FileManager.default.removeItem(atPath: agentSocket)
    fixture = try await launchFixture()
    #expect(try await GRPCHostClient(socketPath: operatorSocket).snapshot().vmState == "Running")
}
