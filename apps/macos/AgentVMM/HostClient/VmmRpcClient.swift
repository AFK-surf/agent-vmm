import Darwin
import Foundation
import SwiftProtobuf

struct RPCError: Error {
    enum Code: UInt8, Decodable {
        case ok, cancelled, unknown, invalidArgument, deadlineExceeded, notFound, alreadyExists
        case permissionDenied, resourceExhausted, failedPrecondition, aborted, outOfRange
        case unimplemented, internalError, unavailable, dataLoss, unauthenticated
    }

    let code: Code
    let message: String
    var details = Data()
}

private struct TerminalStatus: Decodable {
    let code: RPCError.Code
    let message: String
    let details_b64: String?
}

private final class CallSocket: @unchecked Sendable {
    private let lock = NSLock()
    private let fd: Int32
    private var cancelled = false

    init() throws {
        let socket = Darwin.socket(AF_UNIX, SOCK_STREAM, 0)
        guard socket >= 0 else { throw Self.posixError() }
        var enabled: Int32 = 1
        if setsockopt(socket, SOL_SOCKET, SO_NOSIGPIPE, &enabled, socklen_t(MemoryLayout.size(ofValue: enabled))) < 0 {
            let error = Self.posixError()
            Darwin.close(socket)
            throw error
        }
        fd = socket
    }

    deinit { Darwin.close(fd) }

    func cancel() {
        lock.withLock {
            cancelled = true
            Darwin.shutdown(fd, SHUT_RDWR)
        }
    }

    func connect(_ path: String) throws {
        let bytes = Array(path.utf8)
        var address = sockaddr_un()
        guard !bytes.isEmpty, !bytes.contains(0), bytes.count < MemoryLayout.size(ofValue: address.sun_path)
        else { throw HostClientError.invalidResponse("operator socket path") }
        address.sun_family = sa_family_t(AF_UNIX)
        address.sun_len = UInt8(MemoryLayout<sockaddr_un>.size)
        withUnsafeMutableBytes(of: &address.sun_path) { buffer in
            buffer.copyBytes(from: bytes)
        }
        guard !lock.withLock({ cancelled }) else { throw CancellationError() }
        let result = withUnsafePointer(to: &address) { pointer in
            pointer.withMemoryRebound(to: sockaddr.self, capacity: 1) {
                Darwin.connect(fd, $0, socklen_t(MemoryLayout<sockaddr_un>.size))
            }
        }
        guard result == 0 else { throw Self.posixError() }
        guard !lock.withLock({ cancelled }) else { throw CancellationError() }
    }

    private static func posixError() -> POSIXError {
        POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO)
    }

    private func write(_ data: Data) throws {
        try data.withUnsafeBytes { buffer in
            var offset = 0
            while offset < buffer.count {
                let count = Darwin.write(fd, buffer.baseAddress!.advanced(by: offset), buffer.count - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else { throw Self.posixError() }
                offset += count
            }
        }
    }

    func send(_ tag: UInt8, _ bytes: Data) throws {
        let limit = tag == 0 ? 64 << 10 : 16 << 20
        guard bytes.count <= limit else { throw RPCError(code: .resourceExhausted, message: "RPC frame too large") }
        let size = UInt32(bytes.count).bigEndian
        try write(Data([tag]) + withUnsafeBytes(of: size) { Data($0) })
        try write(bytes)
    }

    private func read(_ length: Int) throws -> Data {
        var data = Data(count: length)
        try data.withUnsafeMutableBytes { buffer in
            var offset = 0
            while offset < length {
                let count = Darwin.read(fd, buffer.baseAddress!.advanced(by: offset), length - offset)
                if count < 0 && errno == EINTR { continue }
                guard count > 0 else {
                    if count < 0 { throw Self.posixError() }
                    throw HostClientError.disconnected
                }
                offset += count
            }
        }
        return data
    }

    func receive() throws -> Data? {
        let prefix = try read(5)
        let length = prefix.dropFirst().reduce(0) { ($0 << 8) | Int($1) }
        guard length <= 16 << 20, prefix[0] == 1 || prefix[0] == 3 else {
            throw HostClientError.invalidResponse("RPC frame header")
        }
        let payload = try read(length)
        guard prefix[0] == 3 else { return payload }
        let status = try JSONDecoder().decode(TerminalStatus.self, from: payload)
        guard let details = Data(base64Encoded: status.details_b64 ?? "") else {
            throw HostClientError.invalidResponse("RPC status details")
        }
        if status.code != .ok { throw RPCError(code: status.code, message: status.message, details: details) }
        return nil
    }
}

struct VmmRpcClient: Sendable {
    let path: String
    let service: String

    private struct Header: Encodable {
        let v = 1
        let method: String
        let metadata: [[String]] = []
    }

    private func perform<T: Sendable>(
        _ method: String, _ call: @escaping @Sendable (CallSocket) throws -> T
    ) async throws -> T {
        let socket = try CallSocket()
        return try await withTaskCancellationHandler {
            try await withCheckedThrowingContinuation { continuation in
                DispatchQueue.global(qos: .userInitiated).async {
                    continuation.resume(with: Result {
                        try socket.connect(path)
                        try socket.send(0, JSONEncoder().encode(Header(method: "/\(service)/\(method)")))
                        return try call(socket)
                    })
                }
            }
        } onCancel: { socket.cancel() }
    }

    func unary<I: Message, O: Message>(_ method: String, _ request: I, as: O.Type) async throws -> O {
        try await upload(method, as: O.self) { send in try send(request) }
    }

    func upload<I: Message, O: Message>(
        _ method: String, as: O.Type,
        produce: @escaping @Sendable ((I) throws -> Void) throws -> Void
    ) async throws -> O {
        try await perform(method) { socket in
            try produce { try socket.send(1, $0.serializedData()) }
            try socket.send(2, Data())
            guard let bytes = try socket.receive() else { throw HostClientError.invalidResponse("missing unary response") }
            let message = try O(serializedBytes: bytes)
            guard try socket.receive() == nil else { throw HostClientError.invalidResponse("extra unary response") }
            return message
        }
    }

    func receive<I: Message, O: Message>(
        _ method: String, _ request: I, as: O.Type, emit: @escaping @Sendable (O) -> Void
    ) async throws {
        try await perform(method) { socket in
            try socket.send(1, request.serializedData())
            try socket.send(2, Data())
            while let bytes = try socket.receive() { emit(try O(serializedBytes: bytes)) }
        }
    }
}
