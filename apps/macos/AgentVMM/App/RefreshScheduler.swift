import Foundation

@MainActor
final class RefreshScheduler {
    private let refresh: @MainActor () async throws -> Void
    private var dirty = false
    private var task: Task<Void, Never>?
    private var waiters: [CheckedContinuation<Void, Error>] = []

    init(refresh: @escaping @MainActor () async throws -> Void) {
        self.refresh = refresh
    }

    func requestRefresh() {
        dirty = true
        guard task == nil else { return }
        task = Task { await drain() }
    }

    func refreshNow() async throws {
        try await withCheckedThrowingContinuation { continuation in
            waiters.append(continuation)
            requestRefresh()
        }
    }

    private func drain() async {
        while dirty {
            dirty = false
            let current = waiters
            waiters.removeAll()
            do {
                try await refresh()
                current.forEach { $0.resume() }
            } catch {
                current.forEach { $0.resume(throwing: error) }
            }
        }
        task = nil
    }
}
