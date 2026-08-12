import Foundation
import Observation

@MainActor
@Observable
final class PairingDiscovery: NSObject, @preconcurrency NetServiceBrowserDelegate, @preconcurrency NetServiceDelegate {
    private(set) var invitations: [String] = []
    private let browser = NetServiceBrowser()
    private var discovered: [NetService] = []
    private var advertised: NetService?

    override init() {
        super.init()
        browser.delegate = self
        browser.searchForServices(ofType: "_agent-vmm-pair._tcp.", inDomain: "local.")
    }

    func advertise(_ invitation: String) {
        advertised?.stop()
        advertised = nil
        guard invitation.utf8.count <= 1_200 else { return }
        let chunks = stride(from: 0, to: invitation.count, by: 180).map { offset -> String in
            let start = invitation.index(invitation.startIndex, offsetBy: offset)
            let end = invitation.index(start, offsetBy: min(180, invitation.distance(from: start, to: invitation.endIndex)))
            return String(invitation[start..<end])
        }
        let record = Dictionary(uniqueKeysWithValues: chunks.enumerated().map { index, chunk in
            (String(format: "i%03d", index), Data(chunk.utf8))
        })
        let service = NetService(domain: "local.", type: "_agent-vmm-pair._tcp.", name: "Agent VMM Pairing", port: 9)
        service.setTXTRecord(NetService.data(fromTXTRecord: record))
        service.delegate = self
        service.publish()
        advertised = service
    }

    func stopAdvertising() {
        advertised?.stop()
        advertised = nil
    }

    func netServiceBrowser(
        _ browser: NetServiceBrowser,
        didFind service: NetService,
        moreComing: Bool
    ) {
        service.delegate = self
        discovered.append(service)
        service.resolve(withTimeout: 5)
    }

    func netServiceBrowser(
        _ browser: NetServiceBrowser,
        didRemove service: NetService,
        moreComing: Bool
    ) {
        discovered.removeAll { $0 === service }
        rebuildInvitations()
    }

    func netService(_ sender: NetService, didUpdateTXTRecord data: Data) { rebuildInvitations() }

    func netServiceDidResolveAddress(_ sender: NetService) { rebuildInvitations() }

    private func rebuildInvitations() {
        invitations = discovered.compactMap { service in
            guard let data = service.txtRecordData() else { return nil }
            let record = NetService.dictionary(fromTXTRecord: data)
            let chunks = record.keys.filter { $0.hasPrefix("i") }.sorted().compactMap { key in
                record[key].flatMap { String(data: $0, encoding: .utf8) }
            }
            guard !chunks.isEmpty else { return nil }
            return chunks.joined()
        }
        .filter { Data(base64Encoded: $0) != nil }
        .uniqued()
    }
}

private extension Sequence where Element: Hashable {
    func uniqued() -> [Element] {
        var seen = Set<Element>()
        return filter { seen.insert($0).inserted }
    }
}
