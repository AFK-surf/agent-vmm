import Foundation

extension GRPCHostClient {
    func listAppliances() async throws -> [ApplianceStatusModel] {
        try await applianceService.listAppliances(.init()).appliances.map(\.domainModel)
    }
}
