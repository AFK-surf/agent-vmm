import Foundation

extension SocketHostClient {
    func listAppliances() async throws -> [ApplianceStatusModel] {
        try await applianceService.unary("ListAppliances", Agentvmm_Appliance_V1_ListAppliancesRequest(), as: Agentvmm_Appliance_V1_ListAppliancesResponse.self).appliances.map(\.domainModel)
    }
}
