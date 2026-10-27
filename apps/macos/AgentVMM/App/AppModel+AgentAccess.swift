import Foundation

extension AppModel {
    var bundledMCP: ApplianceStatusModel? {
        appliances.first { $0.id == BundledMCPAppliance.installationID }
    }
}
