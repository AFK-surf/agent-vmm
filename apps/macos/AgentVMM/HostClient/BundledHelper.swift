import Foundation

enum BundledHelper {
    static func executable(named name: String, bundleURL: URL = Bundle.main.bundleURL) -> URL? {
        let helper = bundleURL.appending(path: "Contents/Helpers").appending(path: name)
        guard FileManager.default.isExecutableFile(atPath: helper.path) else { return nil }
        return helper
    }
}
