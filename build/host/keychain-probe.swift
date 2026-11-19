import Foundation
import Security

let expectsAccess = CommandLine.arguments.dropFirst().first == "true"
let key: [String: Any] = [
    kSecClass as String: kSecClassGenericPassword,
    kSecAttrService as String: "com.afk-surf.agent-vmm-host",
    kSecAttrAccount as String: "package-test/" + UUID().uuidString,
    kSecAttrAccessGroup as String: "D9AAN3VJK8.com.afk-surf.agent-vmm-host",
    kSecUseDataProtectionKeychain as String: true,
]
let secret = Data(UUID().uuidString.utf8)
var item = key
item[kSecValueData as String] = secret
let added = SecItemAdd(item as CFDictionary, nil)
if added == errSecSuccess {
    var query = key
    query[kSecReturnData as String] = true
    var result: CFTypeRef?
    let read = SecItemCopyMatching(query as CFDictionary, &result)
    let removed = SecItemDelete(key as CFDictionary)
    guard expectsAccess, read == errSecSuccess, result as? Data == secret, removed == errSecSuccess else {
        fputs("Unexpected Keychain result (add \\(added), read \\(read), delete \\(removed))\n", stderr)
        exit(1)
    }
} else if expectsAccess || added != errSecMissingEntitlement {
    fputs("Unexpected Keychain status \\(added)\n", stderr)
    exit(1)
}
