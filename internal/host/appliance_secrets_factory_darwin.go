//go:build darwin && !development && !e2e

package host

func newApplianceSecretStore(directory string) (applianceSecretStore, error) {
	return newKeychainApplianceSecrets(directory)
}
