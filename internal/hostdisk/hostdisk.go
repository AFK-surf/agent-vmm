// Package hostdisk holds the host disk policy of the VM state disk.
package hostdisk

const (
	// EmergencyBytes stays free on the host volume of the state disk.
	EmergencyBytes = int64(10 << 30)
	// DefaultStateBytes is the virtual size of a new state disk.
	DefaultStateBytes = int64(64 << 30)
)

// NewInstallationBytes is the free space that a new installation needs on the
// host volume of its state disk.
func NewInstallationBytes() int64 {
	return DefaultStateBytes + EmergencyBytes
}
