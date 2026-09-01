package ocimaterializer

// SnapshotReferenceClosure derives parent protection from containerd's actual
// snapshot roots. It is intentionally transient: containerd remains the only
// reference authority.
func SnapshotReferenceClosure(roots map[string]bool, parents map[string]string) map[string]bool {
	result := make(map[string]bool, len(roots))
	for root := range roots {
		result[root] = true
		for parent := parents[root]; parent != ""; parent = parents[parent] {
			if result[parent] {
				break
			}
			result[parent] = true
		}
	}
	return result
}
