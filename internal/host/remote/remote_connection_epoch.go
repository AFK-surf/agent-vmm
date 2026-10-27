//go:build darwin || linux

package remote

func (controller *Owner) remoteConnectionEpoch(registrationID, instance string) uint64 {
	controller.remoteConnectionEpochMu.Lock()
	defer controller.remoteConnectionEpochMu.Unlock()
	return controller.remoteConnectionEpochs[remoteConnectionKey(registrationID, instance)]
}

func (controller *Owner) setRemoteConnectionEpoch(registrationID, instance string, epoch uint64) {
	controller.remoteConnectionEpochMu.Lock()
	defer controller.remoteConnectionEpochMu.Unlock()
	controller.remoteConnectionEpochs[remoteConnectionKey(registrationID, instance)] = epoch
}

func (controller *Owner) deleteRemoteConnectionEpoch(registrationID, instance string) {
	controller.remoteConnectionEpochMu.Lock()
	defer controller.remoteConnectionEpochMu.Unlock()
	delete(controller.remoteConnectionEpochs, remoteConnectionKey(registrationID, instance))
}

func remoteConnectionKey(registrationID, instance string) string {
	return registrationID + "\x00" + instance
}
