package host

import (
	"crypto/sha256"
	"encoding/hex"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/protobuf/proto"
)

func runtimeRequestHash(request proto.Message) []byte {
	body, _ := proto.MarshalOptions{Deterministic: true}.Marshal(request)
	digest := sha256.Sum256(body)
	return digest[:]
}

func containerConfigurationFingerprint(request *hostv1.CreateContainerRequest) string {
	configuration := proto.Clone(request).(*hostv1.CreateContainerRequest)
	configuration.RequestId = ""
	return hex.EncodeToString(runtimeRequestHash(configuration))
}
