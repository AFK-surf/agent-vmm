package remoteconnector

import (
	"context"
	"fmt"

	remotev1 "github.com/AFK-surf/agent-vmm/api/remote/v1"
)

func Enroll(ctx context.Context, config Config, token []byte) (string, error) {
	if err := config.validate(); err != nil {
		wipe(token)
		return "", err
	}
	if len(token) == 0 || len(token) > MaxCredentialBytes {
		wipe(token)
		return "", fmt.Errorf("enrollment token size is invalid")
	}
	client, err := newControllerClient(config, nil)
	if err != nil {
		wipe(token)
		return "", fmt.Errorf("controller TLS connection failed")
	}
	defer client.connection.Close()
	requestToken := append([]byte(nil), token...)
	wipe(token)
	request := &remotev1.EnrollRequest{ProtocolVersion: ProtocolVersion, RegistrationId: config.RegistrationID, EnrollmentToken: requestToken, EndpointIdentity: config.EndpointIdentity, ConnectorRelease: ConnectorRelease}
	response, err := client.client.Enroll(ctx, request)
	wipe(requestToken)
	request.EnrollmentToken = nil
	if err != nil {
		return "", fmt.Errorf("controller enrollment failed")
	}
	if response.GetProtocolVersion() != ProtocolVersion || response.GetRegistrationId() != config.RegistrationID {
		wipe(response.Credential)
		return "", fmt.Errorf("controller enrollment identity mismatch")
	}
	if len(response.GetCredential()) == 0 || len(response.GetCredential()) > MaxCredentialBytes {
		wipe(response.Credential)
		return "", fmt.Errorf("controller enrollment credential is invalid")
	}
	if err := config.CredentialStore.Replace(ctx, config.RegistrationID, response.GetCredential()); err != nil {
		wipe(response.Credential)
		return "", err
	}
	wipe(response.Credential)
	return boundedLabel(response.GetControllerDisplayName()), nil
}

func boundedLabel(value string) string {
	if len(value) > MaxLabelBytes {
		return value[:MaxLabelBytes]
	}
	return value
}
