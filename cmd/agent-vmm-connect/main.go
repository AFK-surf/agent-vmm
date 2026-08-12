//go:build darwin

package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/AFK-surf/agent-vmm/internal/remoteconnector"
)

func main() {
	var config remoteconnector.Config
	var enroll bool
	var revokeCredential bool
	flag.StringVar(&config.Endpoint, "endpoint", "", "controller TLS host:port")
	flag.StringVar(&config.EndpointIdentity, "endpoint-identity", "", "controller TLS DNS identity")
	flag.StringVar(&config.RegistrationID, "registration", "", "local registration ID")
	flag.StringVar(&config.RemoteComputeSocket, "remote-compute-socket", "", "restricted host Unix socket")
	flag.StringVar(&config.TrustSocket, "trust-socket", "", "connector-only host trust Unix socket")
	flag.StringVar(&config.TrustCapabilityFile, "trust-capability-file", "", "install-scoped connector trust capability")
	flag.StringVar(&config.ServiceTunnelSocket, "service-tunnel-socket", "", "destination service tunnel Unix socket")
	flag.StringVar(&config.MeshControlSocket, "mesh-control-socket", "", "personal mesh control Unix socket")
	flag.StringVar(&config.DataWorkerPath, "data-worker", "", "connector-supervised Agent VMM data worker")
	flag.StringVar(&config.DataWorkerSocket, "data-worker-socket", "", "data worker local control socket")
	flag.BoolVar(&enroll, "enroll", false, "read one-time enrollment token from stdin")
	flag.BoolVar(&revokeCredential, "revoke-credential", false, "delete the registration credential from Keychain")
	flag.Parse()
	if config.TrustSocket == "" && config.RemoteComputeSocket != "" {
		config.TrustSocket = filepath.Join(filepath.Dir(config.RemoteComputeSocket), "trust.sock")
	}
	if config.TrustCapabilityFile == "" && config.RemoteComputeSocket != "" {
		config.TrustCapabilityFile = filepath.Join(filepath.Dir(config.RemoteComputeSocket), "connector-trust.cap")
	}
	if config.ServiceTunnelSocket == "" && config.RemoteComputeSocket != "" {
		config.ServiceTunnelSocket = filepath.Join(filepath.Dir(config.RemoteComputeSocket), "service-tunnel.sock")
	}
	if config.MeshControlSocket == "" && config.RemoteComputeSocket != "" {
		config.MeshControlSocket = filepath.Join(filepath.Dir(config.RemoteComputeSocket), "mesh-control.sock")
	}
	if config.DataWorkerSocket == "" && config.RemoteComputeSocket != "" && config.DataWorkerPath != "" {
		config.DataWorkerSocket = filepath.Join(filepath.Dir(config.RemoteComputeSocket), "data-worker.sock")
	}
	config.CredentialStore = remoteconnector.NewCredentialStore()
	config.Logger = log.New(os.Stderr, "agent-vmm-connect: ", log.LstdFlags)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if revokeCredential {
		if config.RegistrationID == "" {
			log.Fatal("registration is required")
		}
		if err := config.CredentialStore.Delete(ctx, config.RegistrationID); err != nil {
			log.Fatal(err)
		}
		return
	}
	if enroll {
		token, err := io.ReadAll(io.LimitReader(os.Stdin, remoteconnector.MaxCredentialBytes+1))
		if err != nil || len(token) > remoteconnector.MaxCredentialBytes {
			log.Fatal("enrollment token input failed")
		}
		token = bytes.TrimSpace(token)
		enrollContext, enrollCancel := context.WithTimeout(ctx, 30*time.Second)
		displayName, err := remoteconnector.Enroll(enrollContext, config, token)
		enrollCancel()
		if err != nil {
			log.Fatal(err)
		}
		_, _ = fmt.Fprintf(os.Stderr, "enrolled controller %q\n", displayName)
		return
	}
	if config.Endpoint == "" && config.RegistrationID == "" {
		trustSocket, trustCapabilityFile := config.TrustSocket, config.TrustCapabilityFile
		serviceTunnelSocket, meshControlSocket, dataWorkerPath, dataWorkerSocket := config.ServiceTunnelSocket, config.MeshControlSocket, config.DataWorkerPath, config.DataWorkerSocket
		discovered, err := remoteconnector.LoadConfig(ctx, config.RemoteComputeSocket, config.CredentialStore)
		if err != nil {
			log.Fatal(err)
		}
		discovered.Logger = config.Logger
		discovered.TrustSocket = trustSocket
		discovered.TrustCapabilityFile = trustCapabilityFile
		discovered.ServiceTunnelSocket = serviceTunnelSocket
		discovered.MeshControlSocket = meshControlSocket
		discovered.DataWorkerPath = dataWorkerPath
		discovered.DataWorkerSocket = dataWorkerSocket
		config = discovered
	}
	if err := remoteconnector.Run(ctx, config); err != nil {
		log.Fatal(err)
	}
}
