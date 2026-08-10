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
	flag.BoolVar(&enroll, "enroll", false, "read one-time enrollment token from stdin")
	flag.BoolVar(&revokeCredential, "revoke-credential", false, "delete the registration credential from Keychain")
	flag.Parse()
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
		discovered, err := remoteconnector.LoadConfig(ctx, config.RemoteComputeSocket, config.CredentialStore)
		if err != nil {
			log.Fatal(err)
		}
		discovered.Logger = config.Logger
		config = discovered
	}
	if err := remoteconnector.Run(ctx, config); err != nil {
		log.Fatal(err)
	}
}
