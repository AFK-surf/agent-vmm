//go:build darwin || linux

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func runMCP(arguments []string) int {
	if len(arguments) < 2 || arguments[0] != "token" || arguments[1] != "create" && arguments[1] != "rotate" {
		fmt.Fprintln(os.Stderr, "usage: agent-vmm mcp token create|rotate [--operator-socket PATH]")
		return 2
	}
	flags := flag.NewFlagSet("mcp token "+arguments[1], flag.ContinueOnError)
	socket := flags.String("operator-socket", filepath.Join(serviceDataDirectory(os.Getenv("HOME")), "operator.sock"), "Agent VMM operator socket")
	if err := flags.Parse(arguments[2:]); err != nil || flags.NArg() != 0 {
		return 2
	}
	if err := runMCPToken(arguments[1], *socket); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

func runMCPToken(action, socket string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}))
	if err != nil {
		return err
	}
	defer connection.Close()
	client := hostv1.NewOperatorServiceClient(connection)
	switch action {
	case "create":
		response, err := client.CreateMCPToken(ctx, &hostv1.CreateMCPTokenRequest{})
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(os.Stdout, response.GetToken())
		return err
	case "rotate":
		response, err := client.RotateMCPTokenKey(ctx, &hostv1.RotateMCPTokenKeyRequest{})
		if err != nil {
			return err
		}
		return watchApplianceOperation(ctx, appliancev1.NewApplianceAdminServiceClient(connection), "agent-vmm-mcp", &appliancev1.ApplianceOperation{OperationId: response.GetOperationId()})
	default:
		return errors.New("unknown MCP token action")
	}
}
