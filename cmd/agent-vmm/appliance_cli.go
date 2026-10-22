//go:build darwin || linux

package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	appliancev1 "github.com/AFK-surf/agent-vmm/api/appliance/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func dispatchApplianceCLI(arguments []string) bool {
	if len(arguments) < 2 || arguments[0] != "appliance" {
		return false
	}
	switch arguments[1] {
	case "--help", "-h":
		fmt.Println("usage: agent-vmm appliance install|update|status|recover|stop|start|activate|restart|revoke|remove ...")
	case "remove":
		applianceRemove(arguments[2:])
	case "status":
		applianceStatus(arguments[2:])
	case "install", "update", "recover", "stop", "start", "activate", "restart", "revoke":
		applianceLifecycle(arguments[1], arguments[2:])
	default:
		return false
	}
	return true
}

func check(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent-vmm:", err)
		os.Exit(1)
	}
}

func applianceStatus(arguments []string) {
	flags := flag.NewFlagSet("appliance status", flag.ExitOnError)
	socket := flags.String("operator-socket", filepath.Join(platformDataDirectory(os.Getenv("HOME")), "operator.sock"), "Agent VMM operator socket")
	flags.Parse(arguments)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	connection, err := grpc.NewClient("passthrough:///agent-vmm", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", *socket)
	}))
	check(err)
	defer connection.Close()
	response, err := appliancev1.NewApplianceAdminServiceClient(connection).List(ctx, &appliancev1.ListAppliancesRequest{})
	check(err)
	for _, appliance := range response.GetAppliances() {
		health := appliance.GetHealth()
		fmt.Printf("id=%s state=%s channel=%s enabled=%t healthy=%t status=%s boot=%s container=%s\n", appliance.GetInstallationId(), appliance.GetApplianceState(), appliance.GetChannelState(), appliance.GetEnabled(), health.GetHealthy(), health.GetStatus(), appliance.GetBootId(), appliance.GetContainerInstance())
		for _, frontend := range appliance.GetFrontends() {
			fmt.Printf("  frontend=%s endpoint=%s available=%t\n", frontend.GetName(), frontend.GetEndpoint(), frontend.GetAvailable())
		}
	}
}
