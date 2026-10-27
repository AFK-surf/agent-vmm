//go:build darwin || linux

package host

import (
	"context"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
)

func (server *operatorServer) ListServiceListeners(ctx context.Context, request *hostv1.ListServiceListenersRequest) (*hostv1.ListServiceListenersResponse, error) {
	return server.controller.services.ListServiceListeners(ctx, request)
}
func (server *operatorServer) RevokeServiceListener(ctx context.Context, request *hostv1.RevokeServiceListenerRequest) (*hostv1.RevokeServiceListenerResponse, error) {
	return server.controller.services.RevokeServiceListener(ctx, request)
}
func (server *operatorServer) CreateLocalServiceListener(ctx context.Context, request *hostv1.CreateLocalServiceListenerRequest) (*hostv1.CreateLocalServiceListenerResponse, error) {
	return server.controller.services.CreateLocalServiceListener(ctx, request)
}
func (server *agentServer) CreateServiceExport(ctx context.Context, request *hostv1.CreateServiceExportRequest) (*hostv1.CreateServiceExportResponse, error) {
	return server.controller.services.CreateServiceExport(ctx, request)
}
func (server *agentServer) ListServiceExports(ctx context.Context, request *hostv1.ListServiceExportsRequest) (*hostv1.ListServiceExportsResponse, error) {
	return server.controller.services.ListServiceExports(ctx, request)
}
func (server *agentServer) RevokeServiceExport(ctx context.Context, request *hostv1.RevokeServiceExportRequest) (*hostv1.RevokeServiceExportResponse, error) {
	return server.controller.services.RevokeServiceExport(ctx, request)
}
func (server *agentServer) RevokeServiceImport(ctx context.Context, request *hostv1.RevokeServiceImportRequest) (*hostv1.RevokeServiceImportResponse, error) {
	return server.controller.services.RevokeServiceImport(ctx, request)
}
func (server *agentServer) CreateServiceImport(ctx context.Context, request *hostv1.CreateServiceImportRequest) (*hostv1.CreateServiceImportResponse, error) {
	return server.controller.services.CreateServiceImport(ctx, request)
}
func (server *agentServer) CreatePersonalServiceImport(ctx context.Context, request *hostv1.CreatePersonalServiceImportRequest) (*hostv1.CreatePersonalServiceImportResponse, error) {
	return server.controller.services.CreatePersonalServiceImport(ctx, request)
}
func (server *agentServer) ListServiceImports(ctx context.Context, request *hostv1.ListServiceImportsRequest) (*hostv1.ListServiceImportsResponse, error) {
	return server.controller.services.ListServiceImports(ctx, request)
}
