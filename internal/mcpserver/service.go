package mcpserver

import (
	"context"
	"encoding/base64"
	"strings"
	"time"

	hostv1 "github.com/AFK-surf/agent-vmm/api/host/v1"
	servicev1 "github.com/AFK-surf/agent-vmm/api/service/v1"
	trustv1 "github.com/AFK-surf/agent-vmm/api/trust/v1"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (adapter *Adapter) registerServiceTools(server *mcp.Server) {
	addTool(server, toolDefinition{name: "mesh_service_list", description: "List service exports and imports visible to this bound environment and its current active mesh membership.", inputSchema: emptyObjectSchema, readOnly: true, idempotent: true, timeout: 30 * time.Second}, adapter.meshServiceList)
	addTool(server, toolDefinition{name: "service_export", description: "Export a port from a current container instance in this bound environment.", inputSchema: serviceExportSchema, timeout: 30 * time.Second}, adapter.serviceExport)
	addTool(server, toolDefinition{name: "service_import", description: "Import a mesh service into a current container instance using a controller-issued route capability.", inputSchema: serviceImportSchema, timeout: 30 * time.Second}, adapter.serviceImport)
	addTool(server, toolDefinition{name: "service_route_revoke", description: "Revoke one service export or import owned by this bound environment.", inputSchema: serviceRouteRevokeSchema, destructive: true, idempotent: true, timeout: 30 * time.Second}, adapter.serviceRouteRevoke)
}

func (adapter *Adapter) meshServiceList(ctx context.Context, _ *mcp.CallToolRequest, _ EmptyInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, false, func(ctx context.Context) (any, error) {
		exports, err := adapter.runtime.ListServiceExports(ctx, &hostv1.ListOwnServiceExportsRequest{})
		if err != nil {
			return nil, err
		}
		imports, err := adapter.runtime.ListServiceImports(ctx, &hostv1.ListServiceImportsRequest{})
		if err != nil {
			return nil, err
		}
		result := MeshServiceListData{Exports: make([]ServiceExportData, 0, len(exports.GetExports())), Imports: make([]ServiceImportData, 0, len(imports.GetImports()))}
		for _, value := range exports.GetExports() {
			result.Exports = append(result.Exports, exportData(value))
		}
		for _, value := range imports.GetImports() {
			result.Imports = append(result.Imports, importData(value))
		}
		return result, nil
	})
}

func (adapter *Adapter) serviceExport(ctx context.Context, _ *mcp.CallToolRequest, input ServiceExportInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(ctx context.Context) (any, error) {
		protocol := servicev1.ServiceProtocol_SERVICE_PROTOCOL_UNSPECIFIED
		switch strings.ToLower(input.Protocol) {
		case "tcp":
			protocol = servicev1.ServiceProtocol_SERVICE_PROTOCOL_TCP
		case "http":
			protocol = servicev1.ServiceProtocol_SERVICE_PROTOCOL_HTTP
		}
		if input.ContainerID == "" || input.ContainerInstanceID == "" || input.Port == 0 || protocol == servicev1.ServiceProtocol_SERVICE_PROTOCOL_UNSPECIFIED {
			return nil, status.Error(codes.InvalidArgument, "complete service export is required")
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		value, err := adapter.runtime.CreateServiceExport(ctx, &hostv1.CreateServiceExportRequest{RequestId: requestID, ContainerId: input.ContainerID, ContainerInstanceId: input.ContainerInstanceID, Port: input.Port, Protocol: protocol})
		if err != nil {
			return nil, err
		}
		return exportData(value), nil
	})
}

func (adapter *Adapter) serviceImport(ctx context.Context, _ *mcp.CallToolRequest, input ServiceImportInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(ctx context.Context) (any, error) {
		if input.SourceContainerID == "" || input.SourceContainerInstanceID == "" || input.DestinationDeviceID == "" || input.DestinationExportID == "" || input.DestinationPort == 0 || input.DestinationPort > 65535 || input.VirtualServiceName == "" {
			return nil, status.Error(codes.InvalidArgument, "complete service import is required")
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		var value *servicev1.ServiceImport
		if input.MeshID != "" {
			if input.Capability != "" || input.ConnectionLimit == 0 || input.ByteLimit == 0 || input.ConcurrencyLimit == 0 {
				return nil, status.Error(codes.InvalidArgument, "personal mesh import requires only mesh_id and an explicit nonzero budget")
			}
			value, err = adapter.runtime.CreatePersonalServiceImport(ctx, &hostv1.CreatePersonalServiceImportRequest{RequestId: requestID, MeshId: input.MeshID,
				SourceContainerId: input.SourceContainerID, SourceContainerInstanceId: input.SourceContainerInstanceID,
				DestinationDeviceId: input.DestinationDeviceID, DestinationExportId: input.DestinationExportID, VirtualServiceName: input.VirtualServiceName,
				Budget: &trustv1.RouteBudget{ConnectionLimit: input.ConnectionLimit, ByteLimit: input.ByteLimit, ConcurrencyLimit: input.ConcurrencyLimit}})
		} else {
			encoded, decodeErr := base64.RawURLEncoding.DecodeString(input.Capability)
			if decodeErr != nil {
				return nil, status.Error(codes.InvalidArgument, "route capability is malformed")
			}
			capability := new(trustv1.RouteCapability)
			if decodeErr := proto.Unmarshal(encoded, capability); decodeErr != nil {
				return nil, status.Error(codes.InvalidArgument, "route capability is malformed")
			}
			value, err = adapter.runtime.CreateServiceImport(ctx, &hostv1.CreateServiceImportRequest{RequestId: requestID, SourceContainerId: input.SourceContainerID, SourceContainerInstanceId: input.SourceContainerInstanceID, DestinationDeviceId: input.DestinationDeviceID, DestinationExportId: input.DestinationExportID, DestinationPort: input.DestinationPort, VirtualServiceName: input.VirtualServiceName, Capability: capability})
		}
		if err != nil {
			return nil, err
		}
		return importData(value), nil
	})
}

func (adapter *Adapter) serviceRouteRevoke(ctx context.Context, _ *mcp.CallToolRequest, input ServiceRouteRevokeInput) (*mcp.CallToolResult, Envelope, error) {
	return adapter.call(ctx, 30*time.Second, true, func(ctx context.Context) (any, error) {
		if input.ID == "" || input.ExpectedRevision == 0 {
			return nil, status.Error(codes.InvalidArgument, "route identity and revision are required")
		}
		requestID, err := newRequestID()
		if err != nil {
			return nil, err
		}
		switch input.Kind {
		case "export":
			_, err = adapter.runtime.RevokeServiceExport(ctx, &hostv1.RevokeOwnServiceExportRequest{RequestId: requestID, ExportId: input.ID, ExpectedRevision: input.ExpectedRevision})
		case "import":
			_, err = adapter.runtime.RevokeServiceImport(ctx, &hostv1.RevokeServiceImportRequest{RequestId: requestID, ImportId: input.ID, ExpectedRevision: input.ExpectedRevision})
		default:
			return nil, status.Error(codes.InvalidArgument, "route kind must be export or import")
		}
		if err != nil {
			return nil, err
		}
		return EmptyData{}, nil
	})
}

func exportData(value *servicev1.ServiceExport) ServiceExportData {
	return ServiceExportData{ID: value.GetExportId(), ContainerID: value.GetContainerId(), ContainerInstanceID: value.GetContainerInstanceId(), Port: value.GetPort(), Protocol: strings.ToLower(strings.TrimPrefix(value.GetProtocol().String(), "SERVICE_PROTOCOL_")), Revision: value.GetRevision(), Revoked: value.GetRevoked()}
}
func importData(value *servicev1.ServiceImport) ServiceImportData {
	return ServiceImportData{ID: value.GetImportId(), SourceContainerID: value.GetSourceContainerId(), SourceContainerInstanceID: value.GetSourceContainerInstanceId(), DestinationDeviceID: value.GetDestinationDeviceId(), DestinationExportID: value.GetDestinationExportId(), VirtualServiceName: value.GetVirtualServiceName(), Revision: value.GetRevision(), Revoked: value.GetRevoked()}
}

const emptyObjectSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false}`
const serviceExportSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["container_id","container_instance_id","port","protocol"],"properties":{"container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"container_instance_id":{"type":"string","minLength":1,"maxLength":128},"port":{"type":"integer","minimum":1,"maximum":65535},"protocol":{"enum":["tcp","http"]}}}`
const serviceImportSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["source_container_id","source_container_instance_id","destination_device_id","destination_export_id","destination_port","virtual_service_name"],"properties":{"source_container_id":{"type":"string","pattern":"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,127}$"},"source_container_instance_id":{"type":"string","minLength":1,"maxLength":128},"destination_device_id":{"type":"string","minLength":1,"maxLength":128},"destination_export_id":{"type":"string","minLength":1,"maxLength":128},"destination_port":{"type":"integer","minimum":1,"maximum":65535},"virtual_service_name":{"type":"string","pattern":"^[a-z][a-z0-9-]{0,62}$"},"capability":{"type":"string","minLength":16,"maxLength":16384},"mesh_id":{"type":"string","minLength":1,"maxLength":128},"connection_limit":{"type":"integer","minimum":1,"maximum":1000},"byte_limit":{"type":"integer","minimum":1,"maximum":1099511627776},"concurrency_limit":{"type":"integer","minimum":1,"maximum":256}},"oneOf":[{"required":["capability"],"not":{"required":["mesh_id"]}},{"required":["mesh_id","connection_limit","byte_limit","concurrency_limit"],"not":{"required":["capability"]}}]}`
const serviceRouteRevokeSchema = `{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object","additionalProperties":false,"required":["kind","id","expected_revision"],"properties":{"kind":{"enum":["export","import"]},"id":{"type":"string","minLength":1,"maxLength":128},"expected_revision":{"type":"integer","minimum":1}}}`
