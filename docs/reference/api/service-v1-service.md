# `agentvmm.service.v1`

Source: `service/v1/service.proto`.

## Service `ServiceRouteService`

ServiceRouteService serves instance-fenced service transport.

### `OpenServiceForward`

OpenServiceForward verifies current route, lease, generation, instances, and budgets before
forwarding bytes.

`stream .agentvmm.service.v1.OpenServiceForwardRequest` → `stream .agentvmm.service.v1.OpenServiceForwardResponse`

## Service `MeshControlService`

MeshControlService issues destination-authorized personal routes after current membership
verification.

### `RequestPersonalRoute`

RequestPersonalRoute asks the destination to verify active source membership and sign a
bounded personal capability.

`.agentvmm.service.v1.RequestPersonalRouteRequest` → `.agentvmm.service.v1.RequestPersonalRouteResponse`

## Message `ServiceExport`

ServiceExport authorizes one owned container ID and port while tracking its current instance
projection.


## Message `ApplianceFrontendDestination`

ApplianceFrontendDestination selects a manifest-declared frontend on one installation rather
than a raw endpoint.


## Message `ServiceImport`

ServiceImport records a current source container's approved connection to one destination
export.


## Message `AccessRoute`

AccessRoute records the authorized route state and generation independently of its live
transport.


## Message `RouteLease`

RouteLease bounds one route use under its current revision, expiry, and budgets.


## Message `OpenServiceForwardHeader`

OpenServiceForwardHeader binds the first transport frame to exact route, lease, source,
destination, and capability facts.


## Message `OpenServiceForwardRequest`

OpenServiceForwardRequest carries inputs for an operation that verifies current route, lease,
generation, instances, and budgets before forwarding bytes.


## Message `OpenServiceForwardResponse`

OpenServiceForwardResponse carries results from an operation that verifies current route,
lease, generation, instances, and budgets before forwarding bytes.


## Message `RequestPersonalRouteResponse`

RequestPersonalRouteResponse carries results from an operation that asks the destination to
verify active source membership and sign a bounded personal capability.


## Message `RequestPersonalRouteRequest`

RequestPersonalRouteRequest carries inputs for an operation that asks the destination to
verify active source membership and sign a bounded personal capability.


## Message `ListServiceExportsResponse`

ListServiceExportsResponse returns the exports visible to the environment owner.


## Enum `TransportAlpn`

These are the only V1 ALPNs. Product or authority names never appear in an
ALPN and feature negotiation must fail closed for an unknown value.

### `TRANSPORT_ALPN_UNSPECIFIED = 0`

### `TRANSPORT_ALPN_PAIRING_V1 = 1`

### `TRANSPORT_ALPN_MESH_CONTROL_V1 = 2`

### `TRANSPORT_ALPN_SERVICE_TUNNEL_V1 = 3`

## Enum `RouteClass`

### `ROUTE_CLASS_UNSPECIFIED = 0`

### `ROUTE_CLASS_LOCAL = 1`

### `ROUTE_CLASS_DEVICE = 2`

### `ROUTE_CLASS_COMPUTE = 3`

### `ROUTE_CLASS_LAN = 4`

### `ROUTE_CLASS_PUBLIC_HTTP = 5`

## Enum `RouteState`

### `ROUTE_STATE_UNSPECIFIED = 0`

### `ROUTE_STATE_PREPARING = 1`

### `ROUTE_STATE_ACTIVE = 2`

### `ROUTE_STATE_DRAINING = 3`

### `ROUTE_STATE_REVOKED = 4`

### `ROUTE_STATE_EXPIRED = 5`

## Enum `ServiceProtocol`

### `SERVICE_PROTOCOL_UNSPECIFIED = 0`

### `SERVICE_PROTOCOL_TCP = 1`

### `SERVICE_PROTOCOL_HTTP = 2`
