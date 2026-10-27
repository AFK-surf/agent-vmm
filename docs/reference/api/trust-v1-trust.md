# `agentvmm.trust.v1`

Source: `trust/v1/trust.proto`.

## Service `PersonalMeshRegistryService`

PersonalMeshRegistryService is provider-neutral. Registry implementations
linearize device-signed operations but never become route issuers.

### `CreateMesh`

CreateMesh commits the initial signed personal mesh operation and returns its signed registry
receipt.

`.agentvmm.trust.v1.CreateMeshRequest` → `.agentvmm.trust.v1.CreateMeshResponse`

### `CommitJoin`

CommitJoin applies one signed membership join under the expected registry revision.

`.agentvmm.trust.v1.CommitJoinRequest` → `.agentvmm.trust.v1.CommitJoinResponse`

### `CommitRevoke`

CommitRevoke applies one signed membership revocation and retains its durable tombstone.

`.agentvmm.trust.v1.CommitRevokeRequest` → `.agentvmm.trust.v1.CommitRevokeResponse`

### `GetSnapshot`

GetSnapshot reads the signed mesh snapshot required for current membership verification.

`.agentvmm.trust.v1.GetSnapshotRequest` → `.agentvmm.trust.v1.GetSnapshotResponse`

### `PublishEndpoint`

PublishEndpoint publishes a member-signed bounded endpoint observation without granting route
authority.

`.agentvmm.trust.v1.PublishEndpointRequest` → `.agentvmm.trust.v1.PublishEndpointResponse`

### `ListEndpoints`

ListEndpoints reads bounded endpoint observations for current mesh reachability checks.

`.agentvmm.trust.v1.ListEndpointsRequest` → `.agentvmm.trust.v1.ListEndpointsResponse`

## Message `DeviceIdentity`

DeviceIdentity publishes the stable device identity and current root public key without private
key material.


## Message `AuthorityDescriptor`

AuthorityDescriptor names the authority class and revision used to interpret a signed trust
object.


## Message `VerificationKey`

VerificationKey publishes a verification key with its algorithm and key identity.


## Message `TrustAnchor`

TrustAnchor contains the authority key set authenticated by the enrollment bootstrap.


## Message `MembershipCredential`

MembershipCredential signs one device's managed membership scope and expiry under its authority.


## Message `PersonalMeshDescriptor`

PersonalMeshDescriptor signs the personal mesh identity and its authorized membership policy.


## Message `MeshMembership`

MeshMembership records an active member's public identity and membership generation.


## Message `MeshTombstone`

MeshTombstone retains a revoked member generation so an old membership cannot return through
stale state.


## Message `RegistryReceipt`

RegistryReceipt signs the committed operation result and registry revision for idempotent
reconciliation.


## Message `MeshSnapshot`

MeshSnapshot carries bounded current membership and retained revocation facts at one registry
revision.


## Message `EndpointObservation`

EndpointObservation signs bounded device reachability. It does not grant service route
authority.


## Message `PairingProof`

PairingProof is signed by the device root named in the PAKE-bound context.
It is exchanged only after PAKE key confirmation and is single-use through
session_id/invite_revision plus the registry operation CAS.


## Message `RouteBudget`

RouteBudget bounds cumulative connections, bytes, and concurrent service streams.


## Message `RouteCapability`

RouteCapability signs exact source, destination, export, audience, generations, expiry, and
transport budgets.


## Message `RegistryOperation`

RegistryOperation signs durable desired membership intent under one operation ID and expected
revision.


## Message `GetDeviceIdentityRequest`

GetDeviceIdentityRequest carries inputs for an operation that reads public device identity and
custody state without exposing private key bytes.


## Message `SignEndpointObservationRequest`

SignEndpointObservationRequest asks the device root to sign bounded data-worker reachability
without granting route authority.


## Message `ApplyMeshEndpointObservationRequest`

ApplyMeshEndpointObservationRequest supplies a peer endpoint observation for current
membership and signature verification.


## Message `GetEndpointObservationRequest`

GetEndpointObservationRequest selects a verified member endpoint projection for the requested
mesh and device.


## Message `ListPersonalMeshesRequest`

ListPersonalMeshesRequest carries inputs for an operation that reads the local projection of
verified personal mesh state.


## Message `PersonalMeshState`

PersonalMeshState reports local verified mesh state and its registry freshness.


## Message `ListPersonalMeshesResponse`

ListPersonalMeshesResponse carries results from an operation that reads the local projection
of verified personal mesh state.


## Message `InstallManagedTrustRequest`

InstallManagedTrustRequest supplies the enrollment-authenticated anchor and device membership
credential for atomic verification and storage.


## Message `InstallManagedTrustResponse`

InstallManagedTrustResponse reports the committed authority and policy revisions after managed
trust installation.


## Message `ApplyMeshSnapshotRequest`

ApplyMeshSnapshotRequest supplies signed registry state with the expected current revision for
local verification.


## Message `ApplyMeshSnapshotResponse`

ApplyMeshSnapshotResponse reports the verified mesh revision committed by the Host.


## Message `PendingRegistryOperation`

PendingRegistryOperation retains signed desired intent until the Host verifies and persists its
registry receipt.


## Message `PullRegistryOperationsRequest`

PullRegistryOperationsRequest selects a bounded page after the Host-owned durable queue
sequence.


## Message `PullRegistryOperationsResponse`

PullRegistryOperationsResponse returns signed desired intent that remains pending until its
verified registry receipt is committed.


## Message `CommitRegistryOperationRequest`

CommitRegistryOperationRequest supplies the original operation identity and signed snapshot
required to advance its queue receipt.


## Message `CreateMeshRequest`

CreateMeshRequest carries inputs for an operation that commits the initial signed personal
mesh operation and returns its signed registry receipt.


## Message `CommitJoinRequest`

CommitJoinRequest carries inputs for an operation that applies one signed membership join
under the expected registry revision.


## Message `CommitRevokeRequest`

CommitRevokeRequest carries inputs for an operation that applies one signed membership
revocation and retains its durable tombstone.


## Message `GetSnapshotRequest`

GetSnapshotRequest carries inputs for an operation that reads the signed mesh snapshot
required for current membership verification.


## Message `PublishEndpointRequest`

PublishEndpointRequest carries inputs for an operation that publishes a member-signed bounded
endpoint observation without granting route authority.


## Message `PublishEndpointResponse`

PublishEndpointResponse carries results from an operation that publishes a member-signed
bounded endpoint observation without granting route authority.


## Message `CreateMeshResponse`

CreateMeshResponse carries results from an operation that commits the initial signed personal
mesh operation and returns its signed registry receipt.


## Message `CommitJoinResponse`

CommitJoinResponse carries results from an operation that applies one signed membership join
under the expected registry revision.


## Message `CommitRevokeResponse`

CommitRevokeResponse carries results from an operation that applies one signed membership
revocation and retains its durable tombstone.


## Message `GetSnapshotResponse`

GetSnapshotResponse carries results from an operation that reads the signed mesh snapshot
required for current membership verification.


## Message `ListEndpointsRequest`

ListEndpointsRequest carries inputs for an operation that reads bounded endpoint observations
for current mesh reachability checks.


## Message `ListEndpointsResponse`

ListEndpointsResponse carries results from an operation that reads bounded endpoint
observations for current mesh reachability checks.


## Enum `SignatureSuite`

### `SIGNATURE_SUITE_UNSPECIFIED = 0`

### `SIGNATURE_SUITE_P256_SHA256 = 1`

## Enum `AuthorityClass`

### `AUTHORITY_CLASS_UNSPECIFIED = 0`

### `AUTHORITY_CLASS_MANAGED_CONTROLLER = 1`

### `AUTHORITY_CLASS_PERSONAL_MESH = 2`

## Enum `CapabilityKind`

### `CAPABILITY_KIND_UNSPECIFIED = 0`

### `CAPABILITY_KIND_SERVICE_ROUTE = 1`

### `CAPABILITY_KIND_MESH_CONTROL = 2`

## Enum `MeshPermission`

### `MESH_PERMISSION_UNSPECIFIED = 0`

### `MESH_PERMISSION_USE_SERVICES = 1`

### `MESH_PERMISSION_MANAGE_MEMBERS = 2`

## Enum `MeshMembershipState`

### `MESH_MEMBERSHIP_STATE_UNSPECIFIED = 0`

### `MESH_MEMBERSHIP_STATE_PENDING_JOIN = 1`

### `MESH_MEMBERSHIP_STATE_ACTIVE = 2`

### `MESH_MEMBERSHIP_STATE_REVOKED = 3`

## Enum `RegistryOperationKind`

### `REGISTRY_OPERATION_KIND_UNSPECIFIED = 0`

### `REGISTRY_OPERATION_KIND_GENESIS = 1`

### `REGISTRY_OPERATION_KIND_JOIN = 2`

### `REGISTRY_OPERATION_KIND_REVOKE = 3`

## Enum `PairingRole`

### `PAIRING_ROLE_UNSPECIFIED = 0`

### `PAIRING_ROLE_MANAGER = 1`

### `PAIRING_ROLE_JOINER = 2`
