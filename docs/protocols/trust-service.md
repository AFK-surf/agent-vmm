# Trust and service protocol

`api/trust/v1` and `api/service/v1` own the provider-neutral trust and route messages.
They carry no environment, local namespace, PIN, PAKE secret, or private key fields.

## Signature input

V1 signs `agent-vmm/signature/v1`, one zero byte, the unqualified ASCII message name, another zero byte, and deterministic protobuf bytes.
For example, the name is `RouteCapability`, without its package prefix.
Canonical encoding clears the signature field and rejects unknown fields and duplicate set elements.
Set-like scalar fields use ascending order. Signed V1 messages contain no map fields.

Signed types are MembershipCredential, PersonalMeshDescriptor, EndpointObservation, PairingProof, RouteCapability,
RegistryOperation, RegistryReceipt, PersonalRouteRequest, and ApplianceLaunchClaim.
P-256 SEC1 public keys use compressed 33-byte form.
ECDSA signatures use fixed-width 64-byte `r || s` with low-S normalization.
V1 uses SHA-256.

[CanonicalInput](../../pkg/signing/canonical.go) defines the Go encoding.
[Shared signing vectors](../../api/trust/v1/testdata/signing-vectors.json) cover canonical bytes and cross-language signatures.
[Canonical tests](../../pkg/signing/canonical_test.go) consume those vectors.
A matching digest does not replace signature verification.

## Bounds and reconciliation

A snapshot contains at most 32 active members and 32 tombstones per response.
Revocation tombstones remain durable until mesh deletion.
Registry operations have a 64 KiB bound. Endpoint observations have a 16 KiB bound and expire within 15 minutes.
A route expires within one hour.
Unknown authority classes, signature suites, route classes, ALPNs, and signed fields fail verification.
Peers select an explicit supported feature intersection. Absence does not imply support.
The ALPNs are `agent-vmm/pairing/2`, `agent-vmm/mesh-control/1`, and `agent-vmm/service-tunnel/1`.

A rejected revision comparison returns current revision without partial application.
A lost side-effect result requires authoritative observation before another action.
Durable signed registry intent has a narrow replay rule.
The connector can resubmit the same operation ID after uncertainty.
The registry returns the same committed result without advancing revision twice.
The Host advances its queue only after it verifies and saves the signed receipt and snapshot.

## Local custody and endpoint ownership

The Host generates a stable random device ID and owns its root signing key.
Read [secret custody](../../SECURITY.md#assumptions) for Keychain, Secure Enclave, and file backends.
SQLite stores public keys, revisions, custody metadata, and trust facts.
A missing or mismatched root enters `NEEDS_REPAIR`.
Repair requires explicit membership reset and creates another device identity.

Operator, remote-compute, trust, service transport, and mesh-control listeners use separate private Unix sockets.
The private connector trust service exposes bounded proofs and the signed registry queue, without private key bytes or operator lifecycle methods.
Read [the security model](../design/security-model.md).

Data-worker endpoint keys have a separate owner and lifetime from the device root.
The Host signs bounded endpoint observations.
Refresh can extend an unchanged generation's expiry but cannot change its node, address digest, ALPNs, or features.
The connector publishes observations and fetches current mesh membership and endpoint projections.
The Host verifies remote endpoints against active membership before storage.

## Managed authority

Managed and personal authorities share encoding and verification code, with separate keys, membership state, and claims.
Managed enrollment carries public DeviceIdentity and a one-time token.
The controller consumes that token with its membership-credential commit and returns the authenticated anchor and credential.
The connector saves its bearer credential and asks the Host to verify and install the trust bundle atomically.
Failed Host verification removes that new bearer credential.

A byte-identical credential remains idempotent.
A different valid credential at the same policy revision can replace stored bytes only after the stored credential expires.
Unexpired substitution and invalid signatures fail.
Policy revision identifies approved policy rather than each signature renewal.
Managed capabilities carry the source credential for destination verification against its installed anchor.
Personal capabilities reject an embedded managed credential.

Appliance managed trust arrives through the current authenticated Bootstrap.
An appliance without a bundle has no managed route authority and receives no automatic trust refresh.
Refresh starts within 24 hours of membership expiry and retries failed or unchanged results after one hour.
Each call uses a fresh nonce, empty management seed, and one-minute deadline.
Renewal requires higher revisions with unchanged issuer, key, subject, and permission scope.
The same revision requires identical bytes.
Current installation, instance, configuration, and revocation fences apply at commit.
Membership remaining lifetime cannot exceed 30 days.
Anchor remaining lifetime cannot exceed 365 days. Its total span cannot exceed 365 days plus one minute.
Issuers must account for clock skew within those bounds.

## Personal route authority and transport

The destination issues a personal capability after it verifies current active source membership.
The destination must also remain active in the fresh snapshot.
An endpoint proves reachability, not authority.
Every capability binds source, destination, export, audience, policy, generation, expiry, nonce, and budget.
The signed personal request and capability both bind route class.

V1 route classes are `LOCAL`, `DEVICE`, `COMPUTE`, `LAN`, and `PUBLIC_HTTP`.
Routes progress through `PREPARING`, `ACTIVE`, `DRAINING`, and `REVOKED`. Expiry can end any non-revoked state.
Revocation and instance replacement fence every transport, including local optimized paths.

The default local device listener uses a private Unix socket.
An explicitly requested DEVICE TCP listener binds only a loopback literal.
A LAN listener binds one explicit private literal present on a local interface.
Wildcard, loopback, multicast, and nonlocal LAN bindings fail.
Active listeners are process-owned. Host restart revokes them instead of reconstructing live authority.

OpenServiceForward starts with exact route, lease, generation, source instance, and capability facts.
The destination checks active state, current revisions, expiry, service-tunnel audience, device identity, instances, and cumulative byte budget.
The connector cannot enumerate or revoke exports.
A known export lookup omits environment and container identities.
Environment owners use AgentRuntimeService for export mutation.

## Pairing dependency

Pairing uses the RFC 9382 `SPAKE2-P256-SHA256-HKDF-SHA256-HMAC-SHA256` suite from the pakery crates.
Those dependencies own the PAKE primitives, transcript, key schedule, and mutual confirmation.
Agent VMM owns the bounded exchange, device binding, user confirmation, and membership publication.
PAKE ephemeral secrets and the device-root signing key have separate lifetimes, domains, and storage.
Their shared P-256 curve does not permit key reuse.

`make audit-data-worker` checks the lockfile against RustSec advisories.
Known-answer and failure vectors exercise the selected pairing suite.
These build checks do not create a runtime pairing admission gate.
The dependency has no claimed independent security audit.
