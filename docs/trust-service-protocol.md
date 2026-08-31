# Trust and service protocol contract

`api/trust/v1` and `api/service/v1` are the provider-neutral wire owners for
managed-controller and personal-mesh trust. Product identifiers, tenant fields,
local namespace identifiers, PINs, PAKE material, and private keys are not wire
fields.

## Signature input

V1 signs `agent-vmm/signature/v1\0`, the ASCII message type, one zero byte, and
the deterministic protobuf encoding of the message after its signature field is
cleared. Implementations must reject unknown fields and duplicate set elements;
set-like scalar fields must use ascending order. There are no map fields in a signed V1 message. The Go
reference is `pkg/signing.CanonicalInput`; its digest fixture is a cross-language
conformance vector, not a substitute for signature verification.

Signed message types are `DeviceRootRotation`, `MembershipCredential`,
`PersonalMeshDescriptor`, `EndpointObservation`, `RouteCapability`,
`RegistryOperation`, and `RegistryReceipt`. P-256 SEC1 public
keys use the compressed 33-byte form. ECDSA signatures use fixed-width 64-byte
`r || s` form with low-S normalization. SHA-256 is the only V1 digest.

## Limits and negotiation

- A mesh snapshot has at most 32 active members and 32 retained tombstones in
  a response. Tombstones remain durable until the mesh is deleted.
- Registry operations are at most 64 KiB. Endpoint observations are at most
  16 KiB and expire within 15 minutes. A route expires within one hour.
- Unknown authority classes, signature suites, route classes, ALPNs, fields, or
  feature requirements fail closed. Peers negotiate an explicit intersection;
  absence never implies support.
- The only V1 ALPN strings are `agent-vmm/pairing/1`,
  `agent-vmm/mesh-control/1`, and `agent-vmm/service-tunnel/1`.
- A rejected CAS returns the current revision but does not partially apply an
  operation. A lost side-effecting result is `unknown_outcome`; it is resolved
  by reading authoritative facts and is never replayed automatically.
- Registry operations are durable desired intent. They are the narrow exception
  to live-RPC replay: the connector may resubmit the same signed operation ID
  after an ambiguous result. The registry must return the same committed result
  without advancing revision twice. The host advances its queue cursor only
  after verifying and persisting the signed receipt and snapshot.

## Local custody and process boundary

`agent-vmm-host` generates a random stable device ID and owns the only root-key
provider. A headless system LaunchDaemon has no user-login Keychain context, so
macOS keeps the private key in the service user's mode-`0600` Agent VMM state
directory; FileVault owns offline disk protection. SQLite stores only compressed
public keys, revisions, custody metadata, signed rotations, and trust facts.
The service user is one failure domain: this design does not isolate the key
from arbitrary code already running as that user or as root.
An unavailable or mismatched stored root enters `NEEDS_REPAIR`; repair requires
explicit membership reset and creates a new device identity.

The operator, remote-compute, and trust APIs use separate Unix listeners.
`TrustService` is connector-only and requires a persistent 256-bit capability
stored in a mode-0600 regular file; same-UID access to the socket alone is not
authentication. It exposes signed proofs, bounded route authorization, and the
durable registry-operation queue, never private-key bytes or operator lifecycle
methods. Endpoint node keys remain data-worker-owned and independent of the
device root; the host only signs bounded endpoint observations.
The connector refreshes an unchanged endpoint before its 15-minute expiry by
re-signing the same generation with a later expiry. A same-generation update
may not change node ID, address digest, ALPNs, or features. It periodically
publishes that observation, fetches the exact current mesh revision and the
bounded endpoint projection, and asks the host to verify every remote endpoint
against current active membership before storing it.

## Authority and route isolation

Managed and personal-mesh authorities share messages and verification code but
not keys, membership state, or product claims. A personal route issuer must be
an active member in a fresh current snapshot. An endpoint proves reachability,
not authority. Every route binds source, destination, export, audience, policy
revision, generation, expiry, nonce, and budget.

Managed enrollment sends the platform-backed `DeviceIdentity` with the
one-time token. Salix consumes that token only in the same transaction that
persists a controller-signed membership credential, then returns the current
managed anchor and credential. The connector writes its bearer credential and
asks the restricted host trust service to verify and atomically install the
bundle; it deletes the bearer credential if host verification fails. Managed
credential policy revision identifies policy, not one hourly signature. A
byte-identical credential remains idempotent. A different fully verified
credential at the same policy revision may replace the stored payload only
after the stored credential expires; an unexpired substitution or invalid
signature fails closed. This lets a later registration renew the same device
without weakening the current credential fence or inventing a second revision.
Managed
route capabilities carry the signed source membership credential so a
destination can verify a remote source against its installed anchor without a
per-route control-plane lookup. Personal capabilities reject that embedded
managed credential.

`LOCAL`, `DEVICE`, `COMPUTE`, `LAN`, and `PUBLIC_HTTP` are the complete V1 route
class set. All follow `PREPARING -> ACTIVE -> DRAINING -> REVOKED`; expiry may
replace any non-revoked state. Revocation and container-instance replacement
fence all transports, including a same-host optimization.

The route class is part of both the signed personal route request and the
destination-signed capability. `DEVICE` listeners may bind only a loopback
literal. `LAN` listeners may bind only one explicit private literal that is
present on a local interface; wildcard, loopback, multicast, and non-local
addresses fail closed. Active listener sockets are process-owned and are
revoked, not reconstructed, after a host restart.

`ServiceRouteService.OpenServiceForward` requires the connector install
capability at the gRPC stream interceptor. Its first frame carries the exact
route/lease/generation, host-injected source environment and container instance,
and the stored capability. The destination re-checks ACTIVE state, route/lease
revision and generation, expiry, audience `agent-vmm/service-tunnel/1`, local
device identity, current source/destination container instances, and cumulative
byte budget. Export enumeration and mutation are not available to the connector.

## SPAKE2 dependency decision

Pairing uses the protocol-owner-approved
RFC 9382 `SPAKE2-P256-SHA256-HKDF-SHA256-HMAC-SHA256` suite from pinned
`pakery-spake2` and `pakery-crypto` releases. Those crates implement the RFC
9382 state machine, transcript encoding, group validation, key schedule, and
mutual confirmation; Agent VMM does not implement those primitives. The
dependency is MIT OR Apache-2.0, forbids unsafe code, zeroizes secret values,
and includes protocol/vector and constant-time testing, but its upstream notes
that it has not received an independent audit. The repository therefore pins
the exact release, runs dependency advisories/licenses plus fixed local
known-answer and failure vectors, and keeps pairing disabled if those checks do
not pass. The RustCrypto/PAKEs `spake2` crate was evaluated and rejected for
this contract because it only supplies an Ed25519 parameter set and explicitly
does not erase secrets from memory. The PAKE ephemeral scalar and the
P-256/SHA-256 device-root signing key have separate owners, lifetimes, domains,
and storage; no key material is reused merely because both use P-256.

`make audit-data-worker` verifies the exact pakery versions, repository and
MIT/Apache-2.0 license metadata and scans the complete lockfile with RustSec.
The 2026-08-12 scan found no vulnerability advisory. It reported the allowed
`RUSTSEC-2024-0436` unmaintained warning for `paste`, reachable only through the
Iroh Linux netlink dependency chain; this is not introduced by the PAKE suite
and remains tracked with the pinned Iroh upgrade decision.

`TrustAnchor` is an authority key set authenticated by the TLS enrollment
bootstrap. Field 7 (`signature`) is reserved in V1 because no verifier ever
accepted it. Offline authority rotation, if required later, uses a separate
previous-anchor-signed rotation message rather than an ornamental field.
