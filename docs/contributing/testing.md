# Testing

Use [CONTRIBUTING.md](../../CONTRIBUTING.md#test-scope) for test scope and required toolchains.

## Runtime and kernel tests

Guest path, numeric-owner, quota, and port-53 tests need root.
Network kernel regressions run in a private network namespace.

Use [the build guide](../build.md#guest) for the Guest image smoke test.
An opt-in test that skips without configuration does not validate a deployed integration.

## Protocol and model checks

Use `make proto` after public protocol changes and inspect the generated clients and references.
Public messages describe authority and lifetime. The Rust Guest protocol stays private.
Use the shared signing vectors for cross-language canonical input and signature checks.
A generated source or metadata-presence check does not prove runtime interoperability.

`make tla` runs [the model roster](../../spec/tla/README.md).
Model protocols together only when they share a stated invariant.
Run each negative configuration separately.
Model checking is a design check, not a product gate.

## Environment evidence

Record the exact artifact, source revision, environment, and exercised behavior with runtime results.
A source-level test cannot stand in for a real MCP client tool call, Guest boot, or backup restore.
Do not report unrun deployment or recovery procedures as passed.
