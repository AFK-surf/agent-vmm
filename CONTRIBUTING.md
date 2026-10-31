# Contributing

## Toolchain

Use the Go toolchain from `go.mod`.
Install `buf`, Rust and Cargo, `jq`, and Docker Buildx.
For macOS development, install Xcode and `xcodegen`.
Install the Swift protobuf and gRPC generators before you generate Swift code.
The Go generators use the `tool` versions from `go.mod`.

Read [the Guest build requirements](docs/build.md#guest).
SBOM tools also need `syft`. Guest extraction needs `unsquashfs`.

## Generate code

```sh
make proto
make proto-check
make swift-project
```

`make proto-check` runs protobuf lint.
`make proto-breaking` compares API compatibility with `origin/master`.
Set `PROTO_BASE_REF` to select another commit or ref. Pull request CI uses its base commit.

`make check-generated` compares regenerated Go files with the source tree.
`make check-swift-generated` compares regenerated Swift files with the source tree.

## Check a change

```sh
make check GOTESTFLAGS=-race
make test-data-worker lint-data-worker
cargo check --locked --manifest-path crates/agent-vmm-proto/Cargo.toml
```

Run Swift tests when you change the debugging app:

```sh
xcodebuild test -project apps/macos/AgentVMM/AgentVMM.xcodeproj \
  -scheme AgentVMM -destination 'platform=macOS' CODE_SIGNING_ALLOWED=NO
```

## Test scope

During development, run the smallest test set that covers the changed behavior.
Run broader phase checks after focused tests pass.
Run the full suite at final convergence or for a justified broad regression check.

Guest tests for fixed runtime paths, numeric ownership, and DNS port 53 require root.
CI compiles race test binaries as the runner user, then runs them with `sudo`.
Kernel network tests use a private network namespace. Other tests run without root.

`make e2e` needs a macOS machine that can boot Virtualization.framework Guests.
It writes `.build/test-results/e2e/report.md`.
Read [the external controller fixture contract](test/e2e/README.md) for cross-repository tests.

`make tla` runs the models. Read [the model testing rules](docs/contributing/testing.md#protocol-and-model-checks).

## Review standards

Optimize the complete design. Refactor or delete code when this reduces maintenance cost.
Avoid excessive design, defensive code, and local patches that preserve obsolete structure.
Compare the branch with `main` and remove low-quality generated code.
Use current best practices. The project has no production release.

Remove comments that conflict with local style or repeat evident code.
Remove checks from trusted paths unless they protect an independent failure boundary.
A build or runtime check must change a real user, operator, or deployer decision.
Test output, model output, and metadata presence alone do not justify a check.

Reuse an existing type when a new type does not improve maintenance.
Extend an existing test when a separate test does not improve its scope.
Match each test filename to its scope. Follow the surrounding code style.
Review changed production files over 500 lines and split distinct responsibilities.
Review components with one direct consumer. Merge them when separation has no clear value.
Delete obsolete components and implementations.
Repeat this audit after all automated work passes.

Do not put secrets in command arguments.
Do not use fixed filenames in shared temporary directories.

## CI

All jobs run without repository secrets.
Runner variables can select Linux, Linux arm64, and macOS runners.
Their defaults are GitHub-hosted Ubuntu 24.04 and macOS 15.
PR jobs use a local builder.
Only trusted branch pushes, schedules, and dispatches can use a persistent Blacksmith builder.
This repository does not publish binaries.

## Writing style

Use STE-flavored prose from [the project skill](.codex/skills/ste-writing/SKILL.md).
Use strict mode for procedures and safety text.

## Repository conventions

`.context/` contains ignored planning documents. Production and test code must not read or write it.
When a fix addresses a repeatable operational failure, update its runbook in the same change.
Do not add one-time failures or validation diaries to the runbook.
Keep current procedures aligned with their code owners.

## License of contributions

Contributions use `AGPL-3.0-or-later`. The project copyright holder is AFK AI, Inc.
Preserve third-party notices and the licenses of derived files.
