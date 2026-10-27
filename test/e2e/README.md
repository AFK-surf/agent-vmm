# External controller fixtures

The opt-in bridge tests run a temporary Host and Guest under an external controller fixture.
Set `AGENT_VMM_EXTERNAL_E2E_CONFIG` to a private JSON file.
Without this variable, the tests skip.

| Field | Meaning |
| --- | --- |
| `remote_endpoint` | Controller endpoint for the pressure fixture. |
| `ca_file` | Path to the PEM certificate authority for that endpoint. |
| `archive_path` | Path to the Linux arm64 OCI archive imported by the fixture. |
| `registrations` | Pressure registrations, each with `id` and a base64 `credential`. |

The pressure fixture uses six isolated registrations.
The settings fixture needs only `archive_path` at startup.
Its controller drives enrollment and lifecycle operations through the command files.

Run the selected test with verbose output:

```sh
AGENT_VMM_EXTERNAL_E2E_CONFIG=FIXTURE go test -tags=e2e ./test/e2e \
  -run '^TestExternalPressureBridgeE2E$' -v -count=1
```

The settings entry point is `TestExternalSettingsBridgeE2E`.
The driver must see `=== RUN` for the selected test before it waits for readiness.
If the process exits before that line, fail immediately.

The Host writes `FIXTURE.host-ready` after it boots and publishes its fixture endpoints.
The settings driver writes JSON requests under `FIXTURE.commands`.
A request contains `args` and `stdin`. Its `.request` filename identifies the response.
The Host writes the matching result file after the operation settles.
Creating `FIXTURE.stop` asks the fixture to stop.
Keep fixture files private. They contain credentials and local socket paths.

## Installation chain

`TestInstallationChainE2E` exercises real launchd and isolated Host installation with ad-hoc development packages.
It uses helpers built with the `e2e` tag and an isolated home and label.
It refuses the production label and home. Its Guest still requires a fully backed 64 GiB disk.

```sh
AGENT_VMM_E2E_INSTALL_CHAIN=1 AGENT_VMM_E2E_BUNDLE=/path/to/guest \
  go test -tags=e2e ./test/e2e -run '^TestInstallationChainE2E$' -count=1 -v -timeout 90m
```

The optional output variables `AGENT_VMM_E2E_INSTALL_CHAIN_OUTPUT_A` and `_B` select reusable test packages.
Those packages must carry the expected E2E release IDs and tagged helpers.
The test covers publication, install, refused downgrade, retryable supersession, preserve update, and preserve uninstall.
It is a manual report-only test.
