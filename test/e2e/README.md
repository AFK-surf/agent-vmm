# External settings fixture

This test runs the Rust Host and Carrier in a temporary macOS home and launchd jobs.
Use a logged-in user on a Mac that can boot the Carrier Guest.

Build the ad-hoc app with `AGENT_VMM_PACKAGE_E2E=1`.
Pass the external driver configuration path and the packaged app.
Supply a public HTTPS URL for the Linux arm64 OCI archive used by the driver.

```sh
AGENT_VMM_EXTERNAL_E2E_CONFIG=FIXTURE \
AGENT_VMM_EXTERNAL_E2E_APP='OUTPUT/Agent VMM Host.app' \
AGENT_VMM_EXTERNAL_E2E_ARCHIVE_URL=https://example.net/fixture.tar \
make e2e
```

The test writes `FIXTURE.host-ready` after the isolated install succeeds.
The external driver writes JSON requests with `args` and `stdin` to `FIXTURE.commands`.
Each `.request` gets a `.response` with the lifecycle helper result or error.
The disconnect and reconnect commands restart only the isolated Host job.
The Carrier keeps ownership of the VM.
Create `FIXTURE.stop` to stop the test.
Keep the fixture and its commands private because they can contain credentials.
