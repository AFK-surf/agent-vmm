# Agent access

## MCP appliance

Agent VMM exposes MCP only through the bundled appliance's loopback Streamable HTTP endpoint. Enable the appliance in Agent Access, then configure a client with its generated endpoint URL. The URL contains a per-client opaque binding and creates one appliance-owned allocation; it is not an OperatorService or runtime credential.

For Codex, the app runs:

```text
codex mcp add agent-vmm --url http://127.0.0.1:<port>/mcp?access_token=<binding>
```

The MCP appliance owns its tools, allocations, leases, runtime sessions, cleanup policy, and HTTP authentication. It does not expose stdio MCP, raw TCP forwarding, or an external controller connector. Containers run without Linux capabilities; use images and volumes prepared for their fixed non-root UID/GID.

Image references use the canonical OCI name returned by `image_pull`; short Docker Hub names normalize to `docker.io/library/...`. `container_run` preserves the image ENTRYPOINT and CMD unless explicitly overridden.

## Cross-device services

The host supervises the data worker and owns pairing, trust, and service authorization. The data worker is not a LaunchAgent and does not provide a controller-management interface.
