# macOS interface adapter

This private package implements the netdev API that netwatch 0.19.3 uses on macOS.
It reads BSD interface facts with getifaddrs and IPv6 address flags with SIOCGIFAFLAG_IN6.
It does not read optional display names or call SystemConfiguration.
Netwatch keeps ownership of its existing routing-table parser.
The outbound source-address lookup uses UDP connect without sending application data.

Linux builds use registry netdev 0.47.0 through a re-export.
Netwatch keeps its separate netdev 0.45 dependency on Windows.
The distinct minor version prevents the Cargo patch from resolving to itself.
This package does not implement the full public netdev API.
Recheck netwatch's consumed API when updating that dependency.

The source-address lookup and IPv6 flag mapping adapt netdev 0.46.3 code.
Upstream: https://github.com/shellrow/netdev/tree/v0.46.3
The required copyright and MIT license are in the repository [NOTICE](../../NOTICE).
Other code is Copyright (c) 2026 AFK AI, Inc., AGPL-3.0-only.
