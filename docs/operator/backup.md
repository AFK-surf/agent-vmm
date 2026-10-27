# Offline backup and restore

This procedure requires the Host, Guest, and every writer to remain stopped during the copy.
A service restart alone does not establish that condition.
The complete backup includes identity and credentials. Protect it as secret data.

## Resolve the source

1. Read `agent-vmm inspect --json` for the exact service.
2. Record the service user, Host application, state directory, state disk, and operation IDs.
3. Record every environment, registration, and appliance whose data the disk contains.
4. Confirm that the backup covers all those owners.

Default state roots are `~/Library/Application Support/AgentVMM` for the macOS service user and `/var/lib/agent-vmm` on Linux.
A configured Host can use different paths. Inspect the loaded service definition before use.

The state root contains the Guest disk, policy database and WAL files, lifecycle receipts, and any file-backed keys and credentials.
Copy the complete stopped state root and protect it as secret data.
Keychain items and Secure Enclave keys are outside this copy.
A Keychain-backed restore needs the original user's Keychain entries. Secure Enclave identity also needs the original Mac.

Preserve the installed release identity and a verified package that can read the saved schema.
Keep external controller recovery material with its owning controller.
Approved Host shares can reference directories outside this state root.
Back up their contents separately under their owners' approval.
Do not copy live Unix sockets as durable state.

## Create the backup

1. Stop new controller work through its owner.
2. Run `agent-vmm vm stop --request-id BACKUP_REQUEST --json`.
3. Require a successful terminal stop receipt and `stopped` from VM status.
4. Stop the exact Host service through its service owner.
5. On Linux, stop `agent-vmm-host.service` only after the explicit VM stop completes.
6. Confirm that no Host or VMM process holds the state disk or database.
7. Copy the complete state root into a separate protected backup directory.
8. Preserve numeric ownership, permissions, extended attributes, links, and the complete raw disk length.
9. Record file sizes and cryptographic hashes for the completed copy.
10. Compare the copied disk, database, key, and credential bytes with the stopped source.
11. Keep the backup private and independently encrypted.

Linux uses `KillMode=process`. The VMM can survive a Host service restart.
A stopped systemd unit alone is insufficient evidence for an offline backup.
Do not run a WAL checkpoint against a live database to simplify this procedure.
Keep the database and any existing WAL files from the same stopped copy.

## Restore and verify

1. Stop the target VM and Host service and confirm disk release.
2. Preserve the current target state before replacement.
3. Restore one complete backup into the resolved state root.
4. Restore numeric ownership and private directory and file modes.
5. Install the verified compatible Host package.
6. Start that exact service and observe the original startup operation.
7. Confirm the saved device identity, registrations, installations, environments, and volume identities.
8. Open an existing container session and read previously saved volume contents.
9. Confirm service routes and controller reconnect under their current fences.
10. Accept new work only after those checks succeed.

Do not combine a disk from one backup with keys or a database from another.
Do not run an older binary against state that a newer Host has changed.
A restore that predates controller revocation needs the controller's current authorization before reconnect.
Preserve both copies until the owners accept recovery.
