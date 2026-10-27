package store

const currentSchema = `CREATE TABLE vm_config (
			id INTEGER PRIMARY KEY CHECK (id = 1), instance_id TEXT NOT NULL,
			bundle_directory TEXT NOT NULL, instance_directory TEXT NOT NULL,
			state_disk_identity TEXT NOT NULL, cpu_count INTEGER NOT NULL,
			memory_bytes INTEGER NOT NULL, state_bytes INTEGER NOT NULL
		);
CREATE TABLE environments (
			id TEXT PRIMARY KEY, namespace_id TEXT NOT NULL UNIQUE, display_name TEXT NOT NULL,
			pids INTEGER NOT NULL,
			disk_bytes INTEGER NOT NULL, egress_mode INTEGER NOT NULL,
			policy_revision INTEGER NOT NULL DEFAULT 1, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		, owner_kind INTEGER NOT NULL DEFAULT 1 CHECK(owner_kind IN (1, 2)), registration_id TEXT CHECK((owner_kind = 1 AND registration_id IS NULL) OR (owner_kind = 2 AND registration_id IS NOT NULL)), allocation_id TEXT CHECK((owner_kind = 1 AND allocation_id IS NULL) OR (owner_kind = 2 AND allocation_id IS NOT NULL)), allocation_state INTEGER CHECK((owner_kind = 1 AND allocation_state IS NULL) OR (owner_kind = 2 AND allocation_state IN (1, 2, 3, 4))), allocation_generation INTEGER NOT NULL DEFAULT 0);
CREATE TABLE sessions (
			id TEXT PRIMARY KEY, environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			socket_path TEXT NOT NULL UNIQUE, expires_at INTEGER NOT NULL, revoked_at INTEGER
		, integration_id TEXT REFERENCES agent_integrations(id), origin INTEGER NOT NULL DEFAULT 0 CHECK(origin IN (0, 1, 2)), created_at INTEGER NOT NULL DEFAULT 0, connected_at INTEGER, last_activity_at INTEGER, closed_at INTEGER, terminal_reason TEXT NOT NULL DEFAULT '');
CREATE TABLE grants (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, source_namespace_id TEXT NOT NULL,
			target_namespace_id TEXT NOT NULL, methods TEXT NOT NULL, expires_at INTEGER,
			revoked_at INTEGER, revision INTEGER NOT NULL DEFAULT 1
		);
CREATE TABLE volume_policies (
			volume_id TEXT NOT NULL, owner_namespace_id TEXT NOT NULL REFERENCES environments(namespace_id) ON DELETE CASCADE,
			revision INTEGER NOT NULL DEFAULT 1,
			PRIMARY KEY (owner_namespace_id, volume_id)
		);
CREATE TABLE operations (
			id TEXT PRIMARY KEY, kind TEXT NOT NULL, environment_id TEXT NOT NULL,
			state INTEGER NOT NULL, stage TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
			error_reason TEXT NOT NULL DEFAULT '', error_message TEXT NOT NULL DEFAULT '', output_path TEXT NOT NULL DEFAULT '',
			started_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		, request_sha256 BLOB, receipt BLOB);
CREATE TABLE audit (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT, actor TEXT NOT NULL, target TEXT NOT NULL,
			method TEXT NOT NULL, grant_id TEXT NOT NULL, request_id TEXT NOT NULL,
			result_code INTEGER NOT NULL, reason TEXT NOT NULL, occurred_at INTEGER NOT NULL
		);
CREATE TABLE remote_registrations (
			id TEXT PRIMARY KEY, endpoint TEXT NOT NULL, display_name TEXT NOT NULL,
			trust_bundle BLOB NOT NULL DEFAULT X'', scope_digest TEXT NOT NULL DEFAULT '',
			source_policy_revision INTEGER NOT NULL DEFAULT 0 CHECK(source_policy_revision >= 0),
			per_env_pids INTEGER NOT NULL CHECK(per_env_pids > 0),
			per_env_disk_bytes INTEGER NOT NULL CHECK(per_env_disk_bytes > 0),
			max_egress_mode INTEGER NOT NULL CHECK(max_egress_mode IN (1, 2)),
			stop_grace_seconds INTEGER NOT NULL CHECK(stop_grace_seconds > 0),
			revision INTEGER NOT NULL DEFAULT 1, state INTEGER NOT NULL CHECK(state IN (1, 2, 3)),
			connection_state INTEGER NOT NULL DEFAULT 1 CHECK(connection_state IN (1, 2, 3)),
			connection_instance TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		, enrollment_state INTEGER NOT NULL DEFAULT 3 CHECK(enrollment_state IN (1, 2, 3, 4)), enrollment_error_code TEXT NOT NULL DEFAULT '', enrollment_error_message TEXT NOT NULL DEFAULT '', connector_phase INTEGER NOT NULL DEFAULT 1 CHECK(connector_phase BETWEEN 1 AND 7), connector_release TEXT NOT NULL DEFAULT '', connector_observed_at INTEGER NOT NULL DEFAULT 0, last_connected_at INTEGER NOT NULL DEFAULT 0, disconnected_at INTEGER NOT NULL DEFAULT 0, next_retry_at INTEGER NOT NULL DEFAULT 0, reconnect_attempt INTEGER NOT NULL DEFAULT 0, connector_failure_code TEXT NOT NULL DEFAULT '', connector_failure_message TEXT NOT NULL DEFAULT '', enrollment_request_id TEXT NOT NULL DEFAULT '', owner_kind INTEGER NOT NULL DEFAULT 1 CHECK(owner_kind IN (1, 2)), installation_id TEXT, inventory_revision INTEGER NOT NULL DEFAULT 1 CHECK(inventory_revision > 0), pool_policy_revision INTEGER NOT NULL DEFAULT 1 CHECK(pool_policy_revision > 0));
CREATE TABLE "agent_integrations" (
			id TEXT PRIMARY KEY, adapter_id TEXT NOT NULL, display_name TEXT NOT NULL,
			scope INTEGER NOT NULL CHECK(scope IN (1, 2, 3)), config_target TEXT NOT NULL,
			environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
			desired_state INTEGER NOT NULL CHECK(desired_state IN (1, 2)),
			config_state INTEGER NOT NULL CHECK(config_state BETWEEN 1 AND 8),
			config_fingerprint TEXT NOT NULL DEFAULT '', revision INTEGER NOT NULL DEFAULT 1,
			last_error_code TEXT NOT NULL DEFAULT '', last_error_message TEXT NOT NULL DEFAULT '',
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL, last_verified_at INTEGER,
			prepare_request_id TEXT NOT NULL UNIQUE
		);
CREATE TABLE device_identity (
			id INTEGER PRIMARY KEY CHECK(id = 1), device_id TEXT NOT NULL UNIQUE,
			root_public_key BLOB NOT NULL, root_key_revision INTEGER NOT NULL CHECK(root_key_revision > 0),
			custody TEXT NOT NULL CHECK(custody IN ('secure_enclave', 'keychain', 'file', 'test')),
			state INTEGER NOT NULL CHECK(state IN (1, 2)), endpoint_generation INTEGER NOT NULL DEFAULT 0,
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		);
CREATE TABLE device_repairs (
			request_id TEXT PRIMARY KEY, identity BLOB NOT NULL, repaired_at INTEGER NOT NULL
		);
CREATE TABLE trust_anchors (
			authority_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind IN (1, 2)),
			revision INTEGER NOT NULL CHECK(revision > 0), expires_at INTEGER NOT NULL,
			anchor BLOB NOT NULL, updated_at INTEGER NOT NULL
		);
CREATE TABLE membership_credentials (
			authority_id TEXT NOT NULL, subject_device_id TEXT NOT NULL, policy_revision INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, credential BLOB NOT NULL, updated_at INTEGER NOT NULL,
			PRIMARY KEY(authority_id, subject_device_id)
		);
CREATE TABLE mesh_descriptors (
			mesh_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind = 2),
			descriptor BLOB NOT NULL, created_at INTEGER NOT NULL
		);
CREATE TABLE mesh_snapshots (
			mesh_id TEXT PRIMARY KEY, owner_kind INTEGER NOT NULL CHECK(owner_kind = 2),
			revision INTEGER NOT NULL CHECK(revision > 0), policy_epoch INTEGER NOT NULL CHECK(policy_epoch > 0),
			expires_at INTEGER NOT NULL, snapshot BLOB NOT NULL, updated_at INTEGER NOT NULL
		);
CREATE TABLE mesh_tombstones (
			mesh_id TEXT NOT NULL, device_id TEXT NOT NULL, root_key_revision INTEGER NOT NULL,
			revoked_at_revision INTEGER NOT NULL, policy_epoch INTEGER NOT NULL, operation_digest BLOB NOT NULL,
			PRIMARY KEY(mesh_id, device_id)
		);
CREATE TABLE endpoint_observations (
			device_id TEXT PRIMARY KEY, root_key_revision INTEGER NOT NULL, endpoint_generation INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, observation BLOB NOT NULL, updated_at INTEGER NOT NULL
		);
CREATE TABLE route_denials (
			capability_id TEXT PRIMARY KEY, denied_at INTEGER NOT NULL, reason TEXT NOT NULL
		);
CREATE TABLE mesh_member_denials (
			mesh_id TEXT NOT NULL, device_id TEXT NOT NULL, root_key_revision INTEGER NOT NULL,
			operation_id TEXT NOT NULL UNIQUE, denied_at INTEGER NOT NULL, PRIMARY KEY(mesh_id, device_id)
		);
CREATE TABLE registry_operations (
			sequence INTEGER PRIMARY KEY AUTOINCREMENT, operation_id TEXT NOT NULL UNIQUE,
			mesh_id TEXT NOT NULL, expected_revision INTEGER NOT NULL, kind INTEGER NOT NULL,
			state INTEGER NOT NULL CHECK(state IN (1, 2)), operation BLOB NOT NULL,
			created_at INTEGER NOT NULL, completed_at INTEGER
		);
CREATE TABLE service_exports (
			export_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE,
			environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			container_id TEXT NOT NULL, container_instance_id TEXT NOT NULL, port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
			protocol INTEGER NOT NULL CHECK(protocol IN (1, 2)), revision INTEGER NOT NULL DEFAULT 1,
			revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		, container_generation_id TEXT NOT NULL DEFAULT '');
CREATE TABLE access_routes (
			route_id TEXT PRIMARY KEY, import_id TEXT NOT NULL UNIQUE REFERENCES service_imports(import_id) ON DELETE CASCADE,
			route_class INTEGER NOT NULL CHECK(route_class BETWEEN 1 AND 5), source_device_id TEXT NOT NULL,
			source_allocation_id TEXT, destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL,
			state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 5), revision INTEGER NOT NULL, generation INTEGER NOT NULL,
			expires_at INTEGER NOT NULL, source_environment_id TEXT NOT NULL, source_container_instance_id TEXT NOT NULL,
			policy_revision INTEGER NOT NULL, capability BLOB NOT NULL, updated_at INTEGER NOT NULL
		, source_installation_id TEXT NOT NULL DEFAULT '', source_boot_id TEXT NOT NULL DEFAULT '', destination_installation_id TEXT NOT NULL DEFAULT '', destination_boot_id TEXT NOT NULL DEFAULT '', destination_container_instance_id TEXT NOT NULL DEFAULT '', destination_grant_revision INTEGER NOT NULL DEFAULT 0);
CREATE TABLE route_leases (
			lease_id TEXT PRIMARY KEY, route_id TEXT NOT NULL UNIQUE REFERENCES access_routes(route_id) ON DELETE CASCADE,
			route_revision INTEGER NOT NULL, generation INTEGER NOT NULL, expires_at INTEGER NOT NULL,
			capability BLOB NOT NULL, created_at INTEGER NOT NULL
		);
CREATE TABLE personal_route_issuances (
			request_id TEXT PRIMARY KEY, mesh_id TEXT NOT NULL, source_device_id TEXT NOT NULL,
			destination_export_id TEXT NOT NULL, expires_at INTEGER NOT NULL, response BLOB NOT NULL, created_at INTEGER NOT NULL
		);
` + serviceListenersSchema + `
CREATE TABLE appliance_grants (
			installation_id TEXT PRIMARY KEY REFERENCES appliance_installations(id) ON DELETE CASCADE,
			revision INTEGER NOT NULL, system_pids INTEGER NOT NULL, system_disk_bytes INTEGER NOT NULL, per_env_pids INTEGER NOT NULL, per_env_disk_bytes INTEGER NOT NULL,
			egress_mode INTEGER NOT NULL CHECK(egress_mode IN (1, 2)),
			max_egress_mode INTEGER NOT NULL CHECK(max_egress_mode IN (1, 2)),
			updated_at INTEGER NOT NULL
		, lan_egress INTEGER NOT NULL DEFAULT 0 CHECK(lan_egress IN (0, 1)), host_tcp_endpoints TEXT NOT NULL DEFAULT '[]');
CREATE TABLE appliance_trust_authorities (
			authority_id TEXT PRIMARY KEY REFERENCES trust_anchors(authority_id) ON DELETE CASCADE,
			installation_id TEXT NOT NULL UNIQUE REFERENCES appliance_installations(id) ON DELETE CASCADE,
			registration_id TEXT NOT NULL UNIQUE REFERENCES remote_registrations(id) ON DELETE CASCADE,
			boot_id TEXT NOT NULL, container_instance TEXT NOT NULL, config_revision INTEGER NOT NULL,
			state INTEGER NOT NULL CHECK(state IN (1, 2)), updated_at INTEGER NOT NULL
		);
CREATE TABLE appliance_operations (
			id TEXT PRIMARY KEY, installation_id TEXT NOT NULL, request_id TEXT NOT NULL UNIQUE,
			kind INTEGER NOT NULL, state INTEGER NOT NULL, expected_revision INTEGER NOT NULL,
			deadline_unix_millis INTEGER NOT NULL, stage TEXT NOT NULL, error_reason TEXT NOT NULL DEFAULT '',
			error_message TEXT NOT NULL DEFAULT '', operation_phase INTEGER NOT NULL DEFAULT 0,
			terminal_reason INTEGER NOT NULL DEFAULT 0, receipt BLOB NOT NULL DEFAULT X'',
			created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		);
CREATE INDEX appliance_operations_installation_kind_state ON appliance_operations(installation_id,kind,state);
CREATE TABLE revoked_appliance_authorities (
			authority_id TEXT PRIMARY KEY, installation_id TEXT NOT NULL,
			revoked_at INTEGER NOT NULL
		);
CREATE TABLE "appliance_artifacts" (
		artifact_id TEXT PRIMARY KEY, canonical_hash BLOB NOT NULL UNIQUE,
		image_digest TEXT NOT NULL, image_reference TEXT NOT NULL, platform TEXT NOT NULL,
		product_id TEXT NOT NULL, release TEXT NOT NULL, manifest BLOB NOT NULL,
		created_at INTEGER NOT NULL
	);
CREATE TABLE local_disposals (
   target_kind TEXT NOT NULL CHECK(target_kind IN ('registration','environment')),
   target_id TEXT NOT NULL, operation_id TEXT NOT NULL REFERENCES operations(id),
   parent_id TEXT NOT NULL DEFAULT '', registration_id TEXT NOT NULL DEFAULT '',
   enrollment_request_id TEXT NOT NULL DEFAULT '', namespace_id TEXT NOT NULL DEFAULT '',
   target_revision INTEGER NOT NULL, completed INTEGER NOT NULL DEFAULT 0, allocation_id TEXT NOT NULL DEFAULT '', allocation_generation INTEGER NOT NULL DEFAULT 0,
   PRIMARY KEY(target_kind,target_id));
` + applianceFrontendsSchema + `
CREATE TABLE appliance_installations (
				id TEXT PRIMARY KEY, product_id TEXT NOT NULL,
				artifact_id TEXT NOT NULL REFERENCES appliance_artifacts(artifact_id),
				desired_enabled INTEGER NOT NULL DEFAULT 0 CHECK(desired_enabled IN (0, 1)),
				state INTEGER NOT NULL CHECK(state BETWEEN 1 AND 6), revision INTEGER NOT NULL DEFAULT 1,
				registration_id TEXT UNIQUE REFERENCES remote_registrations(id),
				data_volume_id TEXT NOT NULL UNIQUE, data_volume_bytes INTEGER NOT NULL,
				bootstrap_id TEXT NOT NULL DEFAULT '', current_boot_id TEXT NOT NULL DEFAULT '',
				current_container_instance TEXT NOT NULL DEFAULT '', created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
			, revoked_at INTEGER);
CREATE TABLE "service_imports" (
 import_id TEXT PRIMARY KEY,request_id TEXT NOT NULL UNIQUE,
 source_environment_id TEXT REFERENCES environments(id) ON DELETE CASCADE,
 source_container_id TEXT NOT NULL,source_container_instance_id TEXT NOT NULL,
 destination_device_id TEXT NOT NULL,destination_export_id TEXT NOT NULL,virtual_service_name TEXT NOT NULL,
 revision INTEGER NOT NULL DEFAULT 1,revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0,1)),
 created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
 destination_port INTEGER NOT NULL DEFAULT 0 CHECK(destination_port BETWEEN 0 AND 65535),
 source_installation_id TEXT REFERENCES appliance_installations(id) ON DELETE CASCADE,
 source_namespace_id TEXT NOT NULL DEFAULT '',
 request_sha256 BLOB,revoke_request_id TEXT UNIQUE,revoke_request_sha256 BLOB, destination_installation_id TEXT REFERENCES appliance_installations(id), destination_frontend_name TEXT NOT NULL DEFAULT '', source_container_generation_id TEXT NOT NULL DEFAULT '',
 CHECK((source_environment_id IS NOT NULL) != (source_installation_id IS NOT NULL)));
CREATE TABLE managed_containers (environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			 container_id TEXT NOT NULL, container_generation_id TEXT NOT NULL, desired_running INTEGER NOT NULL CHECK(desired_running IN (0,1)),
			 revision INTEGER NOT NULL, PRIMARY KEY(environment_id,container_id));
CREATE TABLE host_shares (
			id TEXT PRIMARY KEY,environment_id TEXT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
			source_path TEXT NOT NULL,filesystem_uuid TEXT NOT NULL,file_id TEXT NOT NULL,
			read_only INTEGER NOT NULL CHECK(read_only IN(0,1)),revision INTEGER NOT NULL,state INTEGER NOT NULL,
			unavailable_reason TEXT NOT NULL,operation_id TEXT NOT NULL);
CREATE INDEX audit_retention ON audit(occurred_at, sequence);
CREATE UNIQUE INDEX environments_remote_allocation ON environments(registration_id, allocation_id) WHERE owner_kind = 2;
CREATE INDEX sessions_integration ON sessions(integration_id, created_at);
CREATE UNIQUE INDEX remote_registration_enrollment_request ON remote_registrations(enrollment_request_id) WHERE enrollment_request_id != '';
CREATE INDEX service_exports_environment ON service_exports(environment_id, revoked, export_id);
CREATE UNIQUE INDEX remote_registration_installation ON remote_registrations(installation_id) WHERE installation_id IS NOT NULL;
CREATE INDEX disposal_sessions_pending ON sessions(environment_id,id) WHERE closed_at IS NULL;
CREATE INDEX local_disposals_pending ON local_disposals(completed,parent_id,target_id);
CREATE INDEX local_disposals_namespace ON local_disposals(namespace_id) WHERE namespace_id != '';
CREATE INDEX local_disposals_enrollment ON local_disposals(enrollment_request_id) WHERE enrollment_request_id != '';
CREATE UNIQUE INDEX allocation_disposal_scope ON local_disposals(registration_id,allocation_id,allocation_generation) WHERE allocation_id != '';
CREATE UNIQUE INDEX service_imports_active_appliance_alias ON service_imports(source_installation_id,virtual_service_name) WHERE revoked=0;
CREATE UNIQUE INDEX service_imports_active_alias ON service_imports(source_environment_id,virtual_service_name) WHERE revoked=0;
CREATE TRIGGER remote_registration_owner_insert BEFORE INSERT ON remote_registrations
			WHEN (NEW.owner_kind = 1 AND NEW.installation_id IS NOT NULL) OR (NEW.owner_kind = 2 AND NEW.installation_id IS NULL)
			BEGIN SELECT RAISE(ABORT, 'registration owner binding is invalid'); END;
CREATE TRIGGER remote_registration_owner_update BEFORE UPDATE OF owner_kind, installation_id ON remote_registrations
			WHEN (NEW.owner_kind = 1 AND NEW.installation_id IS NOT NULL) OR (NEW.owner_kind = 2 AND NEW.installation_id IS NULL)
			BEGIN SELECT RAISE(ABORT, 'registration owner binding is invalid'); END;
CREATE TRIGGER remote_inventory_insert AFTER INSERT ON environments WHEN NEW.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=NEW.updated_at WHERE id=NEW.registration_id; END;
CREATE TRIGGER remote_inventory_update AFTER UPDATE OF pids, disk_bytes, egress_mode, policy_revision, allocation_state ON environments WHEN NEW.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=NEW.updated_at WHERE id=NEW.registration_id; END;
CREATE TRIGGER remote_inventory_delete AFTER DELETE ON environments WHEN OLD.owner_kind = 2
			BEGIN UPDATE remote_registrations SET inventory_revision=inventory_revision+1, updated_at=strftime('%s','now')*1000000000 WHERE id=OLD.registration_id; END;
CREATE TRIGGER disposed_registration_insert BEFORE INSERT ON remote_registrations WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE target_kind='registration' AND
   (target_id=NEW.id OR (enrollment_request_id != '' AND enrollment_request_id=NEW.enrollment_request_id)))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_registration_update BEFORE UPDATE ON remote_registrations WHEN
   (NEW.state != 3 OR NEW.enrollment_state != 4 OR NEW.connection_instance != '') AND EXISTS (
   SELECT 1 FROM local_disposals WHERE target_kind='registration' AND target_id=NEW.id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_environment_insert BEFORE INSERT ON environments WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE (target_kind='environment' AND (target_id=NEW.id OR namespace_id=NEW.namespace_id))
    OR (target_kind='registration' AND target_id=NEW.registration_id))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_environment_update BEFORE UPDATE ON environments WHEN EXISTS (
   SELECT 1 FROM local_disposals WHERE (target_kind='environment' AND target_id=NEW.id)
    OR (target_kind='registration' AND target_id=NEW.registration_id))
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_session_insert BEFORE INSERT ON sessions WHEN EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_integration_update BEFORE UPDATE ON agent_integrations WHEN NEW.desired_state=1 AND EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_integration_insert BEFORE INSERT ON agent_integrations WHEN EXISTS (
   SELECT 1 FROM environments e JOIN local_disposals d ON
   (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
   WHERE e.id=NEW.environment_id)
   BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_service_exports_INSERT BEFORE INSERT ON service_exports WHEN NEW.revoked=0 AND EXISTS (
 SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.environment_id) BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_access_routes_INSERT BEFORE INSERT ON access_routes WHEN NEW.state NOT IN (4,5) AND (
 EXISTS (SELECT 1 FROM service_imports WHERE import_id=NEW.import_id AND revoked=1) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_service_exports_UPDATE BEFORE UPDATE ON service_exports WHEN NEW.revoked=0 AND EXISTS (
 SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.environment_id) BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_access_routes_UPDATE BEFORE UPDATE ON access_routes WHEN NEW.state NOT IN (4,5) AND (
 EXISTS (SELECT 1 FROM service_imports WHERE import_id=NEW.import_id AND revoked=1) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_grant_insert BEFORE INSERT ON grants WHEN EXISTS (
 SELECT 1 FROM local_disposals WHERE namespace_id != '' AND (namespace_id=NEW.source_namespace_id OR namespace_id=NEW.target_namespace_id)
 UNION ALL SELECT 1 FROM environments e JOIN local_disposals d ON d.target_kind='registration' AND d.target_id=e.registration_id
 WHERE e.namespace_id=NEW.source_namespace_id OR e.namespace_id=NEW.target_namespace_id)
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_allocation_insert BEFORE INSERT ON environments WHEN EXISTS (
          SELECT 1 FROM local_disposals WHERE allocation_id != '' AND registration_id=NEW.registration_id
            AND allocation_id=NEW.allocation_id AND allocation_generation=NEW.allocation_generation)
          BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_service_imports_INSERT BEFORE INSERT ON service_imports WHEN NEW.revoked=0 AND (
 EXISTS (SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.source_environment_id) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER disposed_service_imports_UPDATE BEFORE UPDATE ON service_imports WHEN NEW.revoked=0 AND (
 EXISTS (SELECT 1 FROM environments e JOIN local_disposals d ON (d.target_kind='environment' AND d.target_id=e.id) OR (d.target_kind='registration' AND d.target_id=e.registration_id)
 WHERE e.id=NEW.source_environment_id) OR EXISTS (SELECT 1 FROM service_exports WHERE export_id=NEW.destination_export_id AND revoked=1))
 BEGIN SELECT RAISE(ABORT,'LOCAL_DISPOSED'); END;
CREATE TRIGGER service_import_destination_insert BEFORE INSERT ON service_imports WHEN
			 NOT ((NEW.destination_export_id!='' AND NEW.destination_installation_id IS NULL AND NEW.destination_frontend_name='') OR
			 (NEW.destination_export_id='' AND NEW.destination_installation_id IS NOT NULL AND NEW.destination_frontend_name='webhook' AND NEW.source_environment_id IS NOT NULL AND NEW.destination_port=80))
			 BEGIN SELECT RAISE(ABORT,'invalid service destination'); END;
`

const serviceListenersSchema = `CREATE TABLE "service_listeners" (
			listener_id TEXT PRIMARY KEY, request_id TEXT NOT NULL UNIQUE, mesh_id TEXT NOT NULL,
			destination_device_id TEXT NOT NULL, destination_export_id TEXT NOT NULL,
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2, 3)), listen_address TEXT NOT NULL,
			route_id TEXT NOT NULL UNIQUE, capability_id TEXT NOT NULL, capability BLOB NOT NULL,
			endpoint BLOB NOT NULL, expires_at INTEGER NOT NULL, revision INTEGER NOT NULL,
			revoked INTEGER NOT NULL DEFAULT 0 CHECK(revoked IN (0, 1)), created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
		, local INTEGER NOT NULL DEFAULT 0 CHECK(local IN (0,1)), environment_id TEXT NOT NULL DEFAULT '', container_id TEXT NOT NULL DEFAULT '', target_port INTEGER NOT NULL DEFAULT 0);`

const applianceFrontendsSchema = `CREATE TABLE "appliance_frontends" (
			installation_id TEXT NOT NULL REFERENCES appliance_installations(id) ON DELETE CASCADE,
			name TEXT NOT NULL, transport INTEGER NOT NULL CHECK(transport IN (1, 2)),
			exposure INTEGER NOT NULL CHECK(exposure IN (1, 2, 3)), max_frame_bytes INTEGER NOT NULL,
			max_total_bytes INTEGER NOT NULL, deadline_seconds INTEGER NOT NULL, idle_seconds INTEGER NOT NULL,
			max_concurrent_streams INTEGER NOT NULL,
			PRIMARY KEY(installation_id, name), CHECK(exposure != 3 OR transport = 2));`
