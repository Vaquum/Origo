import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
_GOVERNANCE_DIR = _REPO_ROOT / 'contracts' / 'governance'
_AGENTS = _REPO_ROOT / 'AGENTS.md'
_TOP_LEVEL_PLAN = _REPO_ROOT / 'spec' / '1-top-level-plan.md'


_EXPECTED = {
    'storage-sql-discipline.json': {
        'scope': 'whole_system_and_all_future_development',
        'clickhouse_schema_and_query_logic_sql_first': True,
        'migrations_are_ordered_versioned_sql_files': True,
        'migration_ledger_table_required': True,
        'applied_migrations_are_immutable_and_forward_only': True,
        'production_core_query_paths_use_explicit_sql': True,
        'query_builder_abstractions_forbidden_in_phase1_core_paths': True,
        'clickhouse_native_sql_features_preferred': True,
        'downstream_handoff_must_preserve_arrow_polars_compatibility': True,
    },
    'serving-error-safety.json': {
        'scope': 'whole_system_and_all_future_development',
        'raw_api_default_mode': 'native',
        'raw_api_supported_modes': ['native', 'aligned_1s'],
        'fixed_status_error_map': [200, 202, 404, 409, 503],
        'unavailable_data_must_error': True,
        'strict_false_serves_with_warnings': True,
        'strict_true_fails_when_warnings_exist': True,
    },
    'integrity-durability.json': {
        'scope': 'whole_system_and_all_future_development',
        'event_log_plus_materialized_view_architecture_required': True,
        'wal_format': 'parquet_with_manifest_and_checksums',
        'schema_registry_and_versioning_required': True,
        'historical_corrections_append_only': True,
        'exchange_integrity_suite_required': [
            'schema_type_checks',
            'sequence_gap_checks',
            'anomaly_checks',
        ],
        'exchange_canonical_historical_truth_source': 'daily_files',
    },
    'rights-secrets.json': {
        'scope': 'whole_system_and_all_future_development',
        'source_rights_matrix_required': True,
        'rights_states': ['Hosted Allowed', 'BYOK Required', 'Ingest Only'],
        'missing_rights_state_fails_closed': True,
        'legal_or_access_blocked_sources_auto_defer_to_phase2': True,
        'hosted_allowed_requires_legal_signoff_before_external_serving_or_export': True,
        'credentials_provided_at_source_init_or_config_stage': True,
        'credentials_not_persisted_in_raw_form': True,
        'authenticated_links_require_tls_at_external_boundary': True,
        'internal_api_key_rotation_is_manual_only': True,
    },
    'runtime-recovery.json': {
        'scope': 'whole_system_and_all_future_development',
        'freshness_policy_uniform_from_source_publish_time': True,
        'runtime_overload_protection_uses_concurrency_and_queue_limits': True,
        'rollout_pattern': 'shadow_then_promote',
        'required_pr_quality_gates': ['style', 'type', 'contract', 'replay', 'integrity'],
        'dr_targets': {'rpo_minutes': 5, 'rto_minutes': 60},
        'dr_drills_quarterly': True,
        'audit_logs_immutable_with_one_year_retention': True,
        'alerting_baseline': 'logs_plus_discord_bot',
        'source_quarantine_policy': 'manual_only_for_now',
    },
    'ingestion-guarantees.json': {
        'scope': 'whole_system_and_all_future_development',
        'exactly_once_defined_at': 'canonical_source_event_identity',
        'canonical_source_event_identity_fields': [
            'source_id',
            'stream_id',
            'partition_id_or_equivalent_shard_key',
            'source_offset_or_deterministic_equivalent_event_key',
        ],
        'duplicate_source_events_must_be_idempotent': True,
        'no_miss_defined_by_contiguous_offsets_or_deterministic_cadence_checks': True,
        'detected_gap_is_hard_error': True,
        'gap_handling_fail_closed_with_quarantine_until_explicit_replay_or_recovery': True,
        'reconciliation_is_mandatory': True,
    },
    'raw-fidelity.json': {
        'scope': 'whole_system_and_all_future_development',
        'canonical_event_log_stores_payload_raw_and_payload_sha256_raw': True,
        'payload_json_is_derivative_and_reproducible_from_payload_raw': True,
        'canonical_ingest_preserves_source_field_values_as_delivered': True,
        'canonical_payload_numeric_representation_forbids_float': True,
        'legacy_projection_float_fields_are_explicit_migration_debt': True,
        'time_fields_preserve_highest_available_source_precision': True,
        'source_migration_requires_round_trip_fidelity_precision_proof': True,
        'run_volatile_processing_metadata_must_not_change_canonical_payload_hash': True,
    },
    'event-sourcing.json': {
        'scope': 'whole_system_and_all_future_development',
        'canonical_raw_truth': 'single_global_append_only_event_log',
        'canonical_event_format_requires_typed_envelope_plus_payload_raw_and_payload_sha256_raw': True,
        'canonical_raw_events_never_rewritten_or_deleted': True,
        'reconcile_resets_use_logical_partition_reset_boundaries': True,
        'live_reads_must_use_boundary_aware_active_view_contract': True,
        'canonical_writer_enforces_unique_source_event_identity_and_idempotent_retries': True,
        'canonical_ingest_runs_continuous_no_miss_reconciliation': True,
        'all_serving_paths_are_projection_driven_from_canonical_events': True,
        'projection_runtime_modes': ['clickhouse_materialized_views', 'python_projectors'],
        'projector_checkpoints_and_watermarks_stored_in_clickhouse': True,
        'persistent_aligned_aggregates_are_mandatory': True,
        'correction_semantics_v1': 'latest_truth_only',
        'freshness_sla_breach_default': 'serve_with_warning',
        'temporary_hosted_rights_must_be_exposed_as_provisional': True,
        'canonical_aligned_1s_aggregates_is_mandatory_aligned_sink': True,
    },
    'historical-surface.json': {
        'scope': 'whole_system_and_all_future_development',
        'native_is_canonical_baseline_layer_across_all_datasets': True,
        'native_behavior_must_be_uniform_to_maximum_feasible_degree': [
            'window_semantics',
            'filter_semantics',
            'strict_warning_error_semantics',
            'response_envelope_semantics',
        ],
        'historical_python_and_http_interfaces_use_shared_parameter_names_when_concepts_match': True,
        'historical_parameter_contract': [
            'mode',
            'start_date',
            'end_date',
            'n_latest_rows',
            'n_random_rows',
            'fields',
            'filters',
            'strict',
        ],
        'all_dataset_surfaces_are_historical_surfaces': ['native', 'aligned_1s'],
        'no_window_selector_defaults_to_full_available_history': True,
        'aligned_1s_mandatory_for_every_onboarded_dataset': True,
    },
    'source-onboarding.json': {
        'scope': 'whole_system_and_all_future_development',
        'every_new_source_must_deliver_native_and_aligned_1s_before_slice_closeout': True,
        'source_onboarding_proof_requires_acceptance_and_replay_determinism_for_both_modes': True,
        'source_onboarding_incomplete_if_either_mode_missing_from': [
            'query_export_paths',
            'historical_http_path',
            'historical_python_interface',
        ],
    },
    'source-migration.json': {
        'scope': 'whole_system_and_all_future_development',
        'every_migrated_source_ports_writes_to_canonical_event_log_and_reads_to_projection_driven_serving': True,
        'every_migrated_source_closes_with_native_and_aligned_1s_in_same_slice': True,
        'parity_and_replay_determinism_required_before_cutover_closeout': True,
        'exactly_once_proof_under_duplicate_replay_and_crash_restart_required': True,
        'no_miss_completeness_proof_with_source_appropriate_gap_detection_required': True,
        'raw_fidelity_and_numeric_precision_proof_against_fixed_fixtures_required': True,
        'aligned_outputs_must_prove_canonical_aligned_1s_aggregates_contract': True,
        'explicit_derived_only_aligned_backlog_debt_must_be_tracked_until_closed': True,
    },
}


def test_system_domain_contract_payloads_are_exact() -> None:
    for name, expected in _EXPECTED.items():
        payload = json.loads((_GOVERNANCE_DIR / name).read_text(encoding='utf-8'))
        assert payload == expected


def test_system_domain_contracts_are_routed_from_agents_and_spec() -> None:
    agents_text = _AGENTS.read_text(encoding='utf-8')
    top_level_plan_text = _TOP_LEVEL_PLAN.read_text(encoding='utf-8')

    for name in _EXPECTED:
        assert f'contracts/governance/{name}' in agents_text
        assert f'contracts/governance/{name}' in top_level_plan_text
