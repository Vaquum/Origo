from __future__ import annotations

from dagster import DefaultSensorStatus

from origo.sources.registry import SOURCE_REGISTRY


def test_legacy_sensors_run_and_revisioned_source_sensors_stop(
    origo_definitions_module: object,
) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    not_running = {
        sensor.name
        for sensor in defs.sensors
        if sensor.default_status is not DefaultSensorStatus.RUNNING
    }
    expected_stopped = {
        f'{source.key}_{consumer.key}_sensor'
        for source in SOURCE_REGISTRY
        for consumer in source.consumers
    } | {
        f'{source.key}_{role}_sensor'
        for source in SOURCE_REGISTRY
        for role in ('failure', 'reconciliation')
    }
    assert not_running == expected_stopped
