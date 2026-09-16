from __future__ import annotations

from dagster import DefaultSensorStatus

from origo.sources.bundle import build_source_bundle
from origo.sources.registry import SOURCE_REGISTRY


def test_all_enabled_source_sensors_run(
    origo_definitions_module: object,
) -> None:
    defs = getattr(origo_definitions_module, 'defs')
    not_running = {
        sensor.name
        for sensor in defs.sensors
        if sensor.default_status is not DefaultSensorStatus.RUNNING
    }
    expected_stopped = {
        sensor.name
        for source in SOURCE_REGISTRY
        if source.rollout_stage.value == 'DORMANT'
        for sensor in build_source_bundle(source).sensors
    }
    assert not_running == expected_stopped
