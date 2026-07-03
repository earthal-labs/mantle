"""Tests for typed plugin parameter schemas."""

from __future__ import annotations

import pytest

from mantle_analytics.plugins.builtin.vrpm.ndvi import NDVI

from mantle_analytics.plugins.builtin.prpm.zonal_stats import ZonalStatsJob

from mantle_analytics.plugins.parameters import (
    ParamDirection,
    ParamType,
    ParameterSpec,
    input_parameters,
    output_parameters,
    parameters_to_json,
    validate_params_against_specs,
)
from mantle_analytics.registry import (
    get_plugin_descriptor,
    initialize_registry,
    reset_registry,
)


@pytest.fixture(autouse=True)
def _fresh_registry() -> None:
    reset_registry()
    initialize_registry()


def test_ndvi_band_parameters() -> None:
    plugin = NDVI()
    specs = plugin.parameters()
    assert len(specs) == 2
    assert specs[0].param_type == ParamType.BAND
    assert specs[0].role == "red"
    plugin.validate_params({"red_band": 3, "nir_band": 4})
    with pytest.raises(ValueError, match="positive integer"):
        plugin.validate_params({"red_band": 0})


def test_parameters_to_json_includes_output_fields() -> None:
    specs = [
        ParameterSpec(
            name="statistics",
            param_type=ParamType.OUTPUT_JSON,
            description="result file",
            direction=ParamDirection.OUTPUT,
            filename_template="zonal_stats.json",
            subpath="jobs",
        )
    ]
    payload = parameters_to_json(specs)
    assert payload[0]["param_type"] == "output_json"
    assert payload[0]["direction"] == "output"
    assert payload[0]["filename_template"] == "zonal_stats.json"
    assert payload[0]["subpath"] == "jobs"


def test_validate_rejects_unknown_keys() -> None:
    specs = [
        ParameterSpec(
            name="values",
            param_type=ParamType.NUMBER_LIST,
            description="samples",
            required=True,
        )
    ]
    with pytest.raises(ValueError, match="unknown parameter"):
        validate_params_against_specs(specs, {"values": [1.0], "extra": 1})


def test_validate_skips_output_direction_params() -> None:
    specs = ZonalStatsJob().parameters()
    validate_params_against_specs(specs, {"values": [1.0, 2.0]})
    with pytest.raises(ValueError, match="unknown parameter"):
        validate_params_against_specs(specs, {"statistics": "ignored"})


def test_registry_plugin_descriptor_splits_inputs_outputs() -> None:
    descriptor = get_plugin_descriptor("ndvi")
    assert descriptor["id"] == "ndvi"
    assert descriptor["model_kind"] == "vrpm"
    assert {param["name"] for param in descriptor["inputs"]} == {"red_band", "nir_band"}
    assert descriptor["outputs"] == []


def test_zonal_stats_descriptor_has_output_param() -> None:
    descriptor = get_plugin_descriptor("zonal_stats")
    assert len(descriptor["inputs"]) == 3
    assert len(descriptor["outputs"]) == 1
    assert descriptor["outputs"][0]["name"] == "statistics"
    assert descriptor["outputs"][0]["param_type"] == "output_json"
    assert descriptor["model_kind"] == "prpm"


def test_input_output_helpers() -> None:
    specs = ZonalStatsJob().parameters()
    assert len(input_parameters(specs)) == 3
    assert len(output_parameters(specs)) == 1
