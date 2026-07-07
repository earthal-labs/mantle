"""Typed parameter specifications for Mantle analytics plugins."""

from __future__ import annotations

from dataclasses import dataclass, field

from enum import Enum

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from mantle_analytics.plugins.base import OutputSpec


class ParamDirection(str, Enum):
    INPUT = "input"
    OUTPUT = "output"


class ParamType(str, Enum):
    BAND = "band"
    BAND_NAME = "band_name"
    NUMBER = "number"
    STRING = "string"
    BOOLEAN = "boolean"
    SERVICE = "service"
    STRING_LIST = "string_list"
    NUMBER_LIST = "number_list"
    OUTPUT_JSON = "output_json"
    OUTPUT_GEOJSON = "output_geojson"
    OUTPUT_COG = "output_cog"
    OUTPUT_ZARR = "output_zarr"
    OUTPUT_TEXT = "output_text"


OUTPUT_KIND_BY_PARAM_TYPE: dict[ParamType, str] = {
    ParamType.OUTPUT_JSON: "json",
    ParamType.OUTPUT_GEOJSON: "geojson",
    ParamType.OUTPUT_COG: "cog",
    ParamType.OUTPUT_ZARR: "zarr",
    ParamType.OUTPUT_TEXT: "text",
}


DEFAULT_FILENAME_BY_KIND: dict[str, str] = {
    "json": "{job_id}.json",
    "geojson": "{job_id}.geojson",
    "cog": "{job_id}.tif",
    "zarr": "{job_id}.zarr",
    "text": "{job_id}.txt",
}


def is_output_param_type(param_type: ParamType) -> bool:
    return param_type.value.startswith("output_")


@dataclass
class ParameterSpec:
    name: str
    param_type: ParamType
    description: str
    direction: ParamDirection = ParamDirection.INPUT
    required: bool = True
    default: Any = None
    minimum: float | None = None
    maximum: float | None = None
    role: str | None = None
    filename_template: str | None = None
    subpath: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if is_output_param_type(self.param_type):
            self.direction = ParamDirection.OUTPUT

    def to_json(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "param_type": self.param_type.value,
            "description": self.description,
            "direction": self.direction.value,
            "required": self.required,
        }
        if self.default is not None:
            payload["default"] = self.default
        if self.minimum is not None:
            payload["minimum"] = self.minimum
        if self.maximum is not None:
            payload["maximum"] = self.maximum
        if self.role is not None:
            payload["role"] = self.role
        if self.filename_template is not None:
            payload["filename_template"] = self.filename_template
        if self.subpath is not None:
            payload["subpath"] = self.subpath
        payload.update(self.extra)
        return payload


def input_parameters(specs: list[ParameterSpec]) -> list[ParameterSpec]:
    return [spec for spec in specs if spec.direction == ParamDirection.INPUT]


def output_parameters(specs: list[ParameterSpec]) -> list[ParameterSpec]:
    return [spec for spec in specs if spec.direction == ParamDirection.OUTPUT]


def primary_output(specs: list[ParameterSpec]) -> ParameterSpec | None:
    outputs = output_parameters(specs)
    return outputs[0] if outputs else None


def output_spec_from_parameters(
    specs: list[ParameterSpec],
    *,
    output_name: str | None = None,
) -> OutputSpec:
    from mantle_analytics.plugins.base import OutputSpec

    if output_name is not None:
        match = next(
            (spec for spec in output_parameters(specs) if spec.name == output_name),
            None,
        )
        if match is None:
            raise ValueError(f"unknown output parameter: {output_name}")
        output_spec = match
    else:
        output_spec = primary_output(specs)
        if output_spec is None:
            raise ValueError("plugin declares no output parameters")
    kind = OUTPUT_KIND_BY_PARAM_TYPE.get(output_spec.param_type)
    if kind is None:
        raise ValueError(f"parameter {output_spec.name} is not an output type")
    filename_template = output_spec.filename_template or DEFAULT_FILENAME_BY_KIND[kind]
    subpath = output_spec.subpath or "jobs"
    return OutputSpec(kind=kind, filename_template=filename_template, subpath=subpath)


def parameters_to_json(specs: list[ParameterSpec]) -> list[dict[str, Any]]:
    return [spec.to_json() for spec in specs]


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_params_against_specs(
    specs: list[ParameterSpec],
    params: dict[str, Any],
    *,
    allow_unknown: bool = False,
) -> None:
    input_specs = input_parameters(specs)
    if not allow_unknown:
        known = {spec.name for spec in input_specs}

        for key in params:
            if key not in known:
                raise ValueError(f"unknown parameter: {key}")
    for spec in input_specs:
        if spec.name not in params:
            if spec.required and spec.default is None:
                raise ValueError(f"missing required parameter: {spec.name}")
            continue
        value = params[spec.name]

        if value is None:
            if spec.required:
                raise ValueError(f"parameter {spec.name} must not be null")
            continue
        match spec.param_type:
            case ParamType.BAND:
                if not isinstance(value, int) or value < 1:
                    raise ValueError(
                        f"{spec.name} must be a positive integer band index"
                    )
            case ParamType.BAND_NAME:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{spec.name} must be a non-empty band name")
            case ParamType.NUMBER:
                if not _is_number(value):
                    raise ValueError(f"{spec.name} must be a number")
                numeric = float(value)
                if spec.minimum is not None and numeric < spec.minimum:
                    raise ValueError(f"{spec.name} must be >= {spec.minimum}")
                if spec.maximum is not None and numeric > spec.maximum:
                    raise ValueError(f"{spec.name} must be <= {spec.maximum}")
            case ParamType.STRING:
                if not isinstance(value, str):
                    raise ValueError(f"{spec.name} must be a string")
            case ParamType.BOOLEAN:
                if not isinstance(value, bool):
                    raise ValueError(f"{spec.name} must be a boolean")
            case ParamType.SERVICE:
                if not isinstance(value, str) or not value.strip():
                    raise ValueError(f"{spec.name} must be a service UUID string")
            case ParamType.STRING_LIST:
                if not isinstance(value, list) or not all(
                    isinstance(item, str) for item in value
                ):
                    raise ValueError(f"{spec.name} must be a list of strings")
            case ParamType.NUMBER_LIST:
                if not isinstance(value, list) or not value:
                    raise ValueError(f"{spec.name} must be a non-empty list of numbers")
                if not all(_is_number(item) for item in value):
                    raise ValueError(f"{spec.name} must contain only numbers")


def merge_params_with_defaults(
    specs: list[ParameterSpec],
    params: dict[str, Any],
) -> dict[str, Any]:

    input_specs = input_parameters(specs)
    merged = {
        spec.name: spec.default for spec in input_specs if spec.default is not None
    }

    merged.update(params)
    validate_params_against_specs(specs, merged, allow_unknown=True)
    return merged


def descriptor_from_plugin(plugin: Any, *, model_kind: str) -> dict[str, Any]:

    metadata = plugin.metadata()
    specs = plugin.parameters()
    return {
        "id": plugin.id,
        "version": metadata.get("version", "1.0.0"),
        "model_kind": model_kind,
        "inputs": parameters_to_json(input_parameters(specs)),
        "outputs": parameters_to_json(output_parameters(specs)),
        "metadata": metadata,
    }
