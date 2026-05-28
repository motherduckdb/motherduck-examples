#!/usr/bin/env python3
# /// script
# dependencies = ["pyyaml"]
# ///

from pathlib import Path
from typing import Any

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]

ALLOWED_KINDS = {"recipe", "flight_template", "dive_template"}
ALLOWED_FEATURES = {"dives", "ducklake", "flights", "pg_endpoint", "wasm"}
ALLOWED_CATEGORIES = {
    "analytics",
    "application",
    "ingestion",
    "lakehouse",
    "machine-learning",
    "orchestration",
    "transformation",
}
ALLOWED_PARAMETER_TYPES = {"boolean", "enum", "string"}
RESERVED_TAGS = {"dbt-duckdb", "motherduck"}


def load_meta(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise AssertionError(f"{path}: expected a YAML mapping")
    return data


def assert_string(data: dict[str, Any], path: Path, key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise AssertionError(f"{path}: {key} must be a non-empty string")
    return value


def assert_bool(data: dict[str, Any], path: Path, key: str) -> bool:
    value = data.get(key)
    if not isinstance(value, bool):
        raise AssertionError(f"{path}: {key} must be true or false")
    return value


def assert_string_list(data: dict[str, Any], path: Path, key: str) -> list[str]:
    value = data.get(key)
    if not isinstance(value, list) or not value:
        raise AssertionError(f"{path}: {key} must be a non-empty list")
    if not all(isinstance(item, str) and item.strip() for item in value):
        raise AssertionError(f"{path}: {key} must contain only non-empty strings")
    return value


def assert_slug_list(path: Path, key: str, values: list[str]) -> None:
    for value in values:
        if value != value.lower() or " " in value:
            raise AssertionError(f"{path}: {key} values must be lowercase slugs, got {value!r}")


def assert_no_products(path: Path, data: dict[str, Any]) -> None:
    if "products" in data:
        raise AssertionError(f"{path}: use features instead of products")


def assert_features(path: Path, data: dict[str, Any]) -> list[str]:
    features = assert_string_list(data, path, "features")
    assert_slug_list(path, "features", features)
    unknown = set(features) - ALLOWED_FEATURES
    if unknown:
        raise AssertionError(
            f"{path}: unknown features {sorted(unknown)}; allowed features are {sorted(ALLOWED_FEATURES)}"
        )
    return features


def assert_categories(path: Path, data: dict[str, Any]) -> None:
    categories = assert_string_list(data, path, "categories")
    assert_slug_list(path, "categories", categories)
    unknown = set(categories) - ALLOWED_CATEGORIES
    if unknown:
        raise AssertionError(
            f"{path}: unknown categories {sorted(unknown)}; allowed categories are {sorted(ALLOWED_CATEGORIES)}"
        )


def assert_tags(path: Path, data: dict[str, Any]) -> None:
    tags = data.get("tags", [])
    if tags == []:
        return
    if not isinstance(tags, list) or not all(isinstance(tag, str) and tag.strip() for tag in tags):
        raise AssertionError(f"{path}: tags must be a list of non-empty strings")
    assert_slug_list(path, "tags", tags)
    reserved = set(tags) & RESERVED_TAGS
    if reserved:
        raise AssertionError(f"{path}: reserved tags {sorted(reserved)} should be features or simpler tags")


def assert_existing_relative_path(meta_path: Path, key_path: str, value: Any) -> None:
    if not isinstance(value, str) or not value:
        raise AssertionError(f"{meta_path}: {key_path} must be a non-empty string path")
    target = (meta_path.parent / value).resolve()
    if not target.exists():
        raise AssertionError(f"{meta_path}: {key_path} points to a missing path: {value}")
    if not target.is_relative_to(REPO_ROOT):
        raise AssertionError(f"{meta_path}: {key_path} must stay inside the repository")


def assert_entrypoints(path: Path, data: dict[str, Any], kind: str, features: list[str]) -> None:
    entrypoints = data.get("entrypoints")
    if not isinstance(entrypoints, dict) or not entrypoints:
        raise AssertionError(f"{path}: entrypoints must be a non-empty mapping")

    if kind == "recipe":
        if "docs" not in entrypoints:
            raise AssertionError(f"{path}: recipe entrypoints must include docs")
        if "flights" in features and {"flight", "flight_source"} - set(entrypoints):
            raise AssertionError(f"{path}: recipes with flights must include flight and flight_source entrypoints")
        if "dives" in features and "dive" not in entrypoints:
            raise AssertionError(f"{path}: recipes with dives must include a dive entrypoint")
    elif kind == "flight_template":
        if "flights" not in features:
            raise AssertionError(f"{path}: flight templates must include the flights feature")
        if {"source", "requirements"} - set(entrypoints):
            raise AssertionError(f"{path}: flight templates must include source and requirements entrypoints")
    elif kind == "dive_template":
        if "dives" not in features:
            raise AssertionError(f"{path}: dive templates must include the dives feature")
        if "source" not in entrypoints:
            raise AssertionError(f"{path}: dive templates must include a source entrypoint")

    for key, value in entrypoints.items():
        assert_existing_relative_path(path, f"entrypoints.{key}", value)


def assert_templates(path: Path, data: dict[str, Any]) -> None:
    templates = data.get("templates", {})
    if templates == {}:
        return
    if not isinstance(templates, dict):
        raise AssertionError(f"{path}: templates must be a mapping")
    for key, value in templates.items():
        if key not in {"dive", "flight"}:
            raise AssertionError(f"{path}: unknown template reference {key!r}")
        assert_existing_relative_path(path, f"templates.{key}", value)


def assert_parameters(path: Path, data: dict[str, Any], kind: str) -> None:
    parameters = data.get("parameters", [])
    if kind.endswith("_template") and not parameters:
        raise AssertionError(f"{path}: templates must define parameters")
    if parameters == []:
        return
    if not isinstance(parameters, list):
        raise AssertionError(f"{path}: parameters must be a list")

    seen_names: set[str] = set()
    for index, parameter in enumerate(parameters):
        prefix = f"parameters[{index}]"
        if not isinstance(parameter, dict):
            raise AssertionError(f"{path}: {prefix} must be a mapping")
        name = parameter.get("name")
        if not isinstance(name, str) or not name.strip():
            raise AssertionError(f"{path}: {prefix}.name must be a non-empty string")
        if name in seen_names:
            raise AssertionError(f"{path}: duplicate parameter name {name!r}")
        seen_names.add(name)
        label = parameter.get("label")
        if not isinstance(label, str) or not label.strip():
            raise AssertionError(f"{path}: {prefix}.label must be a non-empty string")
        required = parameter.get("required")
        if not isinstance(required, bool):
            raise AssertionError(f"{path}: {prefix}.required must be true or false")
        parameter_type = parameter.get("type")
        if not isinstance(parameter_type, str) or not parameter_type.strip():
            raise AssertionError(f"{path}: {prefix}.type must be a non-empty string")
        if parameter_type not in ALLOWED_PARAMETER_TYPES:
            raise AssertionError(f"{path}: {prefix}.type must be one of {sorted(ALLOWED_PARAMETER_TYPES)}")
        if parameter_type == "enum":
            options = parameter.get("options")
            if not isinstance(options, list) or not options:
                raise AssertionError(f"{path}: {prefix}.options must be a non-empty list")
            if not all(isinstance(option, str) and option.strip() for option in options):
                raise AssertionError(f"{path}: {prefix}.options must contain only non-empty strings")
            default = parameter.get("default")
            if default is not None and default not in options:
                raise AssertionError(f"{path}: {prefix}.default must be one of {options}")


def assert_kind_matches_path(path: Path, kind: str) -> None:
    relative = path.relative_to(REPO_ROOT)
    if kind == "recipe" and (len(relative.parts) != 2 or relative.parts[0] == "templates"):
        raise AssertionError(f"{path}: recipe metadata must live in a top-level recipe directory")
    if kind == "flight_template" and relative.parts[:2] != ("templates", "flights"):
        raise AssertionError(f"{path}: flight_template metadata must live under templates/flights")
    if kind == "dive_template" and relative.parts[:2] != ("templates", "dives"):
        raise AssertionError(f"{path}: dive_template metadata must live under templates/dives")


def validate_meta(path: Path) -> None:
    data = load_meta(path)
    if data.get("metadata_version") != 1:
        raise AssertionError(f"{path}: metadata_version must be 1")

    assert_no_products(path, data)
    metadata_id = assert_string(data, path, "id")
    if metadata_id != path.parent.name:
        raise AssertionError(f"{path}: id must match the containing directory name")

    kind = assert_string(data, path, "kind")
    if kind not in ALLOWED_KINDS:
        raise AssertionError(f"{path}: kind must be one of {sorted(ALLOWED_KINDS)}")
    assert_kind_matches_path(path, kind)

    assert_string(data, path, "title")
    assert_string(data, path, "description")
    features = assert_features(path, data)
    assert_categories(path, data)
    assert_tags(path, data)
    assert_entrypoints(path, data, kind, features)
    assert_templates(path, data)
    assert_parameters(path, data, kind)

    standalone = data.get("standalone")
    if kind == "recipe":
        if standalone is not True:
            raise AssertionError(f"{path}: recipes must set standalone: true")
    elif standalone not in (False, None):
        raise AssertionError(f"{path}: templates must omit standalone or set standalone: false")


def iter_meta_files() -> list[Path]:
    return sorted(
        path
        for path in REPO_ROOT.rglob("meta.yml")
        if not any(part.startswith(".") for part in path.relative_to(REPO_ROOT).parts)
    )


def main() -> None:
    paths = iter_meta_files()
    if not paths:
        raise AssertionError("No meta.yml files found")
    for path in paths:
        validate_meta(path)
    print(f"Checked {len(paths)} meta.yml file(s)")


if __name__ == "__main__":
    main()
