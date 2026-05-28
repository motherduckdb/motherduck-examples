#!/usr/bin/env python3

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
RUNNER_TEMPLATE = REPO_ROOT / "templates/flights/dbt-runner"


def read_flight_template_path(meta_path: Path) -> Path | None:
    in_templates = False
    for line in meta_path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped == "templates:":
            in_templates = True
            continue
        if in_templates and line and not line.startswith(" "):
            in_templates = False
        if in_templates and stripped.startswith("flight:"):
            value = stripped.split(":", 1)[1].strip()
            return (meta_path.parent / value).resolve()
    return None


def embedded_flight_source(sql_path: Path) -> str:
    text = sql_path.read_text(encoding="utf-8")
    parts = text.split("$flight$")
    if len(parts) != 3:
        raise AssertionError(f"{sql_path}: expected exactly two $flight$ delimiters")
    return parts[1].strip() + "\n"


def assert_main_is_first_real_function(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    main_index = source.find("def main()")
    helper_index = source.find("def env(")
    if main_index == -1:
        raise AssertionError(f"{path}: missing def main()")
    if helper_index == -1:
        raise AssertionError(f"{path}: missing def env(")
    if main_index > helper_index:
        raise AssertionError(f"{path}: def main() should appear before runner helper functions")


def assert_no_token_query_param(path: Path) -> None:
    source = path.read_text(encoding="utf-8")
    if "motherduck_token" in source:
        raise AssertionError(f"{path}: dbt profile should rely on MOTHERDUCK_TOKEN from the environment")


def main() -> None:
    template_source = (RUNNER_TEMPLATE / "flight.py").read_text(encoding="utf-8")
    checked = 0

    for meta_path in sorted(REPO_ROOT.glob("*/meta.yml")):
        template_path = read_flight_template_path(meta_path)
        if template_path != RUNNER_TEMPLATE:
            continue

        recipe_dir = meta_path.parent
        flight_source_path = recipe_dir / "flights/flight.py"
        create_sql_path = recipe_dir / "flights/create_flight.sql"

        if flight_source_path.read_text(encoding="utf-8") != template_source:
            raise AssertionError(f"{flight_source_path}: does not match {RUNNER_TEMPLATE / 'flight.py'}")
        if embedded_flight_source(create_sql_path) != template_source:
            raise AssertionError(f"{create_sql_path}: embedded source does not match {flight_source_path}")

        assert_main_is_first_real_function(flight_source_path)
        assert_no_token_query_param(flight_source_path)
        checked += 1

    assert_main_is_first_real_function(RUNNER_TEMPLATE / "flight.py")
    assert_no_token_query_param(RUNNER_TEMPLATE / "flight.py")

    if checked == 0:
        raise AssertionError("No recipes reference templates/flights/dbt-runner")
    print(f"Checked {checked} dbt-runner recipe(s)")


if __name__ == "__main__":
    main()
