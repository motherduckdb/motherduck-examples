from __future__ import annotations

import importlib.util
import json
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "diff-catalog.py"
spec = importlib.util.spec_from_file_location("diff_catalog", SCRIPT_PATH)
assert spec is not None
diff_catalog_module = importlib.util.module_from_spec(spec)
assert spec.loader is not None
spec.loader.exec_module(diff_catalog_module)


def catalog(items: list[dict]) -> dict:
    return {
        "$schema": "catalog.schema.json",
        "schema_version": 1,
        "generated_at": "2026-06-03T10:00:00Z",
        "source": {"repo": "motherduckdb/motherduck-examples", "ref": "main"},
        "items": items,
    }


def item(item_id: str, **overrides) -> dict:
    value = {
        "id": item_id,
        "type": "example",
        "title": f"{item_id} title",
        "description": f"{item_id} description",
        "features": [],
        "tags": ["python"],
        "path": item_id,
        "urls": {
            "github": f"https://github.com/motherduckdb/motherduck-examples/tree/main/{item_id}",
            "raw_docs": f"https://raw.githubusercontent.com/motherduckdb/motherduck-examples/main/{item_id}/README.md",
        },
    }
    value.update(overrides)
    return value


def test_catalog_diff_ignores_generated_fields_and_urls() -> None:
    base = catalog([item("python-ingestion")])
    head = catalog(
        [
            item(
                "python-ingestion",
                urls={
                    "github": "https://github.com/motherduckdb/motherduck-examples/tree/my-branch/python-ingestion",
                    "raw_docs": "https://raw.githubusercontent.com/motherduckdb/motherduck-examples/my-branch/python-ingestion/README.md",
                },
            )
        ]
    )
    head["generated_at"] = "2026-06-03T11:00:00Z"
    head["source"] = {"repo": "motherduckdb/motherduck-examples", "ref": "my-branch"}

    diff = diff_catalog_module.diff_catalogs(base, head)

    assert diff.has_changes is False
    assert diff.added == []
    assert diff.removed == []
    assert diff.changed == []


def test_catalog_diff_detects_added_removed_and_changed_items() -> None:
    base = catalog(
        [
            item("cloudflare-workers"),
            item("python-ingestion"),
            item("old-example"),
        ]
    )
    head = catalog(
        [
            item("cloudflare-workers", tags=["typescript"]),
            item("python-ingestion"),
            item("new-example"),
        ]
    )

    diff = diff_catalog_module.diff_catalogs(base, head)

    assert diff.has_changes is True
    assert [entry["id"] for entry in diff.added] == ["new-example"]
    assert [entry["id"] for entry in diff.removed] == ["old-example"]
    assert [(entry["id"], entry["fields"]) for entry in diff.changed] == [
        ("cloudflare-workers", ["tags"])
    ]


def test_catalog_diff_cli_writes_markdown_and_github_outputs(tmp_path: Path) -> None:
    base_path = tmp_path / "base.json"
    head_path = tmp_path / "head.json"
    markdown_path = tmp_path / "catalog-diff.md"
    github_output_path = tmp_path / "github-output.txt"
    base_path.write_text(json.dumps(catalog([item("python-ingestion")])), encoding="utf-8")
    head_path.write_text(
        json.dumps(catalog([item("python-ingestion", title="Updated title")])),
        encoding="utf-8",
    )

    exit_code = diff_catalog_module.main(
        [
            "--base",
            str(base_path),
            "--head",
            str(head_path),
            "--output",
            str(markdown_path),
            "--github-output",
            str(github_output_path),
        ]
    )

    assert exit_code == 0
    assert "Changed" in markdown_path.read_text(encoding="utf-8")
    assert "python-ingestion" in markdown_path.read_text(encoding="utf-8")
    github_output = github_output_path.read_text(encoding="utf-8")
    assert "has_changes=true" in github_output
    assert "added_count=0" in github_output
    assert "removed_count=0" in github_output
    assert "changed_count=1" in github_output
