import os
import pathlib
import re
import shutil
import subprocess

import duckdb


WORK_ROOT = pathlib.Path("/tmp/motherduck-flight-dbt-runner")


def main() -> None:
    repo_url = env("REPO_URL", "https://github.com/motherduckdb/motherduck-examples.git")
    repo_ref = env("REPO_REF", "main")
    project_path = env("PROJECT_PATH", "dbt-ingestion-s3")
    command = env("DBT_COMMAND", "build")

    if command not in {"build", "run", "test"}:
        raise ValueError("DBT_COMMAND must be one of: build, run, test")

    run(["apt-get", "update"])
    run(["apt-get", "install", "-y", "git"])

    if WORK_ROOT.exists():
        shutil.rmtree(WORK_ROOT)
    clone_repo(repo_url, repo_ref)

    project_dir = WORK_ROOT / project_path
    if not project_dir.exists():
        raise FileNotFoundError(f"dbt project path does not exist after clone: {project_path}")

    profile = write_profile(project_dir)

    if (project_dir / "packages.yml").exists() or (project_dir / "dependencies.yml").exists():
        run(["dbt", "deps", "--profiles-dir", "."], cwd=project_dir)

    if env_bool("RUN_DBT_SEED", False):
        seed_args = ["dbt", "seed", "--target", profile["target"], "--profiles-dir", "."]
        if env_bool("DBT_SEED_FULL_REFRESH", False):
            seed_args.append("--full-refresh")
        run(seed_args, cwd=project_dir)

    run(dbt_args(command, profile["target"]), cwd=project_dir)
    record_audit(profile, repo_url, repo_ref, project_path, command)
    print("dbt Flight completed")


def env(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip()
    return value or default


def env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "t", "yes", "y", "on"}


def validate_identifier(name: str, value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError(f"{name} must be a simple SQL identifier, got {value!r}")
    return value


def run(command: list[str], cwd: pathlib.Path | None = None) -> None:
    print("$ " + " ".join(command))
    subprocess.run(command, cwd=cwd, check=True)


def write_profile(project_dir: pathlib.Path) -> dict[str, str]:
    profile_name = validate_identifier("DBT_PROFILE_NAME", env("DBT_PROFILE_NAME", "dbt_ingestion_s3"))
    database = env("MOTHERDUCK_DATABASE", "hacker_news_stats")
    schema = validate_identifier("DBT_SCHEMA", env("DBT_SCHEMA", "main"))
    target = validate_identifier("DBT_TARGET", env("DBT_TARGET", "flight"))
    threads = env("DBT_THREADS", "1")
    profile_lines = [
        f"{profile_name}:",
        "  outputs:",
        f"    {target}:",
        "      type: duckdb",
        f'      path: "md:{database}"',
        f"      schema: {schema}",
        f"      threads: {threads}",
    ]
    if env_bool("DBT_IS_DUCKLAKE", False):
        profile_lines.append("      is_ducklake: true")
    profile_lines.extend(
        [
            f"  target: {target}",
        ]
    )
    profile = "\n".join(profile_lines)

    (project_dir / "profiles.yml").write_text(profile + "\n", encoding="utf-8")
    return {
        "profile_name": profile_name,
        "database": database,
        "schema": schema,
        "target": target,
    }


def dbt_args(command: str, target: str) -> list[str]:
    args = ["dbt", command, "--target", target, "--profiles-dir", "."]
    selector = os.environ.get("DBT_SELECT")
    exclude = os.environ.get("DBT_EXCLUDE")
    if selector:
        args.extend(["--select", selector])
    if exclude:
        args.extend(["--exclude", exclude])
    return args


def clone_repo(repo_url: str, repo_ref: str) -> None:
    WORK_ROOT.mkdir(parents=True)
    run(["git", "init"], cwd=WORK_ROOT)
    run(["git", "remote", "add", "origin", repo_url], cwd=WORK_ROOT)
    run(["git", "fetch", "--depth", "1", "origin", repo_ref], cwd=WORK_ROOT)
    run(["git", "checkout", "--detach", "FETCH_HEAD"], cwd=WORK_ROOT)


def record_audit(profile: dict[str, str], repo_url: str, repo_ref: str, project_path: str, command: str) -> None:
    audit_schema = validate_identifier("AUDIT_SCHEMA", env("AUDIT_SCHEMA", "flight_audit"))
    con = duckdb.connect(f"md:{profile['database']}")
    try:
        con.execute(f"CREATE SCHEMA IF NOT EXISTS {audit_schema}")
        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {audit_schema}.dbt_flight_runs (
                run_at TIMESTAMPTZ,
                repo_url VARCHAR,
                repo_ref VARCHAR,
                project_path VARCHAR,
                profile_name VARCHAR,
                target_name VARCHAR,
                target_schema VARCHAR,
                dbt_command VARCHAR
            )
            """
        )
        con.execute(
            f"""
            INSERT INTO {audit_schema}.dbt_flight_runs
            VALUES (current_timestamp, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                repo_url,
                repo_ref,
                project_path,
                profile["profile_name"],
                profile["target"],
                profile["schema"],
                command,
            ],
        )
    finally:
        con.close()


if __name__ == "__main__":
    main()
