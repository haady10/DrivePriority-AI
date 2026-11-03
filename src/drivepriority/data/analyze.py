"""Disk-backed anomaly analysis for an immutable cleaned quarter."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Sequence

import duckdb

from drivepriority.data.manifest import (
    output_inventory,
    read_json,
    sha256_file,
    utc_now,
    verify_inventory,
    write_json_atomic,
)


ANALYSIS_VERSION = "1.0.0"
RUNTIME_EXCLUSIONS = {
    "source_manifest.json",
    "publication_ready.json",
    "run_state.json",
    "state.duckdb",
    "state.duckdb.wal",
    "processing_failure.json",
}


class AnalysisError(RuntimeError):
    """Raised when immutable input evidence cannot be trusted."""


def _sql_path(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def _verified_input(root: Path) -> dict[str, Any]:
    if not root.is_dir() or root.is_symlink():
        raise AnalysisError("Cleaned quarter must be an existing non-symlink directory")
    manifest = read_json(root / "source_manifest.json")
    inventory = manifest.get("outputs")
    if not isinstance(inventory, list):
        raise AnalysisError("Source manifest output inventory is unavailable")
    errors = verify_inventory(
        root,
        inventory,
        excluded_names=RUNTIME_EXCLUSIONS,
        required_paths={
            "schema_report.json",
            "quality_report.json",
            "validation_results.json",
        },
    )
    if errors:
        raise AnalysisError("Cleaned-output integrity failed: " + "; ".join(errors))
    return manifest


def analyze_quarter(
    quarter_output: Path,
    analysis_output: Path,
    *,
    memory_limit: str = "1GB",
) -> dict[str, Any]:
    quarter_output = quarter_output.resolve()
    analysis_output = analysis_output.resolve()
    if analysis_output.exists() or analysis_output.with_name(analysis_output.name + ".staging").exists():
        raise AnalysisError("Analysis output already exists; reports are immutable")
    manifest = _verified_input(quarter_output)
    quarter = str(manifest.get("assigned_quarter"))
    data_files = sorted((quarter_output / "data").rglob("*.parquet"))
    if not data_files:
        raise AnalysisError("No canonical Parquet files were found")

    staging = analysis_output.with_name(analysis_output.name + ".staging")
    staging.mkdir(parents=True)
    database = staging / "analysis.duckdb"
    connection = duckdb.connect(str(database))
    connection.execute(f"SET memory_limit = '{memory_limit}'")
    connection.execute("SET threads = 2")
    try:
        data_glob = _sql_path(quarter_output / "data" / "**" / "*.parquet")
        connection.execute(
            f"CREATE VIEW drive_rows AS SELECT * FROM read_parquet('{data_glob}')"
        )
        connection.execute(
            """
            CREATE TEMP VIEW identity_observations AS
            SELECT date, serial_number, model, manufacturer, capacity_bytes,
                   source_file,
                   lower(regexp_replace(trim(model), '\\s+', ' ', 'g')) AS model_key,
                   CASE WHEN capacity_bytes > 0 THEN capacity_bytes ELSE NULL END
                       AS known_capacity
            FROM drive_rows
            """
        )
        collision_path = staging / "collision_groups.parquet"
        connection.execute(
            f"""
            COPY (
                WITH daily AS (
                    SELECT serial_number, date,
                           count(DISTINCT model_key) AS daily_model_keys,
                           count(DISTINCT known_capacity) AS daily_known_capacities
                    FROM identity_observations
                    GROUP BY serial_number, date
                ), daily_summary AS (
                    SELECT serial_number,
                           count(*) FILTER (
                               WHERE daily_model_keys > 1 OR daily_known_capacities > 1
                           ) AS same_day_contradictions
                    FROM daily
                    GROUP BY serial_number
                ), grouped AS (
                    SELECT serial_number,
                           count(*) AS observations,
                           min(date) AS first_observation_date,
                           max(date) AS last_observation_date,
                           count(DISTINCT model) AS original_model_variants,
                           count(DISTINCT model_key) AS comparison_model_variants,
                           list(DISTINCT model ORDER BY model) AS original_models,
                           count(DISTINCT known_capacity) AS known_capacity_variants,
                           list(DISTINCT known_capacity ORDER BY known_capacity)
                               FILTER (WHERE known_capacity IS NOT NULL) AS known_capacities,
                           count(*) FILTER (WHERE known_capacity IS NULL) AS unknown_capacity_rows,
                           count(*) FILTER (WHERE known_capacity IS NOT NULL) AS known_capacity_rows
                    FROM identity_observations
                    GROUP BY serial_number
                )
                SELECT grouped.*,
                       coalesce(daily_summary.same_day_contradictions, 0)
                           AS same_day_contradictions,
                       original_model_variants > comparison_model_variants
                           AS model_formatting_variants,
                       comparison_model_variants > 1 AS genuine_model_conflict,
                       known_capacity_variants > 1 AS contradictory_known_capacities,
                       unknown_capacity_rows > 0 AND known_capacity_rows > 0
                           AS known_unknown_capacity_transition,
                       (
                           comparison_model_variants > 1
                           OR known_capacity_variants > 1
                           OR coalesce(daily_summary.same_day_contradictions, 0) > 0
                       ) AS unresolved_identity_ambiguity
                FROM grouped
                LEFT JOIN daily_summary USING (serial_number)
                WHERE original_model_variants > 1
                   OR known_capacity_variants > 1
                   OR (unknown_capacity_rows > 0 AND known_capacity_rows > 0)
                   OR coalesce(daily_summary.same_day_contradictions, 0) > 0
                ORDER BY serial_number
            ) TO '{_sql_path(collision_path)}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        collision_counts = connection.execute(
            f"""
            SELECT count(*) AS affected_serials,
                   coalesce(sum(observations), 0) AS grouped_observations,
                   count(*) FILTER (WHERE model_formatting_variants)
                       AS model_formatting_serials,
                   count(*) FILTER (WHERE known_unknown_capacity_transition)
                       AS known_unknown_capacity_serials,
                   count(*) FILTER (WHERE genuine_model_conflict)
                       AS genuine_model_conflict_serials,
                   count(*) FILTER (WHERE contradictory_known_capacities)
                       AS contradictory_capacity_serials,
                   count(*) FILTER (WHERE same_day_contradictions > 0)
                       AS same_day_contradiction_serials,
                   count(*) FILTER (WHERE unresolved_identity_ambiguity)
                       AS unresolved_serials
            FROM read_parquet('{_sql_path(collision_path)}')
            """
        ).fetchone()

        exception_summary: list[dict[str, Any]] = []
        exceptions = quarter_output / "exceptions.parquet"
        if exceptions.is_file():
            rows = connection.execute(
                f"""
                SELECT exception_code, severity, row_disposition,
                       count(*) AS occurrences,
                       count(DISTINCT serial_number) AS unique_serials,
                       count(DISTINCT source_file) AS source_members
                FROM read_parquet('{_sql_path(exceptions)}')
                GROUP BY exception_code, severity, row_disposition
                ORDER BY exception_code, severity, row_disposition
                """
            ).fetchall()
            exception_summary = [
                {
                    "exception_code": row[0],
                    "severity": row[1],
                    "row_disposition": row[2],
                    "occurrences": int(row[3]),
                    "unique_serials": int(row[4]),
                    "source_members": int(row[5]),
                    "evidence": "exceptions.parquet",
                }
                for row in rows
            ]

        quality = read_json(quarter_output / "quality_report.json")
        validation = read_json(quarter_output / "validation_results.json")
        labels = [
            "affected_serials",
            "grouped_observations",
            "model_formatting_serials",
            "known_unknown_capacity_serials",
            "genuine_model_conflict_serials",
            "contradictory_capacity_serials",
            "same_day_contradiction_serials",
            "unresolved_serials",
        ]
        identity = {name: int(value or 0) for name, value in zip(labels, collision_counts)}
        report = {
            "analysis_version": ANALYSIS_VERSION,
            "quarter": quarter,
            "generated_at": utc_now(),
            "source_output": str(quarter_output),
            "source_manifest_sha256": sha256_file(quarter_output / "source_manifest.json"),
            "original_validation_status": validation.get("status"),
            "cleaning": {
                "input_rows": quality.get("counts", {}).get("input_rows"),
                "retained_rows": quality.get("counts", {}).get("output_rows"),
                "quarantined_rows": quality.get("counts", {}).get("quarantined_rows"),
                "row_accounting": quality.get("row_accounting"),
            },
            "identity_analysis": identity,
            "exception_groups": exception_summary,
            "automatic_treatments": {
                "model_formatting_variants": (
                    "comparison-only Unicode whitespace collapse, trim, and case-fold; "
                    "original model strings remain unchanged"
                ),
                "known_unknown_capacity_transition": (
                    "unknown capacity is non-conflicting with a known capacity; "
                    "different known positive capacities remain unresolved"
                ),
            },
            "unresolved_identity_ambiguity": identity["unresolved_serials"] > 0,
            "next_actions": (
                ["Review collision_groups.parquet and provide signed resolution evidence"]
                if identity["unresolved_serials"]
                else ["Run immutable revalidation using this analysis"]
            ),
            "limitations": [
                "Analysis groups one quarter and does not resolve cross-quarter device identity",
                "Automatic treatments are comparison-only and never rewrite canonical rows",
            ],
        }
        connection.execute("CHECKPOINT")
        connection.close()
        database.unlink(missing_ok=True)
        database.with_suffix(database.suffix + ".wal").unlink(missing_ok=True)
        write_json_atomic(staging / "anomaly_report.json", report)
        inventory = output_inventory(staging, excluded_names={"analysis.duckdb"})
        write_json_atomic(
            staging / "analysis_manifest.json",
            {
                "analysis_version": ANALYSIS_VERSION,
                "quarter": quarter,
                "generated_at": report["generated_at"],
                "source_manifest_sha256": report["source_manifest_sha256"],
                "outputs": inventory,
            },
        )
    except Exception:
        try:
            connection.close()
        except Exception:
            pass
        raise
    staging.replace(analysis_output)
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quarter-output", type=Path, required=True)
    parser.add_argument("--analysis-output", type=Path, required=True)
    parser.add_argument("--memory-limit", default="1GB")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = build_parser().parse_args(argv)
    try:
        result = analyze_quarter(
            arguments.quarter_output,
            arguments.analysis_output,
            memory_limit=arguments.memory_limit,
        )
    except (AnalysisError, OSError, ValueError, duckdb.Error) as exc:
        print(f"ERROR: {exc}")
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
