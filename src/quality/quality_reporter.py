"""
quality_reporter.py
─────────────────────────────────────────────────────────────────────────────
Generates a structured JSON quality report for each pipeline run.
Reports are written to data/quality_reports/ and can be consumed by
monitoring dashboards or alerting systems (AWS CloudWatch equivalent).
"""

import json
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.quality.data_contract import DataContract


REPORTS_DIR = PROJECT_ROOT / "data" / "quality_reports"


def generate_pipeline_quality_report(
    raw_df: pd.DataFrame,
    forecast_df: pd.DataFrame,
    db_path: str = "data/lmd_warehouse.duckdb",
    pipeline_run_id: str = "manual",
) -> dict:
    """
    Runs data contract validation on raw and fact datasets, computes
    additional DuckDB-level quality metrics, and writes a JSON report.

    Parameters
    ----------
    raw_df          : Raw ingested DataFrame
    forecast_df     : Forecasted + staffing DataFrame
    db_path         : Path to DuckDB
    pipeline_run_id : Airflow run ID or 'manual'

    Returns
    -------
    dict : Full quality report (also written to disk)
    """
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    run_ts = datetime.utcnow().isoformat() + "Z"

    print(f"\n[quality_reporter] Generating quality report for run: {pipeline_run_id}")

    # ------------------------------------------------------------------
    # 1. Contract validation — raw layer
    # ------------------------------------------------------------------
    volume_contract = DataContract("config/data_contract_volume.yaml")
    raw_result = volume_contract.validate(raw_df)

    # ------------------------------------------------------------------
    # 2. Fact layer metrics from DuckDB
    # ------------------------------------------------------------------
    db = Path(PROJECT_ROOT / db_path)
    fact_metrics = {}
    if db.exists():
        with duckdb.connect(str(db)) as conn:
            fact_metrics = _compute_fact_metrics(conn)

    # ------------------------------------------------------------------
    # 3. Forecast quality metrics
    # ------------------------------------------------------------------
    forecast_metrics = _compute_forecast_metrics(forecast_df)

    # ------------------------------------------------------------------
    # 4. Assemble report
    # ------------------------------------------------------------------
    report = {
        "pipeline_run_id": pipeline_run_id,
        "generated_at": run_ts,
        "overall_status": raw_result.overall_status,
        "datasets": {
            "raw_delivery_volume": {
                **raw_result.summary(),
                "contract_version": raw_result.contract_version,
                "certification_status": volume_contract.certification_status,
            },
            "fact_staffing_demand": {
                **fact_metrics,
            },
        },
        "forecast_quality": forecast_metrics,
        "sla": {
            "status": (
                "MET"
                if fact_metrics.get("sla_attainment_rate", 0) >= 0.85
                else "BREACHED"
            ),
            "attainment_rate": fact_metrics.get("sla_attainment_rate", 0),
            "threshold": 0.85,
        },
    }

    # ------------------------------------------------------------------
    # 5. Write to disk
    # ------------------------------------------------------------------
    report_path = REPORTS_DIR / (
        f"quality_report_{pipeline_run_id.replace(':', '-')}.json"
    )
    with open(report_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Also write a "latest" copy for easy access
    latest_path = REPORTS_DIR / "quality_report_latest.json"
    with open(latest_path, "w") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"[quality_reporter] ✓ Report written to {report_path.name}")
    print(f"[quality_reporter] Overall status {report['overall_status']}")
    print(f"[quality_reporter] SLA status : {report['sla']['status']}")

    return report


def _compute_fact_metrics(conn: duckdb.DuckDBPyConnection) -> dict:
    """Runs analytical queries against fact_staffing_demand for quality metrics."""
    try:
        row = conn.execute("""
            SELECT
                COUNT(*) AS total_rows,
                COUNT(DISTINCT site_key) AS sites_covered,
                COUNT(DISTINCT date_key) AS dates_covered,
                SUM(package_volume) AS total_volume,
                ROUND(AVG(package_volume), 1) AS avg_package_volume,
                ROUND(AVG(forecast_volume), 1) AS avg_forecast_volume,
                ROUND(AVG(recommended_fte), 1) AS avg_recommended_fte,
                ROUND(AVG(actual_fte), 1) AS avg_actual_fte,
                ROUND(AVG(variance_pct), 4) AS avg_variance_pct,
                CASE
                    WHEN COUNT(sla_met) = 0 THEN 0.0
                    ELSE ROUND(AVG(CASE WHEN sla_met THEN 1.0 ELSE 0.0 END), 4)
                END AS sla_attainment_rate,
                SUM(CASE WHEN sla_met = false THEN 1 ELSE 0 END)
                    AS sla_breaches,
                SUM(CASE WHEN variance_pct < -0.15 THEN 1 ELSE 0 END)
                    AS understaffed_shifts,
                SUM(CASE WHEN variance_pct > 0.20 THEN 1 ELSE 0 END)
                    AS overstaffed_shifts
            FROM fact_staffing_demand
        """).fetchone()
    except duckdb.Error as exc:
        return {
            "status": "UNAVAILABLE",
            "error": str(exc),
        }

    columns = [
        "total_rows",
        "sites_covered",
        "dates_covered",
        "total_volume",
        "avg_package_volume",
        "avg_forecast_volume",
        "avg_recommended_fte",
        "avg_actual_fte",
        "avg_variance_pct",
        "sla_attainment_rate",
        "sla_breaches",
        "understaffed_shifts",
        "overstaffed_shifts",
    ]
    metrics = dict(zip(columns, row))
    metrics["status"] = "AVAILABLE"
    return {key: _normalize_metric(value) for key, value in metrics.items()}


def compute_fact_metrics(conn: duckdb.DuckDBPyConnection) -> dict:
    """Backward-compatible public wrapper for fact quality metrics."""
    return _compute_fact_metrics(conn)


def _compute_forecast_metrics(forecast_df: pd.DataFrame) -> dict:
    """Computes in-memory forecast and staffing quality metrics."""
    metrics = {
        "row_count": int(len(forecast_df)),
        "columns_present": sorted(forecast_df.columns.tolist()),
    }

    if forecast_df.empty:
        metrics["status"] = "EMPTY"
        return metrics

    numeric_columns = [
        "package_volume",
        "forecast_volume",
        "recommended_fte",
        "actual_fte",
        "variance_pct",
    ]
    for column in numeric_columns:
        if column in forecast_df.columns:
            metrics[f"avg_{column}"] = _normalize_metric(forecast_df[column].mean())
            metrics[f"min_{column}"] = _normalize_metric(forecast_df[column].min())
            metrics[f"max_{column}"] = _normalize_metric(forecast_df[column].max())

    if "sla_met" in forecast_df.columns:
        sla = forecast_df["sla_met"].dropna()
        metrics["sla_attainment_rate"] = _normalize_metric(
            sla.mean() if not sla.empty else 0
        )
        metrics["sla_breaches"] = int(forecast_df["sla_met"].eq(False).sum())

    if "variance_pct" in forecast_df.columns:
        metrics["understaffed_shifts"] = int(
            (forecast_df["variance_pct"] < -0.15).sum()
        )
        metrics["overstaffed_shifts"] = int(
            (forecast_df["variance_pct"] > 0.20).sum()
        )

    metrics["status"] = "AVAILABLE"
    return metrics


def _normalize_metric(value):
    """Converts pandas, NumPy, and DuckDB scalar values into JSON-friendly types."""
    if pd.isna(value):
        return 0
    if hasattr(value, "item"):
        value = value.item()
    if isinstance(value, float):
        return round(value, 4)
    return value
