"""
schema_loader.py
─────────────────────────────────────────────────────────────────────────────
Loads transformed data into the DuckDB star schema:
    dim_site  → dim_date  → dim_shift  → fact_staffing_demand

Handles SCD Type 1 (overwrite) for dimensions and upsert for facts.
AWS equivalent: Redshift COPY + MERGE, or dbt models.
"""

import sys
import math
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


SCHEMA_SQL = PROJECT_ROOT / "src" / "models" / "schema.sql"


# ---------------------------------------------------------------------------
# Dimension loaders
# ---------------------------------------------------------------------------

def _load_dim_site(conn: duckdb.DuckDBPyConnection, config_path: str) -> None:
    with open(PROJECT_ROOT / config_path) as f:
        sites = yaml.safe_load(f)["sites"]

    rows = []
    for i, site in enumerate(sites, start=1):
        rows.append({
            "site_key": i,
            "site_id": site["site_id"],
            "site_name": site["site_name"],
            "region": site["region"],
            "site_type": site["site_type"],
            "timezone": site.get("timezone", "UTC"),
            "capacity_max": site.get("capacity_max", 0),
            "peak_days": ",".join(site.get("peak_days", [])),
            "understaffed_threshold": site["volume_thresholds"]["understaffed"],
            "overstaffed_threshold": site["volume_thresholds"]["overstaffed"],
        })

    df = pd.DataFrame(rows)
    conn.register("staging_dim_site", df)
    conn.execute("DELETE FROM dim_site")
    conn.execute("""
        INSERT INTO dim_site
        SELECT site_key, site_id, site_name, region, site_type,
               timezone, capacity_max, peak_days,
               understaffed_threshold, overstaffed_threshold,
               CURRENT_TIMESTAMP
        FROM staging_dim_site
    """)
    print(f"[schema_loader] dim_site loaded: {len(rows)} sites")


def _load_dim_date(
    conn: duckdb.DuckDBPyConnection,
    start: date,
    end: date,
) -> None:
    rows = []
    d = start
    while d <= end:
        rows.append({
            "date_key": int(d.strftime("%Y%m%d")),
            "full_date": d,
            "year": d.year,
            "quarter": (d.month - 1) // 3 + 1,
            "month": d.month,
            "month_name": d.strftime("%B"),
            "week_number": d.isocalendar()[1],
            "day_of_week": d.strftime("%A").lower(),
            "is_weekend": d.weekday() >= 5,
            "is_peak_season": _is_peak(d),
        })
        d += timedelta(days=1)

    df = pd.DataFrame(rows)
    conn.register("staging_dim_date", df)
    conn.execute("DELETE FROM dim_date")
    conn.execute("""
        INSERT INTO dim_date
        SELECT date_key, CAST(full_date AS DATE), year, quarter, month,
               month_name, week_number, day_of_week, is_weekend, is_peak_season
        FROM staging_dim_date
    """)
    print(f"[schema_loader] dim_date loaded: {len(rows)} dates")


def _load_dim_shift(conn: duckdb.DuckDBPyConnection) -> None:
    shifts = [
        {"shift_key": 1, "shift_name": "early_morning", "start_hour": 5,  "end_hour": 13, "shift_type": "day"},
        {"shift_key": 2, "shift_name": "afternoon",     "start_hour": 13, "end_hour": 21, "shift_type": "evening"},
        {"shift_key": 3, "shift_name": "night",         "start_hour": 21, "end_hour": 5,  "shift_type": "overnight"},
    ]
    df = pd.DataFrame(shifts)
    conn.register("staging_dim_shift", df)
    conn.execute("DELETE FROM dim_shift")
    conn.execute("""
        INSERT INTO dim_shift
        SELECT shift_key, shift_name, start_hour, end_hour, shift_type
        FROM staging_dim_shift
    """)
    print(f"[schema_loader] dim_shift loaded: {len(shifts)} shifts")


def _load_fact(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    pipeline_run_id: str,
) -> int:
    """Loads enriched forecast data into fact_staffing_demand."""

    # Build lookup maps
    site_map = conn.execute(
        "SELECT site_id, site_key FROM dim_site"
    ).df().set_index("site_id")["site_key"].to_dict()

    shift_map = conn.execute(
        "SELECT shift_name, shift_key FROM dim_shift"
    ).df().set_index("shift_name")["shift_key"].to_dict()

    fact = df.copy()
    fact["event_date"] = pd.to_datetime(fact["event_date"])

    # Map foreign keys
    fact["site_key"] = fact["site_id"].map(site_map)
    fact["date_key"] = fact["event_date"].dt.strftime("%Y%m%d").astype(int)
    fact["shift_key"] = fact["shift"].map(shift_map)
    fact["pipeline_run_id"] = pipeline_run_id

    # Drop rows where FK mapping failed
    before = len(fact)
    fact = fact.dropna(subset=["site_key", "date_key", "shift_key"])
    if len(fact) < before:
        print(f"[schema_loader] Dropped {before - len(fact)} rows with unmapped FKs")

    fact["site_key"] = fact["site_key"].astype(int)
    fact["shift_key"] = fact["shift_key"].astype(int)

    # Surrogate key: deterministic hash of site+date+shift
    fact["demand_key"] = (
        fact["site_key"].astype(str) + fact["date_key"].astype(str) + fact["shift_key"].astype(str)
    ).apply(lambda x: abs(hash(x)) % (10**12))

    cols = [
        "demand_key", "site_key", "date_key", "shift_key",
        "package_volume", "rolling_7d_avg", "dow_index", "forecast_volume",
        "recommended_fte", "actual_fte", "variance_pct", "sla_met",
        "pipeline_run_id",
    ]
    fact = fact[cols]

    conn.register("staging_fact", fact)
    conn.execute("DELETE FROM fact_staffing_demand")
    conn.execute(f"""
        INSERT INTO fact_staffing_demand
        SELECT
            demand_key, site_key, date_key, shift_key,
            package_volume, rolling_7d_avg, dow_index, forecast_volume,
            recommended_fte, actual_fte, variance_pct, sla_met,
            pipeline_run_id,
            CURRENT_TIMESTAMP,
            CURRENT_TIMESTAMP
        FROM staging_fact
    """)

    count = conn.execute("SELECT COUNT(*) FROM fact_staffing_demand").fetchone()[0]
    print(f"[schema_loader] fact_staffing_demand loaded: {count:,} rows")
    return count


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def load_star_schema(
    forecast_df: pd.DataFrame,
    db_path: str = "data/lmd_warehouse.duckdb",
    config_path: str = "config/sites.yaml",
    pipeline_run_id: str = "manual",
) -> dict:
    """
    Loads all dimensions and the fact table into DuckDB.

    Parameters
    ----------
    forecast_df     : Output of demand_forecast.compute_staffing_variance()
    db_path         : Path to DuckDB file
    config_path     : Path to sites.yaml
    pipeline_run_id : Airflow run ID or 'manual'

    Returns
    -------
    dict : Load summary
    """
    db = Path(PROJECT_ROOT / db_path)
    print(f"\n[schema_loader] Loading star schema into {db.name}")

    # Date range from data
    min_date = forecast_df["event_date"].min()
    max_date = forecast_df["event_date"].max()
    if hasattr(min_date, "date"):
        min_date = min_date.date()
        max_date = max_date.date()

    with duckdb.connect(str(db)) as conn:
        # Init schema
        with open(SCHEMA_SQL) as f:
            conn.execute(f.read())

        # Clear fact first to avoid FK constraint violations on dim reload
        conn.execute("DELETE FROM fact_staffing_demand")

        # Load dimensions
        _load_dim_site(conn, config_path)
        _load_dim_date(conn, min_date, max_date)
        _load_dim_shift(conn)

        # Load fact
        fact_rows = _load_fact(conn, forecast_df, pipeline_run_id)

    print(f"[schema_loader] ✓ Star schema load complete")
    return {
        "status": "success",
        "fact_rows_loaded": fact_rows,
        "pipeline_run_id": pipeline_run_id,
    }


def _is_peak(d: date) -> bool:
    return (d.month == 11 and d.day >= 15) or d.month == 12 or (d.month == 1 and d.day <= 5)


if __name__ == "__main__":
    from src.ingestion.ingestion_framework import IngestionFramework
    from src.transforms.cleaner import clean
    from src.transforms.demand_forecast import compute_demand_forecast, compute_staffing_variance

    IngestionFramework().run()
    df = clean()
    df = compute_demand_forecast(df)
    df = compute_staffing_variance(df)
    result = load_star_schema(df)
    print(result)