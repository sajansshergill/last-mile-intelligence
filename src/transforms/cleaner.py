"""
cleaner.py
─────────────────────────────────────────────────────────────────────────────
Stage 1 transform: reads raw_delivery_volume from DuckDB, applies null
handling, type normalization, range filtering, and duplicate resolution.
Outputs a clean DataFrame ready for demand forecasting.
"""

import sys
from pathlib import Path
from datetime import datetime

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


VOLUME_MIN = 0
VOLUME_MAX = 50_000
VALID_SHIFTS = {"early_morning", "afternoon", "night"}
VALID_SITE_TYPES = {"last_mile", "sort_center"}


def clean(
    db_path: str = "data/lmd_warehouse.duckdb",
) -> pd.DataFrame:
    """
    Reads raw_delivery_volume and returns a cleaned DataFrame.

    Cleaning steps
    --------------
    1. Load raw data from DuckDB
    2. Drop rows with null PKs (site_id, event_date, shift)
    3. Normalize string fields to lowercase / strip whitespace
    4. Filter volume outside [VOLUME_MIN, VOLUME_MAX]
    5. Filter invalid shift / site_type values
    6. Resolve duplicates — keep last ingested record
    7. Add derived date features for downstream transforms

    Returns
    -------
    pd.DataFrame : cleaned volume data
    """
    db = Path(PROJECT_ROOT / db_path)
    print(f"\n[cleaner] Reading raw_delivery_volume from {db.name}")

    with duckdb.connect(str(db)) as conn:
        df = conn.execute("""
            SELECT
                site_id,
                site_name,
                region,
                site_type,
                event_date,
                shift,
                package_volume,
                ingested_at
            FROM raw_delivery_volume
            ORDER BY site_id, event_date, shift
        """).df()

    raw_count = len(df)
    print(f"[cleaner] Raw rows loaded: {raw_count:,}")

    # ------------------------------------------------------------------
    # 1. Drop null PKs
    # ------------------------------------------------------------------
    pk_cols = ["site_id", "event_date", "shift"]
    before = len(df)
    df = df.dropna(subset=pk_cols)
    _log_drop("null PKs", before, len(df))

    # ------------------------------------------------------------------
    # 2. Normalize strings
    # ------------------------------------------------------------------
    df["site_id"] = df["site_id"].str.strip().str.upper()
    df["shift"] = df["shift"].str.strip().str.lower()
    df["site_type"] = df["site_type"].str.strip().str.lower()
    df["region"] = df["region"].str.strip().str.lower()

    # ------------------------------------------------------------------
    # 3. Volume range filter
    # ------------------------------------------------------------------
    before = len(df)
    df = df[df["package_volume"].between(VOLUME_MIN, VOLUME_MAX)]
    _log_drop(f"volume outside [{VOLUME_MIN}, {VOLUME_MAX}]", before, len(df))

    # ------------------------------------------------------------------
    # 4. Invalid categorical values
    # ------------------------------------------------------------------
    before = len(df)
    df = df[df["shift"].isin(VALID_SHIFTS)]
    _log_drop("invalid shifts", before, len(df))

    before = len(df)
    df = df[df["site_type"].isin(VALID_SITE_TYPES)]
    _log_drop("invalid site_types", before, len(df))

    # ------------------------------------------------------------------
    # 5. Deduplicate — keep last ingested
    # ------------------------------------------------------------------
    before = len(df)
    df = df.sort_values("ingested_at").drop_duplicates(
        subset=["site_id", "event_date", "shift"], keep="last"
    )
    _log_drop("duplicates (kept last)", before, len(df))

    # ------------------------------------------------------------------
    # 6. Derived date features
    # ------------------------------------------------------------------
    df["event_date"] = pd.to_datetime(df["event_date"])
    df["day_of_week"] = df["event_date"].dt.strftime("%A").str.lower()
    df["week_number"] = df["event_date"].dt.isocalendar().week.astype(int)
    df["month"] = df["event_date"].dt.month
    df["year"] = df["event_date"].dt.year
    df["is_weekend"] = df["day_of_week"].isin(["saturday", "sunday"])
    df["is_peak_season"] = df["event_date"].apply(_is_peak_season)

    # ------------------------------------------------------------------
    # 7. Reset index
    # ------------------------------------------------------------------
    df = df.reset_index(drop=True)

    clean_count = len(df)
    dropped_total = raw_count - clean_count
    print(
        f"[cleaner] ✓ Cleaning complete — "
        f"{clean_count:,} rows retained, {dropped_total:,} dropped "
        f"({dropped_total / raw_count:.1%} rejection rate)"
    )
    return df


def _is_peak_season(d: pd.Timestamp) -> bool:
    """Nov 15 – Jan 5 is Amazon's peak season."""
    return (d.month == 11 and d.day >= 15) or (d.month == 12) or (d.month == 1 and d.day <= 5)


def _log_drop(reason: str, before: int, after: int) -> None:
    dropped = before - after
    if dropped > 0:
        print(f"[cleaner]   Dropped {dropped:,} rows ({reason})")


if __name__ == "__main__":
    df = clean()
    print(f"\nSchema: {list(df.columns)}")
    print(df.head(5).to_string())