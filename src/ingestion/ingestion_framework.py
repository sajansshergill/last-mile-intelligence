"""
ingestion_framework.py
─────────────────────────────────────────────────────────────────────────────
Configuration-driven ingestion layer. Reads sites.yaml to determine which
delivery stations to ingest, then loads synthetic (or real) volume data into
DuckDB's raw layer.

Design principle: zero site-specific code. Adding a new delivery station
requires only a YAML entry — the framework handles schema creation, type
casting, deduplication, and partitioned loading automatically.

AWS equivalent: this layer maps to Glue Crawlers + Firehose → S3 raw prefix.
"""

import os
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd
import yaml

# Allow running from project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.ingestion.synthetic_generator import generate_volume_data


# ---------------------------------------------------------------------------
# Schema DDL for the raw layer
# ---------------------------------------------------------------------------
RAW_SCHEMA_DDL = """
CREATE TABLE IF NOT EXISTS raw_delivery_volume (
    site_id         VARCHAR     NOT NULL,
    site_name       VARCHAR     NOT NULL,
    region          VARCHAR     NOT NULL,
    site_type       VARCHAR     NOT NULL,
    event_date      DATE        NOT NULL,
    shift           VARCHAR     NOT NULL,
    package_volume  INTEGER     NOT NULL,
    ingested_at     TIMESTAMP   NOT NULL,
    PRIMARY KEY (site_id, event_date, shift)
);
"""


# ---------------------------------------------------------------------------
# IngestionFramework
# ---------------------------------------------------------------------------
class IngestionFramework:
    """
    Config-driven ingestion framework for Amazon LMD delivery volume data.

    Usage
    -----
    framework = IngestionFramework(
        config_path="config/sites.yaml",
        db_path="data/lmd_warehouse.duckdb"
    )
    result = framework.run()
    """

    def __init__(
        self,
        config_path: str = "config/sites.yaml",
        pipeline_config_path: str = "config/pipeline_config.yaml",
        db_path: str = "data/lmd_warehouse.duckdb",
        lookback_days: int = 365,
    ):
        self.config_path = Path(PROJECT_ROOT / config_path)
        self.pipeline_config_path = Path(PROJECT_ROOT / pipeline_config_path)
        self.db_path = Path(PROJECT_ROOT / db_path)
        self.lookback_days = lookback_days

        self._site_config = self._load_site_config()
        self._pipeline_config = self._load_pipeline_config()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Config loaders
    # ------------------------------------------------------------------
    def _load_site_config(self) -> list[dict]:
        with open(self.config_path) as f:
            cfg = yaml.safe_load(f)
        sites = cfg.get("sites", [])
        print(f"[ingestion] Loaded {len(sites)} site configs from {self.config_path.name}")
        return sites

    def _load_pipeline_config(self) -> dict:
        with open(self.pipeline_config_path) as f:
            return yaml.safe_load(f)

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------
    def _init_schema(self, conn: duckdb.DuckDBPyConnection) -> None:
        conn.execute(RAW_SCHEMA_DDL)
        print("[ingestion] Raw schema initialized (raw_delivery_volume)")

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------
    def _validate_sites_in_data(self, df: pd.DataFrame) -> None:
        """Ensures every site in the config has data in the generated DataFrame."""
        config_ids = {s["site_id"] for s in self._site_config}
        data_ids = set(df["site_id"].unique())
        missing = config_ids - data_ids
        if missing:
            raise ValueError(f"[ingestion] Sites in config but missing from data: {missing}")

    def _cast_types(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["event_date"] = pd.to_datetime(df["event_date"]).dt.date
        df["package_volume"] = df["package_volume"].astype(int)
        df["ingested_at"] = pd.to_datetime(df["ingested_at"])
        df["site_id"] = df["site_id"].astype(str).str.strip()
        df["shift"] = df["shift"].astype(str).str.strip().str.lower()
        df["site_type"] = df["site_type"].astype(str).str.strip().str.lower()
        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates(subset=["site_id", "event_date", "shift"], keep="last")
        after = len(df)
        if before != after:
            print(f"[ingestion] Deduplication removed {before - after:,} rows")
        return df

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------
    def _upsert(
        self, conn: duckdb.DuckDBPyConnection, df: pd.DataFrame
    ) -> dict:
        """
        Upserts data into raw_delivery_volume using INSERT OR REPLACE.
        Returns a summary dict with row counts.
        """
        before_count = conn.execute(
            "SELECT COUNT(*) FROM raw_delivery_volume"
        ).fetchone()[0]

        # Register DataFrame as a DuckDB view for the upsert
        conn.register("staging_volume", df)
        conn.execute("""
            INSERT OR REPLACE INTO raw_delivery_volume
            SELECT
                site_id,
                site_name,
                region,
                site_type,
                CAST(event_date AS DATE),
                shift,
                package_volume,
                ingested_at
            FROM staging_volume
        """)

        after_count = conn.execute(
            "SELECT COUNT(*) FROM raw_delivery_volume"
        ).fetchone()[0]

        return {
            "rows_in_batch": len(df),
            "rows_before": before_count,
            "rows_after": after_count,
            "rows_inserted_or_replaced": after_count - before_count + len(df),
        }

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------
    def run(self) -> dict:
        """
        Executes the full ingestion pipeline:
        1. Generate synthetic volume data from config
        2. Validate + cast types
        3. Deduplicate
        4. Upsert into DuckDB raw layer

        Returns
        -------
        dict : Ingestion result summary
        """
        print(f"\n{'='*60}")
        print(f"[ingestion] Starting ingestion run — {datetime.utcnow().isoformat()}Z")
        print(f"[ingestion] Lookback: {self.lookback_days} days | Sites: {len(self._site_config)}")
        print(f"{'='*60}")

        # Step 1 — Generate data
        df = generate_volume_data(
            config_path=str(self.config_path),
            lookback_days=self.lookback_days,
        )

        # Step 2 — Validate + cast
        self._validate_sites_in_data(df)
        df = self._cast_types(df)

        # Step 3 — Deduplicate
        df = self._deduplicate(df)

        # Step 4 — Load
        with duckdb.connect(str(self.db_path)) as conn:
            self._init_schema(conn)
            result = self._upsert(conn, df)

            # Spot-check
            sample = conn.execute("""
                SELECT site_id, event_date, shift, package_volume
                FROM raw_delivery_volume
                ORDER BY RANDOM()
                LIMIT 3
            """).df()

        print(f"\n[ingestion] ✓ Ingestion complete")
        print(f"  Rows in batch          : {result['rows_in_batch']:,}")
        print(f"  Total rows in table    : {result['rows_after']:,}")
        print(f"\n[ingestion] Sample rows:")
        print(sample.to_string(index=False))
        print()

        return {
            "status": "success",
            "run_at": datetime.utcnow().isoformat(),
            "lookback_days": self.lookback_days,
            "sites_configured": len(self._site_config),
            **result,
        }

    def get_site_metadata(self) -> pd.DataFrame:
        """Returns a DataFrame of all configured sites (useful for dim_site loading)."""
        rows = []
        for site in self._site_config:
            rows.append(
                {
                    "site_id": site["site_id"],
                    "site_name": site["site_name"],
                    "region": site["region"],
                    "site_type": site["site_type"],
                    "timezone": site.get("timezone", "UTC"),
                    "capacity_max": site.get("capacity_max", 0),
                    "peak_days": ",".join(site.get("peak_days", [])),
                    "understaffed_threshold": site["volume_thresholds"]["understaffed"],
                    "overstaffed_threshold": site["volume_thresholds"]["overstaffed"],
                }
            )
        return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    framework = IngestionFramework()
    result = framework.run()

    print("\n[ingestion] Site metadata:")
    print(framework.get_site_metadata().to_string(index=False))