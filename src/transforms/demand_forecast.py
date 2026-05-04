"""
demand_forecast.py
─────────────────────────────────────────────────────────────────────────────
Stage 2 transform: applies demand forecasting logic to cleaned volume data.

Algorithm
---------
1. Rolling 7-day average per (site_id, shift) — smooths short-term noise
2. Day-of-week (DOW) index — captures weekday/weekend demand patterns
3. Peak-season multiplier — amplifies forecast during Nov 15 – Jan 5
4. Forecast volume = rolling_avg * DOW_index * peak_multiplier
5. Recommended FTE = ceil(forecast_volume / PACKAGES_PER_FTE_PER_SHIFT)

This is intentionally simple relative to production (no ARIMA, no ML) to
demonstrate the pipeline pattern. In AWS, this layer would be replaced by
a SageMaker endpoint or Redshift ML model.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PACKAGES_PER_FTE_PER_SHIFT = 180
PEAK_SEASON_MULTIPLIER = 1.12
MIN_FTE = 5
ROLLING_WINDOW = 7


def _load_pipeline_config() -> dict:
    cfg_path = PROJECT_ROOT / "config" / "pipeline_config.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)


def compute_demand_forecast(df: pd.DataFrame) -> pd.DataFrame:
    """
    Applies demand forecasting to cleaned volume data.

    Parameters
    ----------
    df : Cleaned DataFrame from cleaner.py

    Returns
    -------
    pd.DataFrame : Input DataFrame enriched with forecast columns:
        rolling_7d_avg, dow_index, forecast_volume,
        recommended_fte
    """
    cfg = _load_pipeline_config()
    pkgs_per_fte = cfg["data"]["packages_per_fte_per_shift"]
    min_fte = cfg["data"]["min_fte_per_shift"]
    rolling_window = cfg["data"]["rolling_window_days"]

    print(f"\n[forecast] Computing demand forecasts")
    print(f"[forecast] Config — FTE/shift: {pkgs_per_fte} pkgs, rolling window: {rolling_window}d")

    df = df.copy().sort_values(["site_id", "shift", "event_date"])

    # ------------------------------------------------------------------
    # 1. Rolling 7-day average per (site_id, shift)
    # ------------------------------------------------------------------
    df["rolling_7d_avg"] = (
        df.groupby(["site_id", "shift"])["package_volume"]
        .transform(lambda x: x.rolling(rolling_window, min_periods=1).mean())
    )

    # ------------------------------------------------------------------
    # 2. Day-of-week index
    # DOW index = mean volume on this DOW / overall mean for (site, shift)
    # Values > 1.0 = above-average day; < 1.0 = below-average
    # ------------------------------------------------------------------
    site_shift_mean = (
        df.groupby(["site_id", "shift"])["package_volume"]
        .transform("mean")
    )
    dow_mean = (
        df.groupby(["site_id", "shift", "day_of_week"])["package_volume"]
        .transform("mean")
    )
    df["dow_index"] = (dow_mean / site_shift_mean).clip(lower=0.5, upper=2.0)

    # ------------------------------------------------------------------
    # 3. Peak-season multiplier
    # ------------------------------------------------------------------
    df["peak_multiplier"] = np.where(df["is_peak_season"], PEAK_SEASON_MULTIPLIER, 1.0)

    # ------------------------------------------------------------------
    # 4. Forecast volume
    # ------------------------------------------------------------------
    df["forecast_volume"] = (
        df["rolling_7d_avg"] * df["dow_index"] * df["peak_multiplier"]
    ).round(1)

    # ------------------------------------------------------------------
    # 5. Recommended FTE
    # ------------------------------------------------------------------
    df["recommended_fte"] = (
        np.ceil(df["forecast_volume"] / pkgs_per_fte).astype(int)
    ).clip(lower=min_fte)

    # ------------------------------------------------------------------
    # Diagnostics
    # ------------------------------------------------------------------
    print(f"[forecast] ✓ Forecast complete")
    print(f"[forecast]   Rows processed       : {len(df):,}")
    print(f"[forecast]   Avg forecast volume  : {df['forecast_volume'].mean():,.0f} pkgs/shift")
    print(f"[forecast]   Avg recommended FTE  : {df['recommended_fte'].mean():.1f}")
    print(f"[forecast]   DOW index range      : [{df['dow_index'].min():.2f}, {df['dow_index'].max():.2f}]")

    return df


def compute_staffing_variance(df: pd.DataFrame, fte_variance_rate: float = 0.12) -> pd.DataFrame:
    """
    Simulates actual FTE staffing with realistic variance around recommended.
    In production, actual FTE would come from the workforce management system.

    Parameters
    ----------
    df                : DataFrame with recommended_fte column
    fte_variance_rate : Standard deviation of staffing variance (12% = realistic)

    Returns
    -------
    pd.DataFrame : Enriched with actual_fte, variance_pct, sla_met
    """
    np.random.seed(99)
    df = df.copy()

    # Simulate actuals: recommended ± noise, biased slightly understaffed
    noise = np.random.normal(loc=-0.03, scale=fte_variance_rate, size=len(df))
    df["actual_fte"] = (df["recommended_fte"] * (1 + noise)).round().astype(int).clip(lower=MIN_FTE)

    # Variance %: (actual - recommended) / recommended
    df["variance_pct"] = (
        (df["actual_fte"] - df["recommended_fte"]) / df["recommended_fte"]
    ).round(4)

    # SLA met: within ±15% of recommended
    df["sla_met"] = df["variance_pct"].between(-0.15, 0.20)

    sla_rate = df["sla_met"].mean()
    understaffed = (df["variance_pct"] < -0.15).sum()
    overstaffed = (df["variance_pct"] > 0.20).sum()

    print(f"\n[forecast] Staffing simulation:")
    print(f"[forecast]   SLA attainment rate  : {sla_rate:.1%}")
    print(f"[forecast]   Understaffed shifts  : {understaffed:,}")
    print(f"[forecast]   Overstaffed shifts   : {overstaffed:,}")

    return df


if __name__ == "__main__":
    # Quick standalone test
    from src.ingestion.ingestion_framework import IngestionFramework
    from src.transforms.cleaner import clean

    framework = IngestionFramework()
    framework.run()
    clean_df = clean()
    forecast_df = compute_demand_forecast(clean_df)
    final_df = compute_staffing_variance(forecast_df)

    print("\nSample output:")
    cols = ["site_id", "event_date", "shift", "package_volume",
            "forecast_volume", "recommended_fte", "actual_fte", "variance_pct", "sla_met"]
    print(final_df[cols].head(10).to_string(index=False))