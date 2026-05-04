"""
synthetic_generator.py
─────────────────────────────────────────────────────────────────────────────
Generates realistic synthetic delivery volume data for all sites defined in
sites.yaml. Applies day-of-week seasonality, holiday spikes, shift-level
variance, and gradual volume growth trends.

This simulates data that would arrive from station management systems in
production (via Kinesis Firehose → S3 → Glue in AWS).
"""

import random
from datetime import date, timedelta

import numpy as np
import pandas as pd
import yaml


# ---------------------------------------------------------------------------
# Seasonality coefficients
# ---------------------------------------------------------------------------
DOW_MULTIPLIERS = {
    "monday": 1.10,
    "tuesday": 1.15,
    "wednesday": 1.18,
    "thursday": 1.12,
    "friday": 1.05,
    "saturday": 0.85,
    "sunday": 0.72,
}

SHIFT_SPLIT = {
    "early_morning": 0.35,
    "afternoon": 0.42,
    "night": 0.23,
}

# Approximate holiday volume spikes (month-day: multiplier)
HOLIDAY_MULTIPLIERS = {
    (11, 25): 1.60,  # Black Friday
    (11, 28): 1.45,  # Cyber Monday proxy
    (12, 20): 1.55,
    (12, 21): 1.55,
    (12, 22): 1.50,
    (12, 23): 1.45,
    (1, 2): 0.60,   # Post-holiday lull
    (1, 3): 0.65,
}


def _load_sites(config_path: str) -> list[dict]:
    with open(config_path) as f:
        return yaml.safe_load(f)["sites"]


def _day_name(d: date) -> str:
    return d.strftime("%A").lower()


def _growth_factor(d: date, start: date, annual_rate: float = 0.08) -> float:
    """Applies a gentle compounding growth trend over the simulation window."""
    days_elapsed = (d - start).days
    return (1 + annual_rate) ** (days_elapsed / 365)


def generate_volume_data(
    config_path: str = "config/sites.yaml",
    lookback_days: int = 365,
    seed: int = 42,
) -> pd.DataFrame:
    """
    Generates a DataFrame of synthetic daily package volume per site per shift.

    Parameters
    ----------
    config_path   : Path to sites.yaml
    lookback_days : Number of historical days to simulate
    seed          : Random seed for reproducibility

    Returns
    -------
    pd.DataFrame with columns:
        site_id, site_name, region, site_type, event_date,
        shift, package_volume, ingested_at
    """
    np.random.seed(seed)
    random.seed(seed)

    sites = _load_sites(config_path)
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=lookback_days - 1)
    date_range = [start_date + timedelta(days=i) for i in range(lookback_days)]

    records = []
    ingested_at = pd.Timestamp.utcnow()

    for site in sites:
        site_id = site["site_id"]
        base_vol = site["base_daily_volume"]
        shifts = [s["name"] for s in site["shifts"]]
        peak_days = set(site.get("peak_days", []))

        for d in date_range:
            dow = _day_name(d)

            # Day-of-week seasonality
            dow_mult = DOW_MULTIPLIERS.get(dow, 1.0)
            if dow in peak_days:
                dow_mult *= 1.05

            # Holiday override
            holiday_mult = HOLIDAY_MULTIPLIERS.get((d.month, d.day), 1.0)

            # Growth trend
            growth = _growth_factor(d, start_date)

            # Daily total with noise
            daily_total = (
                base_vol
                * dow_mult
                * holiday_mult
                * growth
                * np.random.normal(loc=1.0, scale=0.06)
            )
            daily_total = max(0, int(daily_total))

            # Distribute across shifts
            for shift in shifts:
                split = SHIFT_SPLIT.get(shift, 1.0 / len(shifts))
                shift_noise = np.random.normal(loc=1.0, scale=0.04)
                vol = max(0, int(daily_total * split * shift_noise))

                records.append(
                    {
                        "site_id": site_id,
                        "site_name": site["site_name"],
                        "region": site["region"],
                        "site_type": site["site_type"],
                        "event_date": d,
                        "shift": shift,
                        "package_volume": vol,
                        "ingested_at": ingested_at,
                    }
                )

    df = pd.DataFrame(records)
    df["event_date"] = pd.to_datetime(df["event_date"])
    df = df.sort_values(["site_id", "event_date", "shift"]).reset_index(drop=True)

    print(
        f"[synthetic_generator] Generated {len(df):,} rows across "
        f"{df['site_id'].nunique()} sites, "
        f"{lookback_days} days, "
        f"{df['shift'].nunique()} shifts."
    )
    return df


if __name__ == "__main__":
    df = generate_volume_data()
    print(df.head(10).to_string())
    print(f"\nDate range: {df['event_date'].min().date()} → {df['event_date'].max().date()}")
    print(f"Regions:    {sorted(df['region'].unique())}")
    print(f"Site types: {df['site_type'].value_counts().to_dict()}")