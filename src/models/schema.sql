-- ============================================================
-- Amazon LMD Staffing Pipeline — Star Schema DDL
-- DuckDB (Redshift-compatible SQL)
-- ============================================================
-- AWS equivalent: Amazon Redshift with DISTKEY/SORTKEY directives
-- Local: DuckDB columnar engine, same SQL dialect

-- ============================================================
-- DIMENSION: dim_site
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_site (
    site_key              INTEGER PRIMARY KEY,
    site_id               VARCHAR     NOT NULL UNIQUE,
    site_name             VARCHAR     NOT NULL,
    region                VARCHAR     NOT NULL,
    site_type             VARCHAR     NOT NULL,  -- last_mile | sort_center
    timezone              VARCHAR     NOT NULL,
    capacity_max          INTEGER,
    peak_days             VARCHAR,               -- comma-separated day names
    understaffed_threshold DOUBLE,
    overstaffed_threshold  DOUBLE,
    created_at            TIMESTAMP   DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- DIMENSION: dim_date
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_date (
    date_key    INTEGER PRIMARY KEY,  -- YYYYMMDD integer key
    full_date   DATE    NOT NULL UNIQUE,
    year        INTEGER NOT NULL,
    quarter     INTEGER NOT NULL,
    month       INTEGER NOT NULL,
    month_name  VARCHAR NOT NULL,
    week_number INTEGER NOT NULL,
    day_of_week VARCHAR NOT NULL,     -- monday, tuesday, ...
    is_weekend  BOOLEAN NOT NULL,
    is_peak_season BOOLEAN NOT NULL   -- Nov 15 – Jan 5
);

-- ============================================================
-- DIMENSION: dim_shift
-- ============================================================
CREATE TABLE IF NOT EXISTS dim_shift (
    shift_key   INTEGER PRIMARY KEY,
    shift_name  VARCHAR NOT NULL UNIQUE,  -- early_morning | afternoon | night
    start_hour  INTEGER NOT NULL,
    end_hour    INTEGER NOT NULL,
    shift_type  VARCHAR NOT NULL          -- day | evening | overnight
);

-- ============================================================
-- FACT: fact_staffing_demand
-- ============================================================
CREATE TABLE IF NOT EXISTS fact_staffing_demand (
    demand_key          BIGINT PRIMARY KEY,
    site_key            INTEGER NOT NULL REFERENCES dim_site(site_key),
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    shift_key           INTEGER NOT NULL REFERENCES dim_shift(shift_key),

    -- Raw volume
    package_volume      INTEGER NOT NULL,

    -- Forecast
    rolling_7d_avg      DOUBLE,
    dow_index           DOUBLE,
    forecast_volume     DOUBLE,

    -- Staffing
    recommended_fte     INTEGER,
    actual_fte          INTEGER,           -- NULL until actuals are loaded
    variance_pct        DOUBLE,           -- (actual - recommended) / recommended
    sla_met             BOOLEAN,

    -- Audit
    pipeline_run_id     VARCHAR,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================
-- VIEWS: common analytical patterns
-- ============================================================

CREATE OR REPLACE VIEW v_staffing_summary AS
SELECT
    s.site_name,
    s.region,
    s.site_type,
    d.full_date,
    d.day_of_week,
    sh.shift_name,
    f.package_volume,
    f.forecast_volume,
    f.recommended_fte,
    f.actual_fte,
    ROUND(f.variance_pct * 100, 2)  AS variance_pct,
    f.sla_met,
    d.is_peak_season
FROM fact_staffing_demand f
JOIN dim_site  s  ON f.site_key  = s.site_key
JOIN dim_date  d  ON f.date_key  = d.date_key
JOIN dim_shift sh ON f.shift_key = sh.shift_key;


CREATE OR REPLACE VIEW v_site_weekly_summary AS
SELECT
    s.site_name,
    s.region,
    d.year,
    d.week_number,
    SUM(f.package_volume)                               AS total_volume,
    AVG(f.forecast_volume)                              AS avg_daily_forecast,
    SUM(f.recommended_fte)                              AS total_recommended_fte,
    ROUND(AVG(f.variance_pct) * 100, 2)                AS avg_variance_pct,
    SUM(CASE WHEN f.sla_met = false THEN 1 ELSE 0 END) AS sla_breaches
FROM fact_staffing_demand f
JOIN dim_site s ON f.site_key = s.site_key
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY s.site_name, s.region, d.year, d.week_number;


CREATE OR REPLACE VIEW v_understaffed_shifts AS
SELECT
    s.site_name,
    s.region,
    d.full_date,
    d.day_of_week,
    sh.shift_name,
    f.recommended_fte,
    f.actual_fte,
    ROUND(f.variance_pct * 100, 2) AS variance_pct
FROM fact_staffing_demand f
JOIN dim_site  s  ON f.site_key  = s.site_key
JOIN dim_date  d  ON f.date_key  = d.date_key
JOIN dim_shift sh ON f.shift_key = sh.shift_key
WHERE f.variance_pct < -0.15
ORDER BY f.variance_pct ASC;