# Amazon Last-Mile Intelligence Pipeline

AI-native, configuration data-driven infrastruvture for labor demand forecasting across Amazon's last-mile delivery network.

## Overview
Amazon's last-mile delivery network spans thousands of delivery stations and sort centers, each requiring hourly staffinf forecasts to meet SLA commitments. A single understaffed shift ripples across package volume, DPMO metrics, and customer satisfaction scores —— at scale.

This project replicates that infrastructure: a **four-layer, AI-native data pipeline** that ingests delivery volume data, transforms it into hourly demand forecasts, enforces data contracts, and exposes a **natural language query interface** powered by THE Claude API. Adding a new delivery station requires one YAML block —— no custom code.

**Domain**: Amazon UTR Planning (last-mile + sort-center labor planning)
**Scale simulated**: 50 delivery stations, 12 month of daily volume, ~18M synthetic data points
**Architecture pattern**: Configuration-driven frameworks -> Airflow orchestration -> DuckDB star schema -> AI agent query layer

## Architecture
<img width="385" height="616" alt="image" src="https://github.com/user-attachments/assets/8a4164c2-2ebd-4f6b-a95e-bb88ec86dc5f" />

## AWS Equivalency Map
This project runs locally but is architected for direct AWS deployment. Evey local component maps 1:1 to a managed AWS service:

<img width="374" height="253" alt="Screenshot 2026-05-03 at 9 44 01 AM" src="https://github.com/user-attachments/assets/7ee62802-fa83-4196-aae5-2e5723186802" />

## Repo Structure
<img width="374" height="755" alt="image" src="https://github.com/user-attachments/assets/2ab459ba-4a1e-48a9-9f9f-b701971ba4e3" />
<img width="374" height="235" alt="image" src="https://github.com/user-attachments/assets/c7840f23-31d2-47f5-b9d5-32d3ebc08af8" />

## Data Model
**Star schema**
<img width="375" height="472" alt="image" src="https://github.com/user-attachments/assets/e4c29bd1-3f2e-4fe4-a50d-0035da0f5210" />

**Layer 1: Configutation-Driven Ingestion**
Site onboarding requires **zero custom code**. A new delivery station is added via a YAML entry:

#### config/sites.yaml
sites:
  - site_id: DJE5
    site_name: Jersey City Delivery Station
    region: northeast
    site_type: last_mile
    timezone: America/New York
    capacity_max: 850
    shifts:
      - name: early morning     start: "05:00"  end:"13:00"
      - name: afternoon         start:"13:00"   end:"21:00"
      - name: night             start:"21:00"   end:"05:00"
    volume_thresholds:
      understaffed: 0.85
      overstaffed: 1.20
    peak_days: [monday, tuesday, wednesday]

  - site_id: DNY9
    site_name: Brooklyn Sort Center
    region: northeast
    site_type: sort_center
    ...

The ingestion_framework.py reads this config, generates synthetic volume data for each site/shift/day combination, and loads it into the raw layer —— no site-specifi logic anywhere in the codebase.

**Layer 2: Airflow ETL Pipeline**

<em>DAG: lmd_staffing_pipeline

<img width="620" height="488" alt="image" src="https://github.com/user-attachments/assets/e70be86b-0634-4587-9c23-21b12cb7b42e" />

Schedule: 0 6 * * * (daily at 6AM, before shift planning windoes open)

<em> Tranformation Logic
#### Rolling 7-day average with day-of-week seasonality
def compute_demand_forecast(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["site_id", "event_date"])
    df["rolling_7d_avg"] = (
        df.groupby(["site_id", "shift"])["package_volume"]
          .transform(lambda x: x.rolling(7, min_periods=1).mean())
    )
    df["dow_index"] = df.groupby(["site_id", "shift", "day_of_week"])["package_volume"] \
                        .transform("mean") / df.groupby(["site_id", "shift"])["package_volume"] \
                        .transform("mean")
    df["forecast_volume"] = df["rolling_7d_avg"] * df["dow_index"]
    df["recommended_fte"] = (df["forecast_volume"] / PACKAGES_PER_FTE_PER_SHIFT).ceil()
    return df

**Layer 3: Data Quality & Contracts**

Every DAG run produces a quality report. SLA Breaches trigger Airflow alerts.
{
  "run_id": "lmd_staffing_pipeline__2025-01-21T06:00:00",
  "dataset": "fact_staffing_demand",
  "contract_version": "1.2.0",
  "certification_status": "certified",
  "quality_checks": {
    "null_check":        { "status": "PASSED", "null_rate": 0.0 },
    "volume_range":      { "status": "PASSED", "violations": 0 },
    "duplicate_check":   { "status": "PASSED", "duplicates": 0 },
    "sla_latency":       { "status": "PASSED", "latency_minutes": 47 },
    "row_count_anomaly": { "status": "PASSED", "expected": 2700, "actual": 2700 }
  },
  "overall_status": "PASSED",
  "generated_at": "2025-01-21T06:47:23Z"
}

**Layer 4: AI Agent / MCP Interface**
A FastAPI server exposes a natural language query endpoint. The Claude API translates questions into validated SQL, executes it against DuckDB, and returns structured results.

<em>Endpoints
<img width="568" height="202" alt="image" src="https://github.com/user-attachments/assets/30c51842-bb9e-457a-a8b6-38304eeef87a" />

Example Queries
###### Which sites had staffing shortfalls last Tuesday?
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which sites had staffing shortfalls last Tuesday?"}'

###### Response:
{
  "question": "Which sites had staffing shortfalls last Tuesday?",
  "generated_sql": "SELECT site_name, shift, actual_fte, recommended_fte, variance_pct FROM fact_staffing_demand f JOIN dim_site s ON f.site_key = s.site_key JOIN dim_date d ON f.date_key = d.date_key WHERE d.full_date = CURRENT_DATE - INTERVAL 6 DAY AND d.day_of_week = 'Tuesday' AND f.variance_pct < -0.15 ORDER BY variance_pct ASC",
  "row_count": 7,
  "results": [
    { "site_name": "Jersey City DS", "shift": "afternoon", "actual_fte": 62, "recommended_fte": 78, "variance_pct": -20.5 },
    ...
  ],
  "execution_ms": 34
}

Other example natural language queries:
- "Show top 5 stations by weekend demand variance this month"
- "Which regions consistently exceed staffing SLAs?"
- "Compare early morning vs afternoon shift volume trends for northeast sites"
- "Flag any datasets that failed quality certification this week"

##### Agent architecture
# src/agent/nl_to_sql.py
async def natural_language_to_sql(question: str, schema_context: str) -> str:
    """Sends NL question + schema to Claude API, returns validated SQL."""
    prompt = f"""
    You are a SQL expert for Amazon's last-mile labor planning data warehouse.
    
    Schema:
    {schema_context}
    
    Rules:
    - Only generate SELECT statements
    - Always JOIN through dimension tables (never filter on fact table keys directly)
    - Use DuckDB SQL syntax
    - Return ONLY the SQL query, no explanation
    
    Question: {question}
    """
    response = anthropic_client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}]
    )
    sql = response.content[0].text.strip()
    validate_sql(sql)   # Blocks non-SELECT statements
    return sql

## Setup & Installation
**Prerequisites**

- Docker + Docker Compose
- Python 3.11+
- Anthropic API key

**Quick Start**
#### 1. Clone the repo
git clone https://github.com/sajansshergill/amazon-lmd-staffing-pipeline.git
cd amazon-lmd-staffing-pipeline

#### 2. Configure environment
cp .env.example .env
##### Add your Anthropic API key to .env:
##### ANTHROPIC_API_KEY=sk-ant-...

#### 3. Start all services (Airflow + Agent + DuckDB)
docker-compose up -d

#### 4. Verify services
open http://localhost:8080   # Airflow UI  (admin / admin)
open http://localhost:8000   # Agent API   (/docs for Swagger)

#### 5. Trigger the pipeline manually
airflow dags trigger lmd_staffing_pipeline

#### 6. Run a natural language query
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{"question": "Which sites had the highest demand variance this week?"}'

**Local-Only (No Docker)**
pip install -r requirements.txt

#### Generate synthetic data + load DuckDB
python src/ingestion/ingestion_framework.py

#### Run transforms
python src/transforms/demand_forecast.py

#### Start agent server
uvicorn src.agent.server:app --reload --port 8000

## Configuration Reference

<em> pipeline_config.yaml
pipeline:
  schedule: "0 6 * * *"
  max_active_runs: 1
  catchup: false

data:
  packages_per_fte_per_shift: 180
  understaffed_threshold: -0.15   # 15% below recommended
  overstaffed_threshold:  0.20    # 20% above recommended
  rolling_window_days: 7

quality:
  sla_latency_max_hours: 2
  null_tolerance: 0.0
  duplicate_tolerance: 0.0
  alert_on_failure: true

agent:
  model: claude-sonnet-4-20250514
  max_tokens: 500
  temperature: 0
  allowed_statements: [SELECT]

## Testing
###3 Run all tests
pytest tests/ -v

#### Run with coverage
pytest tests/ --cov=src --cov-report=html

#### Test agent SQL generation only
pytest tests/test_agent.py -v -k "test_nl_to_sql"

**Tests coverage targets:** ingestion (90%), tranforms(85%), quality checks (95%), agent SQL validation (100%)

## Key Design Decisions
**Why DuckDB over SQLite?**
DuckDB uses ANSI SQL with columnar storage —— identical query patterns to Amazon Redshift. The entire data layer swaps to Redshift by changing one connection string. SQLite's row-based engine would require query rewrites.

**Why YAML configs over database-driven site management?**
Site configs version-controlled artifacts, not database rows. Every site addition, shift changes, or threshold update is auditable through Git history —— critical for planning system where a configuratio error cascades into hundreds of mis-staffed shifts.

**Why Claude API for SQL generation instead of fine-tuned model?**
The schema evolves. A fine-tuned text-to-SQL model requires retraining on every schema changes. Prompting Claude with the current schema context via /schema means the agent stays accurate as the data model grows —— zero training overhead.

**Why reject non-SELECT statements in the agent?**
This is a read-only analytical interfact. The validate_sql() guard prevents any INSERT, UPDATE, DROP OR DDL from being executed through the natural language layer —— a hard requirement for production data systems.

## Roadmap
- Add Kinesis Firehose simulation for real-time volume streaming
- Implement dbt models layer on top of DuckDB for metric standardization
- Built multi-agent workflow: orchestrator -> SQL agent -> quality agent -> summary agent
- Add IAM role simulation for row-level security by region
- Grafana dashboard for pipeline health + data quality KPIs
- Extent agent to support configuration management ("add a new sir to sites.yaml")

## Skills Demonstrated
<img width="605" height="319" alt="Screenshot 2026-05-03 at 1 29 13 PM" src="https://github.com/user-attachments/assets/9dbee873-fcdf-4e07-aead-794a1145f7d2" />

<em> Built to demonstrate AI-native data engineering patterns aligned with Amazon UTR Planning Tech's infrastructure direction.Share
