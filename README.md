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




