import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st


PROJECT_ROOT = Path(__file__).resolve().parent
LATEST_REPORT = PROJECT_ROOT / "data" / "quality_reports" / "quality_report_latest.json"


st.set_page_config(
    page_title="Last-Mile Quality Reporter",
    layout="wide",
)


@st.cache_data
def load_latest_report() -> dict[str, Any] | None:
    if not LATEST_REPORT.exists():
        return None

    with open(LATEST_REPORT) as f:
        return json.load(f)


def load_uploaded_report() -> dict[str, Any] | None:
    uploaded = st.sidebar.file_uploader("Upload a quality report JSON", type="json")
    if uploaded is None:
        return None

    return json.load(uploaded)


def metric_value(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2%}" if 0 <= value <= 1 else f"{value:,.2f}"
    if isinstance(value, int):
        return f"{value:,}"
    return str(value)


def render_status_badge(label: str, status: str | None) -> None:
    normalized = (status or "UNKNOWN").upper()
    if normalized in {"PASSED", "MET", "AVAILABLE"}:
        st.success(f"{label}: {normalized}")
    elif normalized in {"FAILED", "BREACHED", "UNAVAILABLE"}:
        st.error(f"{label}: {normalized}")
    else:
        st.warning(f"{label}: {normalized}")


def flatten_rule_results(report: dict[str, Any]) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    datasets = report.get("datasets", {})

    for dataset_name, dataset in datasets.items():
        for rule in dataset.get("rule_results", []):
            row = {"dataset": dataset_name, **rule}
            rows.append(row)

    return pd.DataFrame(rows)


def render_report(report: dict[str, Any]) -> None:
    st.title("Last-Mile Quality Reporter")
    st.caption("Pipeline quality, forecast health, and SLA attainment")

    overall_status = report.get("overall_status")
    sla = report.get("sla", {})
    forecast_quality = report.get("forecast_quality", {})
    fact_metrics = report.get("datasets", {}).get("fact_staffing_demand", {})

    left, middle, right = st.columns(3)
    with left:
        render_status_badge("Overall", overall_status)
    with middle:
        render_status_badge("SLA", sla.get("status"))
    with right:
        render_status_badge("Fact table", fact_metrics.get("status"))

    st.divider()

    run_left, run_right = st.columns(2)
    run_left.metric("Pipeline Run", report.get("pipeline_run_id", "unknown"))
    run_right.metric("Generated At", report.get("generated_at", "unknown"))

    st.subheader("SLA Summary")
    sla_cols = st.columns(3)
    sla_cols[0].metric("Attainment Rate", metric_value(sla.get("attainment_rate")))
    sla_cols[1].metric("Threshold", metric_value(sla.get("threshold")))
    sla_cols[2].metric("Breaches", metric_value(fact_metrics.get("sla_breaches")))

    st.subheader("Forecast Quality")
    forecast_cols = st.columns(4)
    forecast_cols[0].metric("Rows", metric_value(forecast_quality.get("row_count")))
    forecast_cols[1].metric(
        "Avg Forecast Volume",
        metric_value(forecast_quality.get("avg_forecast_volume")),
    )
    forecast_cols[2].metric(
        "Avg Recommended FTE",
        metric_value(forecast_quality.get("avg_recommended_fte")),
    )
    forecast_cols[3].metric(
        "SLA Attainment",
        metric_value(forecast_quality.get("sla_attainment_rate")),
    )

    st.subheader("Fact Table Metrics")
    if fact_metrics:
        display_metrics = {
            key: value
            for key, value in fact_metrics.items()
            if key not in {"status", "error"}
        }
        table_rows = [
            {"metric": key, "value": metric_value(value)}
            for key, value in display_metrics.items()
        ]
        st.dataframe(
            pd.DataFrame(table_rows),
            hide_index=True,
            use_container_width=True,
        )
    else:
        st.info("No DuckDB fact table metrics are available in this report.")

    rules = flatten_rule_results(report)
    st.subheader("Contract Rule Results")
    if rules.empty:
        st.info("No contract rule results are available in this report.")
    else:
        st.dataframe(rules, hide_index=True, use_container_width=True)

    with st.expander("Raw Report JSON"):
        st.json(report)


def render_empty_state() -> None:
    st.title("Last-Mile Quality Reporter")
    st.info(
        "No quality report was found. Generate one at "
        "`data/quality_reports/quality_report_latest.json` or upload a JSON report "
        "from the sidebar."
    )
    st.code(
        (
            "python -c \"from src.quality.quality_reporter "
            "import generate_pipeline_quality_report\""
        ),
        language="bash",
    )


uploaded_report = load_uploaded_report()
latest_report = load_latest_report()
active_report = uploaded_report or latest_report

if active_report is None:
    render_empty_state()
else:
    render_report(active_report)
