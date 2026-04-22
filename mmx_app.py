from __future__ import annotations

from datetime import datetime
from html import escape
import os
from typing import Mapping

import altair as alt
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from marketing_mix_model import (
    CHANNEL_LABELS,
    CUSTOMER_COL,
    DATE_COL,
    DEFAULT_CHANNELS,
    TARGET_COL,
    build_response_curve,
    estimate_channel_contribution,
    fit_marketing_mix_model,
    generate_recommendations,
    generate_sample_marketing_data,
    get_baseline_scenario,
    normalize_marketing_data,
    optimize_budget,
    prepare_marketing_data,
    simulate_spend_change,
)

try:
    from marketing_mix_model import (
        DEFAULT_ADSTOCK_DECAYS,
        FIELD_LABELS,
        OPTIONAL_MODEL_COLUMNS,
        REQUIRED_MODEL_COLUMNS,
        apply_column_mapping,
        assess_data_readiness,
        compare_candidate_models,
        fit_bayesian_marketing_mix_model,
        predict_with_interval,
        suggest_column_mapping,
    )
except ImportError:
    DEFAULT_ADSTOCK_DECAYS = {
        "google_ads": 0.35,
        "meta_ads": 0.40,
        "instagram_ads": 0.38,
        "tv_ads": 0.65,
        "email_marketing": 0.20,
        "promotions": 0.25,
    }
    FIELD_LABELS = {DATE_COL: "Date", TARGET_COL: "Revenue", CUSTOMER_COL: "New Customers", **CHANNEL_LABELS}
    REQUIRED_MODEL_COLUMNS = (DATE_COL, TARGET_COL, *DEFAULT_CHANNELS)
    OPTIONAL_MODEL_COLUMNS = (CUSTOMER_COL,)

    def suggest_column_mapping(data: pd.DataFrame) -> dict[str, str]:
        return {column: column if column in data.columns else "" for column in (*REQUIRED_MODEL_COLUMNS, *OPTIONAL_MODEL_COLUMNS)}

    def apply_column_mapping(data: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
        frame = data.copy()
        for canonical, source in mapping.items():
            if source and source in data.columns:
                frame[canonical] = data[source]
        return frame

    def assess_data_readiness(data: pd.DataFrame) -> dict[str, object]:
        missing = [column for column in REQUIRED_MODEL_COLUMNS if column not in normalize_marketing_data(data).columns]
        score = max(0, 100 - 12 * len(missing))
        return {
            "score": score,
            "status": "Ready" if not missing else "Needs cleanup",
            "checks": pd.DataFrame(
                [{"Area": "Required columns", "Status": "Ready" if not missing else "Needs attention", "Detail": ", ".join(missing) or "Required fields present."}]
            ),
        }

    def compare_candidate_models(*args, **kwargs) -> pd.DataFrame:
        return pd.DataFrame()

    def fit_bayesian_marketing_mix_model(data: pd.DataFrame):
        return fit_marketing_mix_model(data)

    def predict_with_interval(model, data, confidence: float = 0.80) -> pd.DataFrame:
        prediction = model.predict(data)
        error = float(model.metrics.get("rmse", 1.0))
        return pd.DataFrame({"Prediction": prediction, "Lower": prediction - error, "Upper": prediction + error})

try:
    from marketing_mix_model import evaluate_model_against_baseline
except ImportError:
    def evaluate_model_against_baseline(*args, **kwargs):
        raise ValueError("Train/test evaluation requires the latest marketing_mix_model.py.")


load_dotenv()

st.set_page_config(
    page_title="Mixalyzer",
    page_icon="M",
    layout="wide",
    initial_sidebar_state="expanded",
)


st.markdown(
    """
    <style>
      #MainMenu, footer {visibility: hidden;}
      .stApp {
        background:
          linear-gradient(180deg, #07100f 0%, #08131a 44%, #0d1117 100%);
      }
      .block-container {
        max-width: 1420px;
        padding-top: 1.4rem;
        padding-bottom: 2.5rem;
      }
      .brand-kicker {
        color: #24c6a1;
        font-size: 0.84rem;
        font-weight: 800;
        letter-spacing: 0.08em;
        text-transform: uppercase;
        margin-bottom: 0.2rem;
      }
      .brand-title {
        color: #f8fafc;
        font-size: 3rem;
        font-weight: 850;
        line-height: 1;
        margin: 0 0 0.35rem;
      }
      .brand-subtitle {
        color: rgba(226, 232, 240, 0.76);
        max-width: 860px;
        font-size: 1rem;
        line-height: 1.5;
        margin-bottom: 1.2rem;
      }
      .metric-label {
        color: rgba(226, 232, 240, 0.72);
        font-size: 0.82rem;
        margin-bottom: 0.25rem;
      }
      .metric-value {
        color: #f8fafc;
        font-size: 1.65rem;
        font-weight: 750;
        line-height: 1.1;
      }
      .metric-delta {
        color: #38d7c1;
        font-size: 0.82rem;
        margin-top: 0.18rem;
      }
      .kpi-card {
        border: 1px solid rgba(148, 163, 184, 0.18);
        background: linear-gradient(180deg, rgba(19, 29, 38, 0.82), rgba(12, 19, 27, 0.84));
        border-radius: 8px;
        padding: 0.8rem 0.95rem;
        min-height: 104px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        overflow: visible;
      }
      .kpi-label {
        color: rgba(226, 232, 240, 0.82);
        font-size: 0.86rem;
        font-weight: 650;
        line-height: 1.2;
        margin-bottom: 0.55rem;
        white-space: normal;
        overflow-wrap: anywhere;
      }
      .kpi-value {
        color: #f8fafc;
        font-size: 1.55rem;
        font-weight: 720;
        line-height: 1.16;
        letter-spacing: 0;
        white-space: normal;
        overflow-wrap: anywhere;
        word-break: normal;
      }
      .kpi-delta {
        color: #38d7c1;
        font-size: 0.86rem;
        font-weight: 650;
        line-height: 1.2;
        margin-top: 0.45rem;
        white-space: normal;
        overflow-wrap: anywhere;
      }
      .panel {
        border: 1px solid rgba(148, 163, 184, 0.18);
        background: rgba(13, 21, 29, 0.76);
        border-radius: 8px;
        padding: 1rem;
      }
      .recommendation {
        border-left: 3px solid #38d7c1;
        background: rgba(56, 215, 193, 0.08);
        padding: 0.7rem 0.8rem;
        margin-bottom: 0.65rem;
        border-radius: 6px;
        color: #e5eefb;
      }
      .hero {
        border: 1px solid rgba(148, 163, 184, 0.18);
        background:
          linear-gradient(135deg, rgba(36, 198, 161, 0.16), rgba(246, 200, 95, 0.07)),
          rgba(13, 21, 29, 0.82);
        border-radius: 8px;
        padding: 1.35rem 1.45rem;
        margin-bottom: 1rem;
      }
      .hero h2 {
        color: #f8fafc;
        font-size: 2rem;
        line-height: 1.12;
        margin: 0 0 0.45rem;
      }
      .hero p {
        color: rgba(226, 232, 240, 0.78);
        font-size: 1rem;
        margin: 0;
      }
      .feature-card {
        border: 1px solid rgba(148, 163, 184, 0.16);
        background: rgba(13, 21, 29, 0.72);
        border-radius: 8px;
        padding: 0.9rem;
        min-height: 120px;
      }
      .feature-card h4 {
        color: #f8fafc;
        margin: 0 0 0.35rem;
      }
      .feature-card p {
        color: rgba(226, 232, 240, 0.74);
        margin: 0;
      }
      .risk-ok {
        border-left: 3px solid #38d7c1;
      }
      .risk-watch {
        border-left: 3px solid #f59e0b;
      }
      .readiness-good {
        border-left: 4px solid #24c6a1;
      }
      .readiness-watch {
        border-left: 4px solid #f6c85f;
      }
      .readiness-bad {
        border-left: 4px solid #fb7185;
      }
      div[data-testid="stTabs"] button p {
        font-weight: 700;
      }
      div[data-testid="stMetric"] {
        border: 1px solid rgba(148, 163, 184, 0.16);
        background: rgba(15, 23, 42, 0.55);
        border-radius: 8px;
        padding: 0.8rem 0.95rem;
      }
      div[data-testid="stMetricValue"],
      div[data-testid="stMetricValue"] div {
        white-space: normal !important;
        overflow: visible !important;
        text-overflow: clip !important;
      }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(show_spinner=False)
def load_sample_data() -> pd.DataFrame:
    return generate_sample_marketing_data()


@st.cache_data(show_spinner=False)
def train_model(data: pd.DataFrame, regularization: float, model_engine: str):
    if model_engine.startswith("Bayesian"):
        model = fit_bayesian_marketing_mix_model(data)
    else:
        model = fit_marketing_mix_model(data, regularization=regularization)
    contribution = estimate_channel_contribution(model, data)
    baseline = get_baseline_scenario(data)
    return model, contribution, baseline


@st.cache_data(show_spinner=False)
def evaluate_current_model(data: pd.DataFrame, regularization: float):
    return evaluate_model_against_baseline(data, regularization=regularization)


@st.cache_data(show_spinner=False)
def compare_models(data: pd.DataFrame, regularization: float):
    return compare_candidate_models(data, regularization=regularization)


def money(value: float) -> str:
    return f"${value:,.0f}"


def pct(value: float) -> str:
    return f"{value:.1f}%"


def signed_money(value: float) -> str:
    prefix = "+" if value >= 0 else "-"
    return f"{prefix}${abs(value):,.0f}"


def render_metric_card(label: object, value: object, delta: object | None = None) -> None:
    delta_markup = (
        f'<div class="kpi-delta">{escape(str(delta))}</div>'
        if delta is not None and str(delta) != ""
        else ""
    )
    st.markdown(
        f"""
        <div class="kpi-card">
          <div class="kpi-label">{escape(str(label))}</div>
          <div class="kpi-value">{escape(str(value))}</div>
          {delta_markup}
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_loader(message: str) -> None:
    st.markdown(
        f"""
        <div class="panel">
          <strong>{escape(message)}</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )


def dataframe_to_markdown_table(frame: pd.DataFrame) -> str:
    headers = list(frame.columns)
    rows = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for _, row in frame.iterrows():
        cells = []
        for value in row:
            if isinstance(value, float):
                cells.append(f"{value:,.2f}")
            else:
                cells.append(str(value))
        rows.append("| " + " | ".join(cells) + " |")
    return "\n".join(rows)


def read_input_data(upload) -> pd.DataFrame:
    if upload is None:
        return load_sample_data()
    return pd.read_csv(upload)


def build_column_mapping_controls(raw_frame: pd.DataFrame, upload_present: bool) -> dict[str, str]:
    if not upload_present:
        return {column: column for column in (*REQUIRED_MODEL_COLUMNS, *OPTIONAL_MODEL_COLUMNS)}

    suggestions = suggest_column_mapping(raw_frame)
    options = [""] + list(raw_frame.columns)
    mapping: dict[str, str] = {}
    for canonical in (*REQUIRED_MODEL_COLUMNS, *OPTIONAL_MODEL_COLUMNS):
        suggested = suggestions.get(canonical, "")
        default_index = options.index(suggested) if suggested in options else 0
        mapping[canonical] = st.selectbox(
            FIELD_LABELS.get(canonical, canonical),
            options=options,
            index=default_index,
            key=f"column_map_{canonical}",
        )
    return mapping


def readiness_class(status: str) -> str:
    if status == "Ready":
        return "readiness-good"
    if status == "Usable with caveats":
        return "readiness-watch"
    return "readiness-bad"


def confidence_range_chart(summary: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(summary)
        .mark_bar(cornerRadius=5)
        .encode(
            x=alt.X("Case:N", title=None),
            y=alt.Y("Revenue Delta:Q", title="Predicted revenue impact"),
            color=alt.Color(
                "Case:N",
                scale=alt.Scale(range=["#fb7185", "#24c6a1", "#f6c85f"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Case:N"),
                alt.Tooltip("Revenue Delta:Q", format="$,.0f"),
            ],
        )
        .properties(height=240)
    )


def model_comparison_chart(comparison: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(comparison)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Model:N", sort=alt.SortField("MAPE", order="ascending"), title=None),
            y=alt.Y("MAPE:Q", title="MAPE"),
            color=alt.Color(
                "Model:N",
                scale=alt.Scale(range=["#24c6a1", "#8ab4ff", "#f6c85f", "#fb7185"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Model:N"),
                alt.Tooltip("MAPE:Q", format=".2f"),
                alt.Tooltip("RMSE:Q", format="$,.0f"),
                alt.Tooltip("R2:Q", format=".2f"),
            ],
        )
        .properties(height=300)
    )


def simulate_with_confidence(model, baseline: Mapping[str, object], changes: Mapping[str, float], confidence: float):
    try:
        return simulate_spend_change(model, baseline, changes, confidence=confidence)
    except TypeError:
        result = simulate_spend_change(model, baseline, changes)
        spread = float(model.metrics.get("rmse", 0.0) or 0.0)
        result.setdefault("revenue_delta_low", result["revenue_delta"] - spread)
        result.setdefault("revenue_delta_high", result["revenue_delta"] + spread)
        return result


def optimize_with_confidence(model, baseline: Mapping[str, object], budget: float, confidence: float):
    try:
        return optimize_budget(model, baseline, total_budget=budget, confidence=confidence)
    except TypeError:
        result = optimize_budget(model, baseline, total_budget=budget)
        spread = float(model.metrics.get("rmse", 0.0) or 0.0)
        result.setdefault("revenue_delta_low", result["revenue_delta"] - spread)
        result.setdefault("revenue_delta_high", result["revenue_delta"] + spread)
        return result


def build_csv_template() -> bytes:
    template = pd.DataFrame(
        [
            {
                DATE_COL: "2026-01-04",
                "google_ads": 12000,
                "meta_ads": 9000,
                "instagram_ads": 7000,
                "tv_ads": 18000,
                "email_marketing": 2500,
                "promotions": 4500,
                TARGET_COL: 260000,
                CUSTOMER_COL: 720,
            }
        ]
    )
    return template.to_csv(index=False).encode("utf-8")


def prediction_chart(predictions: pd.DataFrame) -> alt.Chart:
    long = predictions.melt(
        id_vars=[DATE_COL],
        value_vars=["Actual Revenue", "MMX Prediction", "Baseline Prediction"],
        var_name="Series",
        value_name="Revenue",
    )
    return (
        alt.Chart(long)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X(f"{DATE_COL}:T", title=None),
            y=alt.Y("Revenue:Q", title="Revenue"),
            color=alt.Color(
                "Series:N",
                scale=alt.Scale(range=["#f8fafc", "#38d7c1", "#f97316"]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip(f"{DATE_COL}:T", title="Week"),
                alt.Tooltip("Series:N"),
                alt.Tooltip("Revenue:Q", format="$,.0f"),
            ],
        )
        .properties(height=330)
    )


def evaluation_metric_chart(evaluation: Mapping[str, object]) -> alt.Chart:
    model_metrics = evaluation["model_metrics"]
    baseline_metrics = evaluation["baseline_metrics"]
    rows = [
        {"Metric": "MAPE", "Model": "MMX", "Value": model_metrics["mape"]},
        {"Metric": "MAPE", "Model": "Baseline", "Value": baseline_metrics["mape"]},
        {"Metric": "RMSE", "Model": "MMX", "Value": model_metrics["rmse"]},
        {"Metric": "RMSE", "Model": "Baseline", "Value": baseline_metrics["rmse"]},
    ]
    return (
        alt.Chart(pd.DataFrame(rows))
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Metric:N", title=None),
            y=alt.Y("Value:Q", title=None),
            color=alt.Color(
                "Model:N",
                scale=alt.Scale(range=["#38d7c1", "#f97316"]),
                legend=alt.Legend(orient="top"),
            ),
            xOffset="Model:N",
            tooltip=[alt.Tooltip("Model:N"), alt.Tooltip("Metric:N"), alt.Tooltip("Value:Q", format=",.2f")],
        )
        .properties(height=300)
    )


def build_responsible_ai_audit(
    data: pd.DataFrame,
    evaluation: Mapping[str, object] | None,
    contribution: pd.DataFrame,
) -> pd.DataFrame:
    weeks = len(data)
    spend = data[list(DEFAULT_CHANNELS)].sum()
    total_spend = float(spend.sum())
    top_channel_share = float(spend.max() / total_spend) if total_spend else 0.0
    has_customers = CUSTOMER_COL in data.columns and float(data[CUSTOMER_COL].sum()) > 0
    mape = float(evaluation["model_metrics"]["mape"]) if evaluation else None

    rows = [
        {
            "Area": "Data representativeness",
            "Status": "Watch" if weeks < 104 else "Managed",
            "Risk": "Short history can overstate recent campaigns or miss yearly seasonality.",
            "Mitigation": "Use at least two years of weekly data where possible and refresh the model monthly.",
        },
        {
            "Area": "Channel concentration",
            "Status": "Watch" if top_channel_share > 0.45 else "Managed",
            "Risk": f"The largest channel is {top_channel_share:.0%} of tracked spend.",
            "Mitigation": "Set optimizer bounds and review recommendations before making large reallocations.",
        },
        {
            "Area": "Customer privacy",
            "Status": "Managed" if has_customers else "Watch",
            "Risk": "Customer-level data may contain sensitive identifiers if raw CRM exports are uploaded.",
            "Mitigation": "Use aggregated weekly channel data only; avoid names, emails, device IDs, or protected attributes.",
        },
        {
            "Area": "Model reliability",
            "Status": "Watch" if mape is not None and mape > 10 else "Managed",
            "Risk": "Forecast error can lead to overconfident budget changes.",
            "Mitigation": "Compare against the baseline, monitor MAPE, and show best/base/worst ranges before launch.",
        },
        {
            "Area": "Recommendation hallucination",
            "Status": "Managed",
            "Risk": "Generated recommendations may sound confident even when evidence is weak.",
            "Mitigation": "Ground the AI narrative in model outputs and fall back to deterministic recommendations.",
        },
        {
            "Area": "Fairness and proxy bias",
            "Status": "Watch",
            "Risk": "Geo, audience, or platform targeting can proxy for sensitive groups even without explicit demographics.",
            "Mitigation": "Audit campaign segments outside this prototype before activating recommendations in production.",
        },
    ]

    if not contribution.empty and float(contribution["ROI"].max()) > 3 * max(float(contribution["ROI"].median()), 0.01):
        rows.append(
            {
                "Area": "Outlier ROI",
                "Status": "Watch",
                "Risk": "One channel appears much stronger than the rest, which can signal sparse data or attribution bias.",
                "Mitigation": "Validate with holdout tests or incrementality experiments before major spend shifts.",
            }
        )

    return pd.DataFrame(rows)


def build_executive_report(
    data: pd.DataFrame,
    contribution: pd.DataFrame,
    optimization: Mapping[str, object],
    simulation: Mapping[str, object],
    evaluation: Mapping[str, object] | None,
    recommendations: list[str],
) -> str:
    total_spend_value = float(data[list(DEFAULT_CHANNELS)].sum().sum())
    total_revenue_value = float(data[TARGET_COL].sum())
    total_customers_value = float(data[CUSTOMER_COL].sum()) if CUSTOMER_COL in data.columns else 0.0
    cac_value = total_spend_value / total_customers_value if total_customers_value else None
    top_roi = contribution.sort_values("ROI", ascending=False).iloc[0]
    weakest_roi = contribution.sort_values("ROI", ascending=True).iloc[0]
    allocation = optimization["allocation"][
        ["Channel", "Current Spend", "Recommended Spend", "Spend Shift", "Change %"]
    ].copy()

    lines = [
        "# Marketing Mix Optimization Executive Report",
        "",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "",
        "## Business KPI Snapshot",
        f"- Revenue analyzed: {money(total_revenue_value)}",
        f"- Marketing spend analyzed: {money(total_spend_value)}",
        f"- Marketing ROI: {total_revenue_value / total_spend_value:.2f}x" if total_spend_value else "- Marketing ROI: N/A",
        f"- CAC: {money(cac_value)}" if cac_value is not None else "- CAC: N/A",
        "",
        "## Recommended Budget Action",
        f"- Current next-period budget: {money(optimization['current_budget'])}",
        f"- Recommended next-period budget: {money(optimization['recommended_budget'])}",
        f"- Predicted revenue impact: {signed_money(optimization['revenue_delta'])} ({optimization['revenue_delta_pct']:.1f}%)",
        "",
        "## Channel Insights",
        f"- Highest estimated ROI: {top_roi['Channel']} at {float(top_roi['ROI']):.2f}x",
        f"- Lowest estimated ROI: {weakest_roi['Channel']} at {float(weakest_roi['ROI']):.2f}x",
        "",
        "## Model Evaluation",
    ]

    if evaluation:
        lines.extend(
            [
                f"- Train rows: {evaluation['train_rows']}; test rows: {evaluation['test_rows']}",
                f"- MMX MAPE: {evaluation['model_metrics']['mape']:.2f}%",
                f"- Baseline MAPE: {evaluation['baseline_metrics']['mape']:.2f}%",
                f"- RMSE improvement vs baseline: {evaluation['rmse_improvement_pct']:.1f}%",
            ]
        )
    else:
        lines.append("- Evaluation unavailable because the uploaded dataset is too short.")

    lines.extend(["", "## Active Simulation", f"- Predicted revenue impact: {signed_money(simulation['revenue_delta'])} ({simulation['revenue_delta_pct']:.1f}%)", ""])
    lines.append("## Recommended Allocation")
    lines.append(dataframe_to_markdown_table(allocation))
    lines.extend(["", "## Management Recommendations"])
    lines.extend([f"- {item}" for item in recommendations])
    lines.extend(
        [
            "",
            "## Responsible AI Notes",
            "- Treat recommendations as decision support, not automated media-buying instructions.",
            "- Validate large reallocations with incrementality tests or controlled experiments.",
            "- Use aggregated spend/revenue/customer data and avoid customer-level identifiers.",
        ]
    )
    return "\n".join(lines)


def line_spend_revenue_chart(data: pd.DataFrame) -> alt.Chart:
    frame = data.copy()
    frame["total_spend"] = frame[list(DEFAULT_CHANNELS)].sum(axis=1)
    long = frame.melt(
        id_vars=["date"],
        value_vars=[TARGET_COL, "total_spend"],
        var_name="Metric",
        value_name="Value",
    )
    long["Metric"] = long["Metric"].map({"revenue": "Revenue", "total_spend": "Marketing Spend"})

    return (
        alt.Chart(long)
        .mark_line(strokeWidth=3)
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("Value:Q", title=None),
            color=alt.Color(
                "Metric:N",
                scale=alt.Scale(range=["#38d7c1", "#7c8cff"]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip("date:T", title="Week"),
                alt.Tooltip("Metric:N"),
                alt.Tooltip("Value:Q", format="$,.0f"),
            ],
        )
        .properties(height=320)
    )


def channel_spend_chart(data: pd.DataFrame) -> alt.Chart:
    long = data.melt(
        id_vars=["date"],
        value_vars=list(DEFAULT_CHANNELS),
        var_name="channel",
        value_name="Spend",
    )
    long["Channel"] = long["channel"].map(CHANNEL_LABELS)
    return (
        alt.Chart(long)
        .mark_area(opacity=0.88)
        .encode(
            x=alt.X("date:T", title=None),
            y=alt.Y("Spend:Q", stack=True, title="Weekly spend"),
            color=alt.Color("Channel:N", legend=alt.Legend(orient="bottom")),
            tooltip=[
                alt.Tooltip("date:T", title="Week"),
                alt.Tooltip("Channel:N"),
                alt.Tooltip("Spend:Q", format="$,.0f"),
            ],
        )
        .properties(height=280)
    )


def roi_chart(contribution: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(contribution)
        .mark_bar(cornerRadiusTopRight=4, cornerRadiusBottomRight=4)
        .encode(
            y=alt.Y("Channel:N", sort="-x", title=None),
            x=alt.X("ROI:Q", title="Estimated revenue per spend dollar"),
            color=alt.Color(
                "ROI:Q",
                scale=alt.Scale(range=["#f97316", "#38d7c1"]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Channel:N"),
                alt.Tooltip("Spend:Q", format="$,.0f"),
                alt.Tooltip("Estimated Contribution:Q", format="$,.0f"),
                alt.Tooltip("ROI:Q", format=".2f"),
            ],
        )
        .properties(height=300)
    )


def contribution_chart(contribution: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(contribution)
        .mark_arc(innerRadius=62, outerRadius=122)
        .encode(
            theta=alt.Theta("Estimated Contribution:Q"),
            color=alt.Color("Channel:N", legend=alt.Legend(orient="bottom")),
            tooltip=[
                alt.Tooltip("Channel:N"),
                alt.Tooltip("Estimated Contribution:Q", format="$,.0f"),
                alt.Tooltip("Contribution Share:Q", format=".1%"),
            ],
        )
        .properties(height=300)
    )


def spend_shift_chart(details: pd.DataFrame, current_col: str, scenario_col: str) -> alt.Chart:
    long = details.melt(
        id_vars=["Channel"],
        value_vars=[current_col, scenario_col],
        var_name="Scenario",
        value_name="Spend",
    )
    return (
        alt.Chart(long)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("Channel:N", title=None),
            y=alt.Y("Spend:Q", title="Weekly spend"),
            color=alt.Color(
                "Scenario:N",
                scale=alt.Scale(range=["#94a3b8", "#38d7c1"]),
                legend=alt.Legend(orient="top"),
            ),
            tooltip=[
                alt.Tooltip("Channel:N"),
                alt.Tooltip("Scenario:N"),
                alt.Tooltip("Spend:Q", format="$,.0f"),
            ],
        )
        .properties(height=310)
    )


def response_curve_chart(curve: pd.DataFrame) -> alt.Chart:
    return (
        alt.Chart(curve)
        .mark_line(point=True, strokeWidth=3)
        .encode(
            x=alt.X("Spend:Q", title="Weekly spend"),
            y=alt.Y("Predicted Revenue:Q", title="Predicted revenue"),
            tooltip=[
                alt.Tooltip("Spend:Q", format="$,.0f"),
                alt.Tooltip("Predicted Revenue:Q", format="$,.0f"),
                alt.Tooltip("Incremental Revenue:Q", format="$,.0f"),
            ],
        )
        .properties(height=310)
    )


def maybe_generate_openai_recommendations(
    contribution: pd.DataFrame,
    optimization: Mapping[str, object],
    simulation: Mapping[str, object],
) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        from openai import OpenAI

        client = OpenAI(api_key=api_key)
        allocation = optimization["allocation"].copy()
        payload = {
            "top_roi": contribution[["Channel", "ROI", "Estimated Contribution"]].head(3).to_dict("records"),
            "recommended_allocation": allocation[
                ["Channel", "Current Spend", "Recommended Spend", "Change %"]
            ].to_dict("records"),
            "optimized_revenue_delta_pct": optimization["revenue_delta_pct"],
            "simulation_revenue_delta_pct": simulation["revenue_delta_pct"],
        }
        response = client.chat.completions.create(
            model=os.getenv("OPENAI_MMX_MODEL", os.getenv("OPENAI_MODEL", "gpt-4o-mini")),
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a marketing analytics consultant. Give 3 concise, "
                        "actionable budget recommendations. Mention revenue, ROI, and CAC."
                    ),
                },
                {"role": "user", "content": str(payload)},
            ],
            temperature=0.35,
        )
        return response.choices[0].message.content or None
    except Exception:
        return None


with st.sidebar:
    st.markdown("### Mixalyzer")
    uploaded = st.file_uploader("Marketing dataset", type=["csv"])
    st.download_button(
        "Download CSV template",
        data=build_csv_template(),
        file_name="marketing_mix_template.csv",
        mime="text/csv",
        use_container_width=True,
    )
    raw_data = read_input_data(uploaded)
    with st.expander("Auto column mapping", expanded=uploaded is not None):
        column_mapping = build_column_mapping_controls(raw_data, uploaded is not None)
    mapped_input = apply_column_mapping(raw_data, column_mapping) if uploaded is not None else raw_data
    readiness = assess_data_readiness(mapped_input)
    st.caption(f"Data readiness: {readiness['score']}/100 · {readiness['status']}")
    model_engine = st.selectbox(
        "Model engine",
        ["Ridge MMM (fast)", "Bayesian MMM (posterior)"],
        index=0,
    )
    confidence_level = st.select_slider(
        "Confidence range",
        options=[0.80, 0.90, 0.95],
        value=0.80,
        format_func=lambda value: f"{int(value * 100)}%",
    )
    regularization = st.slider("Model regularization", 0.1, 5.0, 1.5, 0.1)
    use_openai = st.toggle("OpenAI recommendation narrative", value=False)


try:
    with st.spinner("Mixalyzer is cleaning data and training the MMM engine..."):
        data = prepare_marketing_data(mapped_input)
        model, contribution_df, baseline_scenario = train_model(data, regularization, model_engine)
except Exception as exc:
    st.error(f"Could not train the marketing mix model: {exc}")
    st.stop()

try:
    with st.spinner("Running holdout evaluation and model comparison..."):
        evaluation_results = evaluate_current_model(data, regularization)
        model_comparison_df = compare_models(data, regularization)
except Exception:
    evaluation_results = None
    model_comparison_df = pd.DataFrame()


total_spend = float(data[list(DEFAULT_CHANNELS)].sum().sum())
total_revenue = float(data[TARGET_COL].sum())
model_roi = float(total_revenue / total_spend) if total_spend else 0.0
total_customers = float(data[CUSTOMER_COL].sum()) if CUSTOMER_COL in data.columns else 0.0
cac = float(total_spend / total_customers) if total_customers else None

st.markdown(
    """
    <div class="brand-kicker">Mixalyzer</div>
    <div class="brand-title">Marketing Mix Intelligence</div>
    <div class="brand-subtitle">
      Forecast revenue, compare MMM engines, audit data readiness, and turn budget shifts into executive-ready recommendations.
    </div>
    """,
    unsafe_allow_html=True,
)

kpi_cols = st.columns(6)
top_metrics = [
    ("Revenue", money(total_revenue)),
    ("Marketing spend", money(total_spend)),
    ("Marketing ROI", f"{model_roi:.2f}x"),
    ("CAC", money(cac) if cac is not None else "N/A"),
    ("Model R-squared", f"{model.metrics['r2']:.2f}"),
    ("MAPE", pct(model.metrics["mape"])),
]
for col, (label, value) in zip(kpi_cols, top_metrics):
    with col:
        render_metric_card(label, value)

tabs = st.tabs(
    [
        "Product",
        "Data Setup",
        "Dashboard",
        "Simulation",
        "Optimization",
        "Evaluation",
        "Responsible AI",
        "Model",
    ]
)

with tabs[0]:
    st.markdown(
        """
        <div class="hero">
          <h2>Allocate marketing budget with evidence, not guesswork.</h2>
          <p>
            Mixalyzer helps growth teams identify which channels drive revenue, simulate budget shifts,
            quantify confidence ranges, and reduce CAC while protecting marketing ROI.
          </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    who, why, how = st.columns(3)
    with who:
        st.markdown(
            """
            <div class="feature-card">
              <h4>Who</h4>
              <p>Marketing managers, growth teams, and finance partners managing multi-channel budgets.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with why:
        st.markdown(
            """
            <div class="feature-card">
              <h4>Why</h4>
              <p>Ad platforms report activity, but executives need revenue impact, ROI, and CAC tradeoffs.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with how:
        st.markdown(
            """
            <div class="feature-card">
              <h4>How</h4>
              <p>MMM prediction, adstock carryover, simulation, optimization, and grounded AI recommendations.</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.divider()
    product_cols = st.columns(4)
    product_metrics = [
        ("Business objective", "ROI up"),
        ("Operating KPI", "CAC down"),
        ("Planning output", "Budget mix"),
        ("AI approach", "Prediction + GenAI"),
    ]
    for col, (label, value) in zip(product_cols, product_metrics):
        with col:
            render_metric_card(label, value)

    feature_cols = st.columns(4)
    feature_copy = [
        ("Dashboard", "Channel contribution, spend mix, and revenue trend analysis."),
        ("Simulator", "Budget what-if scenarios with predicted revenue impact."),
        ("Optimizer", "Recommended allocation under a selected spend constraint."),
        ("Governance", "Risk controls for privacy, bias, hallucination, and model reliability."),
    ]
    for col, (title, body) in zip(feature_cols, feature_copy):
        with col:
            st.markdown(
                f"""
                <div class="feature-card">
                  <h4>{title}</h4>
                  <p>{body}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )

with tabs[1]:
    st.subheader("Data readiness and column mapping")
    ready_cols = st.columns(4)
    with ready_cols[0]:
        render_metric_card("Readiness score", f"{readiness['score']}/100")
    with ready_cols[1]:
        render_metric_card("Status", readiness["status"])
    with ready_cols[2]:
        render_metric_card("Rows", f"{len(data):,}")
    with ready_cols[3]:
        render_metric_card("Mapped fields", f"{len([v for v in column_mapping.values() if v])}/{len(column_mapping)}")

    st.markdown(
        f"""
        <div class="feature-card {readiness_class(readiness['status'])}">
          <h4>Data readiness verdict</h4>
          <p>Mixalyzer checked required fields, date quality, history length, numeric quality, CAC support, and spend coverage.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    map_left, map_right = st.columns([1, 1])
    with map_left:
        mapping_rows = [
            {
                "Mixalyzer Field": FIELD_LABELS.get(canonical, canonical),
                "Source Column": source or "Not mapped",
            }
            for canonical, source in column_mapping.items()
        ]
        st.dataframe(pd.DataFrame(mapping_rows), hide_index=True, use_container_width=True)
    with map_right:
        st.dataframe(readiness["checks"], hide_index=True, use_container_width=True)

    st.subheader("Cleaned data preview")
    st.dataframe(data.head(20), hide_index=True, use_container_width=True)

with tabs[2]:
    left, right = st.columns([1.45, 1])
    with left:
        st.subheader("Spend and revenue trend")
        st.altair_chart(line_spend_revenue_chart(data), use_container_width=True)
    with right:
        st.subheader("Channel contribution")
        st.altair_chart(contribution_chart(contribution_df), use_container_width=True)

    chart_a, chart_b = st.columns([1, 1])
    with chart_a:
        st.subheader("ROI by channel")
        st.altair_chart(roi_chart(contribution_df), use_container_width=True)
    with chart_b:
        st.subheader("Channel spend mix")
        st.altair_chart(channel_spend_chart(data), use_container_width=True)

with tabs[3]:
    st.subheader("Budget simulation")
    sliders = {}
    slider_cols = st.columns(3)
    for idx, channel in enumerate(DEFAULT_CHANNELS):
        with slider_cols[idx % 3]:
            sliders[channel] = st.slider(
                CHANNEL_LABELS[channel],
                min_value=-60,
                max_value=80,
                value=0,
                step=5,
                format="%d%%",
            )

    simulation = simulate_with_confidence(model, baseline_scenario, sliders, confidence_level)
    metric_cols = st.columns(4)
    simulation_metrics = [
        ("Current revenue", money(simulation["current_revenue"]), None),
        ("Scenario revenue", money(simulation["scenario_revenue"]), pct(simulation["revenue_delta_pct"])),
        ("Budget change", signed_money(simulation["budget_delta"]), None),
        ("Scenario budget", money(simulation["scenario_budget"]), None),
    ]
    for col, (label, value, delta) in zip(metric_cols, simulation_metrics):
        with col:
            render_metric_card(label, value, delta)

    confidence_summary = pd.DataFrame(
        [
            {"Case": "Conservative", "Revenue Delta": simulation["revenue_delta_low"]},
            {"Case": "Expected", "Revenue Delta": simulation["revenue_delta"]},
            {"Case": "Optimistic", "Revenue Delta": simulation["revenue_delta_high"]},
        ]
    )
    st.altair_chart(confidence_range_chart(confidence_summary), use_container_width=True)

    sim_left, sim_right = st.columns([1.1, 1])
    with sim_left:
        st.altair_chart(
            spend_shift_chart(simulation["details"], "Current Spend", "Scenario Spend"),
            use_container_width=True,
        )
    with sim_right:
        selected_channel = st.selectbox(
            "Diminishing returns channel",
            list(DEFAULT_CHANNELS),
            format_func=lambda value: CHANNEL_LABELS[value],
        )
        curve = build_response_curve(model, baseline_scenario, selected_channel)
        st.altair_chart(response_curve_chart(curve), use_container_width=True)

with tabs[4]:
    current_weekly_budget = sum(float(baseline_scenario[channel]) for channel in DEFAULT_CHANNELS)
    target_budget = st.slider(
        "Next-period marketing budget",
        min_value=float(current_weekly_budget * 0.5),
        max_value=float(current_weekly_budget * 1.5),
        value=float(current_weekly_budget),
        step=500.0,
        format="$%.0f",
    )
    optimization = optimize_with_confidence(model, baseline_scenario, target_budget, confidence_level)

    opt_cols = st.columns(4)
    optimization_metrics = [
        ("Current budget", money(optimization["current_budget"]), None),
        ("Recommended budget", money(optimization["recommended_budget"]), None),
        ("Optimized revenue", money(optimization["optimized_revenue"]), pct(optimization["revenue_delta_pct"])),
        ("Unallocated", money(optimization["unallocated_budget"]), None),
    ]
    for col, (label, value, delta) in zip(opt_cols, optimization_metrics):
        with col:
            render_metric_card(label, value, delta)

    st.caption(
        "Optimized revenue impact range: "
        f"{signed_money(optimization['revenue_delta_low'])} to {signed_money(optimization['revenue_delta_high'])} "
        f"at {int(confidence_level * 100)}% confidence."
    )

    opt_left, opt_right = st.columns([1.2, 1])
    with opt_left:
        st.subheader("Recommended allocation")
        st.altair_chart(
            spend_shift_chart(optimization["allocation"], "Current Spend", "Recommended Spend"),
            use_container_width=True,
        )
        st.dataframe(
            optimization["allocation"][
                ["Channel", "Current Spend", "Recommended Spend", "Spend Shift", "Change %"]
            ],
            hide_index=True,
            use_container_width=True,
            column_config={
                "Current Spend": st.column_config.NumberColumn(format="$%.0f"),
                "Recommended Spend": st.column_config.NumberColumn(format="$%.0f"),
                "Spend Shift": st.column_config.NumberColumn(format="$%.0f"),
                "Change %": st.column_config.NumberColumn(format="%.1f%%"),
            },
        )
    with opt_right:
        st.subheader("AI recommendation panel")
        narrative = (
            maybe_generate_openai_recommendations(contribution_df, optimization, simulation)
            if use_openai
            else None
        )
        deterministic_recommendations = generate_recommendations(contribution_df, optimization, simulation)
        if narrative:
            st.markdown(narrative)
        else:
            for item in deterministic_recommendations:
                st.markdown(f"<div class='recommendation'>{item}</div>", unsafe_allow_html=True)

        st.divider()
        report = build_executive_report(
            data=data,
            contribution=contribution_df,
            optimization=optimization,
            simulation=simulation,
            evaluation=evaluation_results,
            recommendations=deterministic_recommendations,
        )
        st.download_button(
            "Download executive report",
            data=report.encode("utf-8"),
            file_name="marketing_mix_executive_report.md",
            mime="text/markdown",
            use_container_width=True,
        )
        st.download_button(
            "Download allocation CSV",
            data=optimization["allocation"].to_csv(index=False).encode("utf-8"),
            file_name="recommended_budget_allocation.csv",
            mime="text/csv",
            use_container_width=True,
        )

with tabs[5]:
    st.subheader("Model comparison")
    if not model_comparison_df.empty:
        best_model = model_comparison_df.iloc[0]
        cmp_cols = st.columns(4)
        with cmp_cols[0]:
            render_metric_card("Best model", best_model["Model"])
        with cmp_cols[1]:
            render_metric_card("Best MAPE", pct(best_model["MAPE"]))
        with cmp_cols[2]:
            render_metric_card("Selected engine", model.model_kind)
        with cmp_cols[3]:
            render_metric_card("Confidence mode", f"{int(confidence_level * 100)}% interval")
        left_cmp, right_cmp = st.columns([1, 1.25])
        with left_cmp:
            st.altair_chart(model_comparison_chart(model_comparison_df), use_container_width=True)
        with right_cmp:
            st.dataframe(
                model_comparison_df,
                hide_index=True,
                use_container_width=True,
                column_config={
                    "R2": st.column_config.NumberColumn(format="%.2f"),
                    "MAE": st.column_config.NumberColumn(format="$%.0f"),
                    "RMSE": st.column_config.NumberColumn(format="$%.0f"),
                    "MAPE": st.column_config.NumberColumn(format="%.2f%%"),
                },
            )
    else:
        st.info("Upload at least 12 dated observations to compare model candidates.")

    st.divider()
    st.subheader("Train/test evaluation")
    if evaluation_results:
        eval_cols = st.columns(5)
        evaluation_metrics = [
            ("Train rows", f"{evaluation_results['train_rows']}"),
            ("Test rows", f"{evaluation_results['test_rows']}"),
            ("MMX MAPE", pct(evaluation_results["model_metrics"]["mape"])),
            ("Baseline MAPE", pct(evaluation_results["baseline_metrics"]["mape"])),
            ("RMSE lift", pct(evaluation_results["rmse_improvement_pct"])),
        ]
        for col, (label, value) in zip(eval_cols, evaluation_metrics):
            with col:
                render_metric_card(label, value)

        pred_left, pred_right = st.columns([1.35, 1])
        with pred_left:
            st.altair_chart(prediction_chart(evaluation_results["predictions"]), use_container_width=True)
        with pred_right:
            st.altair_chart(evaluation_metric_chart(evaluation_results), use_container_width=True)

        metrics_df = pd.DataFrame(
            [
                {"Metric": "R-squared", "MMX": evaluation_results["model_metrics"]["r2"], "Baseline": evaluation_results["baseline_metrics"]["r2"]},
                {"Metric": "MAE", "MMX": evaluation_results["model_metrics"]["mae"], "Baseline": evaluation_results["baseline_metrics"]["mae"]},
                {"Metric": "RMSE", "MMX": evaluation_results["model_metrics"]["rmse"], "Baseline": evaluation_results["baseline_metrics"]["rmse"]},
                {"Metric": "MAPE", "MMX": evaluation_results["model_metrics"]["mape"], "Baseline": evaluation_results["baseline_metrics"]["mape"]},
            ]
        )
        st.dataframe(
            metrics_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "MMX": st.column_config.NumberColumn(format="%.2f"),
                "Baseline": st.column_config.NumberColumn(format="%.2f"),
            },
        )
    else:
        st.info("Upload at least 12 dated observations to show train/test evaluation.")

with tabs[6]:
    st.subheader("Responsible AI and risk audit")
    risk_df = build_responsible_ai_audit(data, evaluation_results, contribution_df)
    managed = int((risk_df["Status"] == "Managed").sum())
    watch = int((risk_df["Status"] == "Watch").sum())
    risk_cols = st.columns(4)
    risk_metrics = [
        ("Managed controls", managed),
        ("Watch items", watch),
        ("Privacy posture", "Aggregated"),
        ("Recommendation mode", "Human review"),
    ]
    for col, (label, value) in zip(risk_cols, risk_metrics):
        with col:
            render_metric_card(label, value)

    for _, row in risk_df.iterrows():
        css_class = "risk-ok" if row["Status"] == "Managed" else "risk-watch"
        st.markdown(
            f"""
            <div class="feature-card {css_class}">
              <h4>{row['Area']} · {row['Status']}</h4>
              <p><strong>Risk:</strong> {row['Risk']}</p>
              <p><strong>Mitigation:</strong> {row['Mitigation']}</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.dataframe(risk_df, hide_index=True, use_container_width=True)

with tabs[7]:
    model_left, model_right = st.columns([1, 1])
    with model_left:
        st.subheader("Training data")
        st.dataframe(data.tail(12), hide_index=True, use_container_width=True)
        st.subheader("Adstock carryover settings")
        adstock_df = pd.DataFrame(
            [
                {
                    "Channel": CHANNEL_LABELS[channel],
                    "Carryover Decay": DEFAULT_ADSTOCK_DECAYS[channel],
                    "Interpretation": "Higher values mean spend influence lasts longer.",
                }
                for channel in DEFAULT_CHANNELS
            ]
        )
        st.dataframe(
            adstock_df,
            hide_index=True,
            use_container_width=True,
            column_config={"Carryover Decay": st.column_config.NumberColumn(format="%.2f")},
        )
    with model_right:
        st.subheader("Model diagnostics")
        diagnostics = pd.DataFrame(
            [
                {"Metric": "R-squared", "Value": model.metrics["r2"]},
                {"Metric": "MAE", "Value": model.metrics["mae"]},
                {"Metric": "RMSE", "Value": model.metrics["rmse"]},
                {"Metric": "MAPE", "Value": model.metrics["mape"]},
            ]
        )
        st.dataframe(diagnostics, hide_index=True, use_container_width=True)

        coefficients = (
            model.coefficients.rename_axis("Feature")
            .reset_index(name="Coefficient")
            .sort_values("Coefficient", ascending=False)
        )
        st.dataframe(coefficients, hide_index=True, use_container_width=True)
