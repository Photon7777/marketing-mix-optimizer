from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, Mapping, Sequence

import numpy as np
import pandas as pd


DATE_COL = "date"
TARGET_COL = "revenue"
CUSTOMER_COL = "new_customers"

CHANNEL_LABELS: Dict[str, str] = {
    "google_ads": "Google Ads",
    "meta_ads": "Meta Ads",
    "instagram_ads": "Instagram Ads",
    "tv_ads": "TV Ads",
    "email_marketing": "Email Marketing",
    "promotions": "Promotions",
}

DEFAULT_CHANNELS = tuple(CHANNEL_LABELS.keys())

DEFAULT_ADSTOCK_DECAYS: Dict[str, float] = {
    "google_ads": 0.35,
    "meta_ads": 0.40,
    "instagram_ads": 0.38,
    "tv_ads": 0.65,
    "email_marketing": 0.20,
    "promotions": 0.25,
}

_COLUMN_ALIASES = {
    "week": DATE_COL,
    "date": DATE_COL,
    "period": DATE_COL,
    "revenue": TARGET_COL,
    "sales": TARGET_COL,
    "total_revenue": TARGET_COL,
    "customers": CUSTOMER_COL,
    "new_customers": CUSTOMER_COL,
    "customer_acquisitions": CUSTOMER_COL,
    "acquisitions": CUSTOMER_COL,
    "conversions": CUSTOMER_COL,
    "google": "google_ads",
    "google_ads": "google_ads",
    "google_ad_spend": "google_ads",
    "google_ads_spend": "google_ads",
    "search": "google_ads",
    "paid_search": "google_ads",
    "meta": "meta_ads",
    "meta_ads": "meta_ads",
    "facebook": "meta_ads",
    "facebook_ads": "meta_ads",
    "facebook_ads_spend": "meta_ads",
    "instagram": "instagram_ads",
    "instagram_ads": "instagram_ads",
    "instagram_ads_spend": "instagram_ads",
    "tv": "tv_ads",
    "tv_ads": "tv_ads",
    "tv_spend": "tv_ads",
    "email": "email_marketing",
    "email_marketing": "email_marketing",
    "email_spend": "email_marketing",
    "promotions": "promotions",
    "promotion": "promotions",
    "promo": "promotions",
    "discounts": "promotions",
    "discount_spend": "promotions",
}


@dataclass(frozen=True)
class MarketingMixModel:
    channel_cols: tuple[str, ...]
    feature_columns: tuple[str, ...]
    feature_means: pd.Series
    feature_stds: pd.Series
    coefficients: pd.Series
    intercept: float
    date_origin: pd.Timestamp
    date_span_days: int
    metrics: Dict[str, float]
    model_kind: str = "Ridge MMM"
    posterior_covariance: np.ndarray | None = None
    residual_std: float | None = None

    def predict(self, data: pd.DataFrame | Mapping[str, object]) -> np.ndarray:
        frame = pd.DataFrame([data]) if isinstance(data, Mapping) else data.copy()
        frame = _coerce_model_input(frame, self.channel_cols, require_revenue=False)
        features = build_feature_frame(
            frame,
            self.channel_cols,
            date_origin=self.date_origin,
            date_span_days=self.date_span_days,
        )
        features = features.reindex(columns=self.feature_columns, fill_value=0.0)
        standardized = (features - self.feature_means) / self.feature_stds
        predictions = self.intercept + np.einsum(
            "ij,j->i",
            standardized.to_numpy(dtype=float),
            self.coefficients.to_numpy(dtype=float),
        )
        return np.maximum(predictions.astype(float), 0.0)


FIELD_LABELS: Dict[str, str] = {
    DATE_COL: "Date",
    TARGET_COL: "Revenue",
    CUSTOMER_COL: "New Customers",
    **CHANNEL_LABELS,
}

REQUIRED_MODEL_COLUMNS = (DATE_COL, TARGET_COL, *DEFAULT_CHANNELS)
OPTIONAL_MODEL_COLUMNS = (CUSTOMER_COL,)


def normalize_marketing_data(data: pd.DataFrame) -> pd.DataFrame:
    """Return a copy with common marketing-mix column names normalized."""
    rename_map: Dict[str, str] = {}
    seen_targets = set()

    for column in data.columns:
        normalized = re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")
        target = _COLUMN_ALIASES.get(normalized)
        if target and target not in seen_targets:
            rename_map[column] = target
            seen_targets.add(target)

    frame = data.rename(columns=rename_map).copy()
    return frame.loc[:, ~frame.columns.duplicated(keep="last")]


def suggest_column_mapping(data: pd.DataFrame) -> Dict[str, str]:
    """Suggest source columns for the canonical Mixalyzer schema."""
    mapping: Dict[str, str] = {}
    used: set[str] = set()
    normalized_columns = {column: _normalize_column_name(column) for column in data.columns}

    for canonical in (*REQUIRED_MODEL_COLUMNS, *OPTIONAL_MODEL_COLUMNS):
        best_column = ""
        best_score = 0
        for column, normalized in normalized_columns.items():
            if column in used:
                continue
            score = _column_match_score(canonical, normalized)
            if score > best_score:
                best_column = column
                best_score = score
        if best_score >= 3:
            mapping[canonical] = best_column
            used.add(best_column)
        else:
            mapping[canonical] = ""

    return mapping


def apply_column_mapping(data: pd.DataFrame, mapping: Mapping[str, str]) -> pd.DataFrame:
    """Copy selected source columns into the canonical schema used by the model."""
    frame = data.copy()
    copied_sources = []
    for canonical, source in mapping.items():
        if source and source in data.columns:
            frame[canonical] = data[source]
            if source != canonical:
                copied_sources.append(source)
    frame = frame.drop(columns=copied_sources, errors="ignore")
    return frame


def assess_data_readiness(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
) -> Dict[str, object]:
    """Score whether a dataset is ready for MMM modeling and explain blockers."""
    frame = normalize_marketing_data(data)
    checks = []
    score = 100

    def add_check(area: str, status: str, detail: str, penalty: int = 0) -> None:
        nonlocal score
        score -= penalty
        checks.append({"Area": area, "Status": status, "Detail": detail})

    missing = [column for column in (DATE_COL, TARGET_COL, *channel_cols) if column not in frame.columns]
    if missing:
        add_check("Required columns", "Needs attention", f"Missing: {', '.join(missing)}", 12 * len(missing))
    else:
        add_check("Required columns", "Ready", "Date, revenue, and channel spend fields are mapped.")

    if DATE_COL in frame.columns:
        parsed_dates = pd.to_datetime(frame[DATE_COL], errors="coerce")
        invalid_dates = int(parsed_dates.isna().sum())
        duplicate_dates = int(parsed_dates.duplicated().sum())
        if invalid_dates:
            add_check("Date quality", "Needs attention", f"{invalid_dates} invalid date value(s).", 15)
        elif duplicate_dates:
            add_check("Date quality", "Watch", f"{duplicate_dates} duplicate date value(s).", 5)
        else:
            add_check("Date quality", "Ready", "Dates parse cleanly with no duplicates.")
    else:
        add_check("Date quality", "Needs attention", "No mapped date column.", 15)

    row_count = len(frame)
    if row_count >= 104:
        add_check("History length", "Ready", f"{row_count} observations; enough for seasonality and holdout testing.")
    elif row_count >= 52:
        add_check("History length", "Watch", f"{row_count} observations; usable, but two years is stronger.", 8)
    else:
        add_check("History length", "Needs attention", f"{row_count} observations; MMM works better with 52+ weekly rows.", 15)

    numeric_columns = [column for column in (TARGET_COL, *channel_cols) if column in frame.columns]
    numeric_issues = []
    for column in numeric_columns:
        numeric = pd.to_numeric(frame[column], errors="coerce")
        bad_values = int(numeric.isna().sum())
        negative_values = int((numeric < 0).sum())
        if bad_values or negative_values:
            numeric_issues.append(f"{column}: {bad_values} non-numeric, {negative_values} negative")
    if numeric_issues:
        add_check("Numeric quality", "Needs attention", "; ".join(numeric_issues), 10)
    else:
        add_check("Numeric quality", "Ready", "Revenue and spend fields are numeric and non-negative.")

    if CUSTOMER_COL in frame.columns and pd.to_numeric(frame[CUSTOMER_COL], errors="coerce").fillna(0).sum() > 0:
        add_check("CAC support", "Ready", "Customer/conversion counts are available for CAC.")
    else:
        add_check("CAC support", "Watch", "Customer/conversion counts are optional but improve CAC reporting.", 5)

    mapped_channels = [channel for channel in channel_cols if channel in frame.columns]
    if mapped_channels:
        spend = frame[mapped_channels].apply(pd.to_numeric, errors="coerce").fillna(0)
        zero_channels = [channel for channel in mapped_channels if float(spend[channel].sum()) <= 0]
        if zero_channels:
            add_check("Spend coverage", "Watch", f"No spend detected for: {', '.join(zero_channels)}", 5)
        else:
            add_check("Spend coverage", "Ready", "All mapped channels contain spend.")

    readiness_score = int(min(max(score, 0), 100))
    if readiness_score >= 85:
        status = "Ready"
    elif readiness_score >= 65:
        status = "Usable with caveats"
    else:
        status = "Needs cleanup"

    return {
        "score": readiness_score,
        "status": status,
        "checks": pd.DataFrame(checks),
        "mapping_required": missing,
    }


def prepare_marketing_data(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
    require_revenue: bool = True,
) -> pd.DataFrame:
    """Normalize columns and coerce date, spend, and revenue fields for modeling/UI use."""
    return _coerce_model_input(data, channel_cols, require_revenue=require_revenue)


def generate_sample_marketing_data(weeks: int = 156, seed: int = 42) -> pd.DataFrame:
    """Create a realistic demo dataset with seasonality and saturated channel effects."""
    rng = np.random.default_rng(seed)
    dates = pd.date_range(end=pd.Timestamp("2026-03-29"), periods=weeks, freq="W-SUN")
    t = np.arange(weeks)

    holiday = np.where(pd.Series(dates).dt.month.isin([11, 12]), 1.0, 0.0)
    seasonal = np.sin(2 * np.pi * t / 52)
    semiannual = np.cos(2 * np.pi * t / 26)
    trend = np.linspace(0, 1, weeks)

    google = 22_000 + 4_500 * seasonal + 4_000 * holiday + rng.normal(0, 2_100, weeks)
    meta = 17_000 + 2_800 * semiannual + 2_500 * holiday + rng.normal(0, 1_900, weeks)
    instagram = 12_500 + 2_600 * np.sin(2 * np.pi * (t + 8) / 52) + rng.normal(0, 1_600, weeks)
    tv = 28_000 + 12_000 * ((t % 13) < 4).astype(float) + rng.normal(0, 3_500, weeks)
    email = 5_800 + 1_200 * holiday + rng.normal(0, 650, weeks)
    promotions = 7_500 + 7_000 * holiday + 3_500 * ((t % 17) < 3).astype(float) + rng.normal(0, 1_200, weeks)

    channel_values = {
        "google_ads": google,
        "meta_ads": meta,
        "instagram_ads": instagram,
        "tv_ads": tv,
        "email_marketing": email,
        "promotions": promotions,
    }
    for channel, values in channel_values.items():
        channel_values[channel] = np.maximum(values, 500 if channel == "email_marketing" else 2_000)

    baseline = 138_000 + 58_000 * trend + 15_000 * seasonal + 22_000 * holiday
    revenue = (
        baseline
        + _saturated_effect(channel_values["google_ads"], scale=30_000, ceiling=112_000)
        + _saturated_effect(channel_values["meta_ads"], scale=24_000, ceiling=70_000)
        + _saturated_effect(channel_values["instagram_ads"], scale=17_000, ceiling=64_000)
        + _saturated_effect(channel_values["tv_ads"], scale=52_000, ceiling=86_000)
        + _saturated_effect(channel_values["email_marketing"], scale=7_500, ceiling=34_000)
        + _saturated_effect(channel_values["promotions"], scale=15_000, ceiling=58_000)
        + rng.normal(0, 9_000, weeks)
    )

    new_customers = np.maximum(
        450
        + revenue / 390
        + 0.0018 * google
        + 0.0022 * instagram
        + 0.0014 * meta
        + rng.normal(0, 55, weeks),
        100,
    )

    frame = pd.DataFrame(
        {
            DATE_COL: dates,
            **channel_values,
            TARGET_COL: revenue,
            CUSTOMER_COL: new_customers.round(0),
        }
    )
    return frame.round(2)


def fit_marketing_mix_model(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
    regularization: float = 1.5,
) -> MarketingMixModel:
    """Fit a ridge regression model on saturated spend and time features."""
    frame = _coerce_model_input(data, channel_cols, require_revenue=True)
    frame = frame.sort_values(DATE_COL).reset_index(drop=True)

    dates = pd.to_datetime(frame[DATE_COL])
    date_origin = dates.min()
    date_span_days = max(int((dates.max() - date_origin).days), 1)

    features = build_feature_frame(
        frame,
        channel_cols,
        date_origin=date_origin,
        date_span_days=date_span_days,
    )
    target = pd.to_numeric(frame[TARGET_COL], errors="coerce").astype(float)

    feature_means = features.mean()
    feature_stds = features.std(ddof=0).replace(0, 1.0)
    standardized = (features - feature_means) / feature_stds

    x_matrix = standardized.to_numpy(dtype=float)
    y_values = target.to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(standardized)), x_matrix])
    penalty = np.eye(design.shape[1]) * float(regularization)
    penalty[0, 0] = 0.0

    gram = np.einsum("ni,nj->ij", design, design)
    rhs = np.einsum("ni,n->i", design, y_values)

    try:
        weights = np.linalg.solve(gram + penalty, rhs)
    except np.linalg.LinAlgError:
        weights = np.linalg.pinv(gram + penalty) @ rhs

    intercept = float(weights[0])
    coefficients = pd.Series(weights[1:], index=features.columns, dtype=float)

    predictions = intercept + np.einsum("ij,j->i", x_matrix, coefficients.to_numpy(dtype=float))
    metrics = _regression_metrics(y_values, predictions)

    return MarketingMixModel(
        channel_cols=tuple(channel_cols),
        feature_columns=tuple(features.columns),
        feature_means=feature_means,
        feature_stds=feature_stds,
        coefficients=coefficients,
        intercept=intercept,
        date_origin=date_origin,
        date_span_days=date_span_days,
        metrics=metrics,
        residual_std=float(metrics["rmse"]),
    )


def fit_bayesian_marketing_mix_model(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
    prior_variance: float = 25.0,
) -> MarketingMixModel:
    """Fit a lightweight Bayesian MMM using conjugate Bayesian linear regression."""
    frame = _coerce_model_input(data, channel_cols, require_revenue=True)
    frame = frame.sort_values(DATE_COL).reset_index(drop=True)

    dates = pd.to_datetime(frame[DATE_COL])
    date_origin = dates.min()
    date_span_days = max(int((dates.max() - date_origin).days), 1)

    features = build_feature_frame(
        frame,
        channel_cols,
        date_origin=date_origin,
        date_span_days=date_span_days,
    )
    target = pd.to_numeric(frame[TARGET_COL], errors="coerce").astype(float)
    feature_means = features.mean()
    feature_stds = features.std(ddof=0).replace(0, 1.0)
    standardized = (features - feature_means) / feature_stds

    x_matrix = standardized.to_numpy(dtype=float)
    y_values = target.to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(standardized)), x_matrix])

    prior_precision = np.eye(design.shape[1]) / max(float(prior_variance), 1e-6)
    prior_precision[0, 0] = 1e-8

    gram = np.einsum("ni,nj->ij", design, design)
    rhs = np.einsum("ni,n->i", design, y_values)
    system = gram + prior_precision
    try:
        ridge_weights = np.linalg.solve(system, rhs)
    except np.linalg.LinAlgError:
        ridge_weights = np.linalg.lstsq(system, rhs, rcond=None)[0]
    residuals = y_values - np.einsum("ij,j->i", design, ridge_weights)
    residual_std = float(max(np.sqrt(np.mean(residuals**2)), 1.0))

    posterior_precision = prior_precision + np.einsum("ni,nj->ij", design, design) / (residual_std**2)
    posterior_rhs = np.einsum("ni,n->i", design, y_values) / (residual_std**2)
    identity = np.eye(posterior_precision.shape[0])
    try:
        posterior_covariance = np.linalg.solve(posterior_precision, identity)
        posterior_mean = np.linalg.solve(posterior_precision, posterior_rhs)
    except np.linalg.LinAlgError:
        posterior_covariance = np.linalg.lstsq(posterior_precision, identity, rcond=None)[0]
        posterior_mean = np.linalg.lstsq(posterior_precision, posterior_rhs, rcond=None)[0]

    intercept = float(posterior_mean[0])
    coefficients = pd.Series(posterior_mean[1:], index=features.columns, dtype=float)
    predictions = intercept + np.einsum("ij,j->i", x_matrix, coefficients.to_numpy(dtype=float))
    metrics = _regression_metrics(y_values, predictions)

    return MarketingMixModel(
        channel_cols=tuple(channel_cols),
        feature_columns=tuple(features.columns),
        feature_means=feature_means,
        feature_stds=feature_stds,
        coefficients=coefficients,
        intercept=intercept,
        date_origin=date_origin,
        date_span_days=date_span_days,
        metrics=metrics,
        model_kind="Bayesian MMM",
        posterior_covariance=posterior_covariance,
        residual_std=residual_std,
    )


def apply_adstock(values: Sequence[float], decay: float) -> np.ndarray:
    """Carry marketing spend forward so delayed impact is represented in the model."""
    decay = min(max(float(decay), 0.0), 0.95)
    adstocked = np.zeros(len(values), dtype=float)
    previous = 0.0

    for idx, value in enumerate(values):
        current = max(float(value), 0.0)
        adstocked[idx] = current + decay * previous
        previous = adstocked[idx]

    return adstocked


def evaluate_model_against_baseline(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
    regularization: float = 1.5,
    test_size: float = 0.20,
) -> Dict[str, object]:
    """Run a time-based train/test evaluation and compare against a simple baseline."""
    frame = _coerce_model_input(data, channel_cols, require_revenue=True)
    frame = frame.sort_values(DATE_COL).reset_index(drop=True)
    if len(frame) < 12:
        raise ValueError("At least 12 rows are needed for train/test evaluation.")

    split_idx = int(len(frame) * (1 - float(test_size)))
    split_idx = min(max(split_idx, 6), len(frame) - 3)
    train_df = frame.iloc[:split_idx].copy()
    test_df = frame.iloc[split_idx:].copy()

    model = fit_marketing_mix_model(train_df, channel_cols=channel_cols, regularization=regularization)
    model_predictions = model.predict(test_df)
    actual = pd.to_numeric(test_df[TARGET_COL], errors="coerce").to_numpy(dtype=float)

    baseline_value = float(pd.to_numeric(train_df[TARGET_COL], errors="coerce").mean())
    baseline_predictions = np.full(len(test_df), baseline_value, dtype=float)

    prediction_df = pd.DataFrame(
        {
            DATE_COL: test_df[DATE_COL].to_numpy(),
            "Actual Revenue": actual,
            "MMX Prediction": model_predictions,
            "Baseline Prediction": baseline_predictions,
            "MMX Error": actual - model_predictions,
            "Baseline Error": actual - baseline_predictions,
        }
    )

    model_metrics = _regression_metrics(actual, model_predictions)
    baseline_metrics = _regression_metrics(actual, baseline_predictions)
    mape_improvement = baseline_metrics["mape"] - model_metrics["mape"]
    rmse_improvement_pct = _safe_pct(
        baseline_metrics["rmse"] - model_metrics["rmse"],
        baseline_metrics["rmse"],
    )

    return {
        "train_rows": len(train_df),
        "test_rows": len(test_df),
        "split_date": pd.to_datetime(test_df[DATE_COL]).min(),
        "model_metrics": model_metrics,
        "baseline_metrics": baseline_metrics,
        "mape_improvement": float(mape_improvement),
        "rmse_improvement_pct": float(rmse_improvement_pct),
        "predictions": prediction_df,
    }


def compare_candidate_models(
    data: pd.DataFrame,
    channel_cols: Sequence[str] = DEFAULT_CHANNELS,
    regularization: float = 1.5,
    test_size: float = 0.20,
) -> pd.DataFrame:
    """Compare multiple forecasting approaches on the same time-based holdout split."""
    frame = _coerce_model_input(data, channel_cols, require_revenue=True)
    frame = frame.sort_values(DATE_COL).reset_index(drop=True)
    if len(frame) < 12:
        raise ValueError("At least 12 rows are needed for model comparison.")

    train_df, test_df = _time_train_test_split(frame, test_size=test_size)
    actual = pd.to_numeric(test_df[TARGET_COL], errors="coerce").to_numpy(dtype=float)
    rows = []

    baseline_prediction = np.full(
        len(test_df),
        float(pd.to_numeric(train_df[TARGET_COL], errors="coerce").mean()),
        dtype=float,
    )
    rows.append(
        _model_comparison_row(
            "Average baseline",
            "Uses historical average revenue only.",
            actual,
            baseline_prediction,
        )
    )

    calendar_prediction = _fit_predict_feature_model(
        train_df,
        test_df,
        feature_builder=lambda df: _build_calendar_feature_frame(df, train_df),
        regularization=regularization,
    )
    rows.append(
        _model_comparison_row(
            "Seasonality baseline",
            "Uses trend, holiday, and yearly seasonality controls.",
            actual,
            calendar_prediction,
        )
    )

    ridge_model = fit_marketing_mix_model(train_df, channel_cols=channel_cols, regularization=regularization)
    rows.append(
        _model_comparison_row(
            "Ridge MMM",
            "Uses spend saturation, adstock carryover, trend, and seasonality.",
            actual,
            ridge_model.predict(test_df),
        )
    )

    bayesian_model = fit_bayesian_marketing_mix_model(train_df, channel_cols=channel_cols)
    rows.append(
        _model_comparison_row(
            "Bayesian MMM",
            "Uses the same MMM features with posterior uncertainty estimates.",
            actual,
            bayesian_model.predict(test_df),
        )
    )

    comparison = pd.DataFrame(rows).sort_values("MAPE", ascending=True).reset_index(drop=True)
    comparison["Rank"] = np.arange(1, len(comparison) + 1)
    return comparison[["Rank", "Model", "Description", "R2", "MAE", "RMSE", "MAPE"]]


def predict_with_interval(
    model: MarketingMixModel,
    data: pd.DataFrame | Mapping[str, object],
    confidence: float = 0.80,
) -> pd.DataFrame:
    """Return mean prediction plus lower/upper confidence bounds."""
    frame = pd.DataFrame([data]) if isinstance(data, Mapping) else data.copy()
    frame = _coerce_model_input(frame, model.channel_cols, require_revenue=False)
    features = build_feature_frame(
        frame,
        model.channel_cols,
        date_origin=model.date_origin,
        date_span_days=model.date_span_days,
    ).reindex(columns=model.feature_columns, fill_value=0.0)
    standardized = (features - model.feature_means) / model.feature_stds
    design = np.column_stack([np.ones(len(standardized)), standardized.to_numpy(dtype=float)])
    weights = np.concatenate([[model.intercept], model.coefficients.to_numpy(dtype=float)])
    mean = np.einsum("ij,j->i", design, weights)

    residual_std = float(model.residual_std or model.metrics.get("rmse", 0.0) or 1.0)
    if model.posterior_covariance is not None:
        parameter_variance = np.einsum("ij,jk,ik->i", design, model.posterior_covariance, design)
        std = np.sqrt(np.maximum(parameter_variance, 0.0) + residual_std**2)
    else:
        std = np.full(len(frame), residual_std, dtype=float)

    z_score = _z_score_for_confidence(confidence)
    lower = np.maximum(mean - z_score * std, 0.0)
    upper = np.maximum(mean + z_score * std, 0.0)
    return pd.DataFrame({"Prediction": np.maximum(mean, 0.0), "Lower": lower, "Upper": upper})


def build_feature_frame(
    data: pd.DataFrame,
    channel_cols: Sequence[str],
    date_origin: pd.Timestamp,
    date_span_days: int,
) -> pd.DataFrame:
    frame = normalize_marketing_data(data)
    dates = pd.to_datetime(frame[DATE_COL])
    week = dates.dt.isocalendar().week.astype("int64").astype(float)
    days_since_origin = (dates - pd.Timestamp(date_origin)).dt.days.astype(float)

    features = pd.DataFrame(index=frame.index)
    features["trend"] = days_since_origin / max(int(date_span_days), 1)
    features["season_sin"] = np.sin(2 * np.pi * week / 52)
    features["season_cos"] = np.cos(2 * np.pi * week / 52)
    features["holiday_q4"] = dates.dt.month.isin([11, 12]).astype("float64")

    for channel in channel_cols:
        spend = pd.to_numeric(frame[channel], errors="coerce").fillna(0).clip(lower=0)
        features[f"log_{channel}"] = np.log1p(spend)
        decay = DEFAULT_ADSTOCK_DECAYS.get(channel, 0.35)
        adstocked = apply_adstock(spend.to_numpy(dtype=float), decay=decay)
        features[f"adstock_{channel}"] = np.log1p(adstocked)

    return features.astype(float)


def estimate_channel_contribution(
    model: MarketingMixModel,
    data: pd.DataFrame,
) -> pd.DataFrame:
    frame = _coerce_model_input(data, model.channel_cols, require_revenue=False)
    actual_prediction = model.predict(frame)

    rows = []
    total_contribution = 0.0
    for channel in model.channel_cols:
        counterfactual = frame.copy()
        counterfactual[channel] = 0.0
        contribution = float((actual_prediction - model.predict(counterfactual)).sum())
        spend = float(pd.to_numeric(frame[channel], errors="coerce").fillna(0).sum())
        total_contribution += contribution
        rows.append(
            {
                "channel": channel,
                "Channel": CHANNEL_LABELS.get(channel, channel.replace("_", " ").title()),
                "Spend": spend,
                "Estimated Contribution": contribution,
                "ROI": contribution / spend if spend else 0.0,
            }
        )

    contribution_df = pd.DataFrame(rows)
    contribution_df["Contribution Share"] = (
        contribution_df["Estimated Contribution"] / total_contribution
        if total_contribution
        else 0.0
    )
    return contribution_df.sort_values("ROI", ascending=False).reset_index(drop=True)


def get_baseline_scenario(data: pd.DataFrame, recent_weeks: int = 4) -> Dict[str, object]:
    frame = _coerce_model_input(data, DEFAULT_CHANNELS, require_revenue=False)
    frame = frame.sort_values(DATE_COL).tail(max(int(recent_weeks), 1))
    scenario: Dict[str, object] = {
        DATE_COL: pd.to_datetime(frame[DATE_COL]).max() + pd.Timedelta(days=7)
    }
    for channel in DEFAULT_CHANNELS:
        scenario[channel] = float(pd.to_numeric(frame[channel], errors="coerce").fillna(0).mean())
    return scenario


def simulate_spend_change(
    model: MarketingMixModel,
    baseline: Mapping[str, object],
    changes_pct: Mapping[str, float],
    confidence: float = 0.80,
) -> Dict[str, object]:
    current = _scenario_to_frame(baseline, model.channel_cols)
    scenario = current.copy()

    rows = []
    for channel in model.channel_cols:
        current_value = float(current.at[0, channel])
        pct_change = float(changes_pct.get(channel, 0.0))
        new_value = max(current_value * (1 + pct_change / 100), 0.0)
        scenario.at[0, channel] = new_value
        rows.append(
            {
                "channel": channel,
                "Channel": CHANNEL_LABELS.get(channel, channel),
                "Current Spend": current_value,
                "Scenario Spend": new_value,
                "Change %": pct_change,
                "Spend Shift": new_value - current_value,
            }
        )

    current_revenue = float(model.predict(current)[0])
    scenario_revenue = float(model.predict(scenario)[0])
    current_interval = predict_with_interval(model, current, confidence=confidence).iloc[0]
    scenario_interval = predict_with_interval(model, scenario, confidence=confidence).iloc[0]
    current_budget = _sum_budget(current.iloc[0], model.channel_cols)
    scenario_budget = _sum_budget(scenario.iloc[0], model.channel_cols)

    return {
        "current_revenue": current_revenue,
        "scenario_revenue": scenario_revenue,
        "revenue_delta": scenario_revenue - current_revenue,
        "revenue_delta_pct": _safe_pct(scenario_revenue - current_revenue, current_revenue),
        "current_revenue_low": float(current_interval["Lower"]),
        "current_revenue_high": float(current_interval["Upper"]),
        "scenario_revenue_low": float(scenario_interval["Lower"]),
        "scenario_revenue_high": float(scenario_interval["Upper"]),
        "revenue_delta_low": float(scenario_interval["Lower"] - current_interval["Upper"]),
        "revenue_delta_high": float(scenario_interval["Upper"] - current_interval["Lower"]),
        "current_budget": current_budget,
        "scenario_budget": scenario_budget,
        "budget_delta": scenario_budget - current_budget,
        "details": pd.DataFrame(rows),
    }


def build_response_curve(
    model: MarketingMixModel,
    baseline: Mapping[str, object],
    channel: str,
    multipliers: Iterable[float] | None = None,
) -> pd.DataFrame:
    if channel not in model.channel_cols:
        raise ValueError(f"Unknown channel: {channel}")

    multipliers = list(multipliers or np.linspace(0, 2.0, 21))
    base_frame = _scenario_to_frame(baseline, model.channel_cols)
    base_spend = float(base_frame.at[0, channel])
    current_revenue = float(model.predict(base_frame)[0])

    rows = []
    for multiplier in multipliers:
        scenario = base_frame.copy()
        scenario.at[0, channel] = max(base_spend * float(multiplier), 0.0)
        predicted_revenue = float(model.predict(scenario)[0])
        rows.append(
            {
                "Spend Multiplier": float(multiplier),
                "Spend": float(scenario.at[0, channel]),
                "Predicted Revenue": predicted_revenue,
                "Incremental Revenue": predicted_revenue - current_revenue,
            }
        )

    return pd.DataFrame(rows)


def optimize_budget(
    model: MarketingMixModel,
    baseline: Mapping[str, object],
    total_budget: float | None = None,
    min_multiplier: float = 0.25,
    max_multiplier: float = 2.0,
    step_count: int = 90,
    confidence: float = 0.80,
) -> Dict[str, object]:
    base_frame = _scenario_to_frame(baseline, model.channel_cols)
    current_values = np.array([float(base_frame.at[0, ch]) for ch in model.channel_cols])
    current_budget = float(current_values.sum())
    budget = float(total_budget if total_budget is not None else current_budget)
    budget = max(budget, 0.0)

    lower = current_values * max(float(min_multiplier), 0.0)
    upper = np.maximum(current_values * max(float(max_multiplier), 1.0), budget / len(current_values))

    if lower.sum() > budget and lower.sum() > 0:
        allocation = lower * (budget / lower.sum())
        remaining = 0.0
    else:
        allocation = lower.copy()
        remaining = budget - float(allocation.sum())

    def predict_for(values: np.ndarray) -> float:
        scenario = base_frame.copy()
        for idx, channel_name in enumerate(model.channel_cols):
            scenario.at[0, channel_name] = float(values[idx])
        return float(model.predict(scenario)[0])

    current_alloc_prediction = predict_for(allocation)
    step = max(budget / max(int(step_count), 1), 1.0)
    iterations = 0

    while remaining > 0.01 and iterations < max(int(step_count) * 4, 1):
        iterations += 1
        best_idx = None
        best_gain_per_dollar = -np.inf
        best_amount = 0.0
        best_prediction = current_alloc_prediction

        for idx in range(len(model.channel_cols)):
            capacity = float(upper[idx] - allocation[idx])
            if capacity <= 0:
                continue
            amount = min(step, remaining, capacity)
            candidate = allocation.copy()
            candidate[idx] += amount
            candidate_prediction = predict_for(candidate)
            gain_per_dollar = (candidate_prediction - current_alloc_prediction) / amount
            if gain_per_dollar > best_gain_per_dollar:
                best_gain_per_dollar = gain_per_dollar
                best_idx = idx
                best_amount = amount
                best_prediction = candidate_prediction

        if best_idx is None or best_gain_per_dollar <= 0:
            break

        allocation[best_idx] += best_amount
        remaining -= best_amount
        current_alloc_prediction = best_prediction

    optimized_prediction = predict_for(allocation)
    baseline_prediction = predict_for(current_values)
    baseline_scenario = base_frame.copy()
    optimized_scenario = base_frame.copy()
    for idx, channel_name in enumerate(model.channel_cols):
        baseline_scenario.at[0, channel_name] = float(current_values[idx])
        optimized_scenario.at[0, channel_name] = float(allocation[idx])
    baseline_interval = predict_with_interval(model, baseline_scenario, confidence=confidence).iloc[0]
    optimized_interval = predict_with_interval(model, optimized_scenario, confidence=confidence).iloc[0]

    rows = []
    for idx, channel in enumerate(model.channel_cols):
        current_spend = float(current_values[idx])
        recommended = float(allocation[idx])
        rows.append(
            {
                "channel": channel,
                "Channel": CHANNEL_LABELS.get(channel, channel),
                "Current Spend": current_spend,
                "Recommended Spend": recommended,
                "Spend Shift": recommended - current_spend,
                "Change %": _safe_pct(recommended - current_spend, current_spend),
            }
        )

    allocation_df = pd.DataFrame(rows).sort_values("Recommended Spend", ascending=False)
    return {
        "current_budget": current_budget,
        "recommended_budget": float(allocation.sum()),
        "unallocated_budget": max(float(remaining), 0.0),
        "current_revenue": baseline_prediction,
        "optimized_revenue": optimized_prediction,
        "revenue_delta": optimized_prediction - baseline_prediction,
        "revenue_delta_pct": _safe_pct(optimized_prediction - baseline_prediction, baseline_prediction),
        "revenue_delta_low": float(optimized_interval["Lower"] - baseline_interval["Upper"]),
        "revenue_delta_high": float(optimized_interval["Upper"] - baseline_interval["Lower"]),
        "allocation": allocation_df.reset_index(drop=True),
    }


def generate_recommendations(
    contribution_df: pd.DataFrame,
    optimization: Mapping[str, object],
    simulation: Mapping[str, object] | None = None,
) -> list[str]:
    recommendations: list[str] = []
    if contribution_df.empty:
        return ["Upload more complete spend and revenue data before changing budget allocation."]

    best_roi = contribution_df.sort_values("ROI", ascending=False).iloc[0]
    weakest_roi = contribution_df.sort_values("ROI", ascending=True).iloc[0]
    allocation = optimization.get("allocation")

    if isinstance(allocation, pd.DataFrame) and not allocation.empty:
        increase = allocation.sort_values("Spend Shift", ascending=False).iloc[0]
        decrease = allocation.sort_values("Spend Shift", ascending=True).iloc[0]
        if float(increase["Spend Shift"]) > 0 and float(decrease["Spend Shift"]) < 0:
            recommendations.append(
                "Shift budget from "
                f"{decrease['Channel']} to {increase['Channel']}; the optimizer estimates "
                f"{optimization.get('revenue_delta_pct', 0.0):.1f}% revenue lift at this budget level."
            )
        elif float(optimization.get("revenue_delta", 0.0)) > 0:
            recommendations.append(
                "Keep the budget level steady but rebalance toward the channels with stronger marginal response."
            )

    recommendations.append(
        f"Protect {best_roi['Channel']} because its estimated ROI is "
        f"{float(best_roi['ROI']):.2f} revenue dollars per spend dollar."
    )

    if float(weakest_roi["ROI"]) < float(contribution_df["ROI"].median()):
        recommendations.append(
            f"Audit {weakest_roi['Channel']} creative, targeting, or saturation before adding more spend."
        )

    if simulation:
        delta = float(simulation.get("revenue_delta_pct", 0.0))
        direction = "increase" if delta >= 0 else "decrease"
        recommendations.append(
            f"The active simulation would {direction} predicted revenue by {abs(delta):.1f}%."
        )

    recommendations.append(
        "Track revenue lift, marketing ROI, and CAC together so the recommendation moves business KPIs, not only model scores."
    )
    return recommendations[:5]


def _coerce_model_input(
    data: pd.DataFrame,
    channel_cols: Sequence[str],
    require_revenue: bool,
) -> pd.DataFrame:
    frame = normalize_marketing_data(data)
    required = [DATE_COL, *channel_cols]
    if require_revenue:
        required.append(TARGET_COL)

    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise ValueError(f"Missing required column(s): {', '.join(missing)}")

    frame = frame.copy()
    frame[DATE_COL] = pd.to_datetime(frame[DATE_COL], errors="coerce")
    if frame[DATE_COL].isna().any():
        raise ValueError("Date column contains invalid dates.")

    for channel in channel_cols:
        frame[channel] = pd.to_numeric(frame[channel], errors="coerce").fillna(0).clip(lower=0)

    if require_revenue:
        frame[TARGET_COL] = pd.to_numeric(frame[TARGET_COL], errors="coerce")
        if frame[TARGET_COL].isna().any():
            raise ValueError("Revenue column contains non-numeric values.")

    if CUSTOMER_COL in frame.columns:
        frame[CUSTOMER_COL] = pd.to_numeric(frame[CUSTOMER_COL], errors="coerce").fillna(0).clip(lower=0)

    return frame


def _normalize_column_name(column: object) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(column).strip().lower()).strip("_")


def _column_match_score(canonical: str, normalized: str) -> int:
    alias_target = _COLUMN_ALIASES.get(normalized)
    if normalized == canonical or alias_target == canonical:
        return 10

    token_sets = {
        DATE_COL: {"date", "week", "period", "month", "day"},
        TARGET_COL: {"revenue", "sales", "income", "turnover"},
        CUSTOMER_COL: {"customer", "customers", "conversion", "conversions", "acquisition", "acquisitions"},
        "google_ads": {"google", "search", "sem", "paid_search", "google_ads"},
        "meta_ads": {"meta", "facebook", "fb", "paid_social"},
        "instagram_ads": {"instagram", "ig"},
        "tv_ads": {"tv", "television", "linear_tv"},
        "email_marketing": {"email", "newsletter", "crm"},
        "promotions": {"promo", "promotion", "promotions", "discount", "discounts"},
    }
    spend_terms = {"spend", "cost", "budget", "investment"}
    tokens = set(normalized.split("_"))
    score = 0
    for term in token_sets.get(canonical, set()):
        term_tokens = set(term.split("_"))
        if term == normalized or term in normalized:
            score += 4
        if term_tokens.issubset(tokens):
            score += 4
    if canonical in DEFAULT_CHANNELS and tokens.intersection(spend_terms):
        score += 2
    return score


def _time_train_test_split(data: pd.DataFrame, test_size: float = 0.20) -> tuple[pd.DataFrame, pd.DataFrame]:
    split_idx = int(len(data) * (1 - float(test_size)))
    split_idx = min(max(split_idx, 6), len(data) - 3)
    return data.iloc[:split_idx].copy(), data.iloc[split_idx:].copy()


def _build_calendar_feature_frame(data: pd.DataFrame, train_df: pd.DataFrame) -> pd.DataFrame:
    dates = pd.to_datetime(data[DATE_COL])
    train_dates = pd.to_datetime(train_df[DATE_COL])
    origin = train_dates.min()
    span = max(int((train_dates.max() - origin).days), 1)
    week = dates.dt.isocalendar().week.astype("int64").astype(float)
    days_since_origin = (dates - origin).dt.days.astype(float)
    return pd.DataFrame(
        {
            "trend": days_since_origin / span,
            "season_sin": np.sin(2 * np.pi * week / 52),
            "season_cos": np.cos(2 * np.pi * week / 52),
            "holiday_q4": dates.dt.month.isin([11, 12]).astype("float64"),
        },
        index=data.index,
    )


def _fit_predict_feature_model(
    train_df: pd.DataFrame,
    test_df: pd.DataFrame,
    feature_builder,
    regularization: float,
) -> np.ndarray:
    x_train = feature_builder(train_df).astype(float)
    x_test = feature_builder(test_df).astype(float).reindex(columns=x_train.columns, fill_value=0.0)
    means = x_train.mean()
    stds = x_train.std(ddof=0).replace(0, 1.0)
    x_train_std = ((x_train - means) / stds).to_numpy(dtype=float)
    x_test_std = ((x_test - means) / stds).to_numpy(dtype=float)
    y_train = pd.to_numeric(train_df[TARGET_COL], errors="coerce").to_numpy(dtype=float)
    design = np.column_stack([np.ones(len(x_train_std)), x_train_std])
    penalty = np.eye(design.shape[1]) * float(regularization)
    penalty[0, 0] = 0.0
    system = np.einsum("ni,nj->ij", design, design) + penalty
    rhs = np.einsum("ni,n->i", design, y_train)
    try:
        weights = np.linalg.solve(system, rhs)
    except np.linalg.LinAlgError:
        weights = np.linalg.lstsq(system, rhs, rcond=None)[0]
    test_design = np.column_stack([np.ones(len(x_test_std)), x_test_std])
    return np.maximum(np.einsum("ij,j->i", test_design, weights), 0.0)


def _model_comparison_row(
    name: str,
    description: str,
    actual: np.ndarray,
    prediction: np.ndarray,
) -> Dict[str, object]:
    metrics = _regression_metrics(actual, prediction)
    return {
        "Model": name,
        "Description": description,
        "R2": metrics["r2"],
        "MAE": metrics["mae"],
        "RMSE": metrics["rmse"],
        "MAPE": metrics["mape"],
    }


def _z_score_for_confidence(confidence: float) -> float:
    confidence = float(confidence)
    if confidence >= 0.98:
        return 2.33
    if confidence >= 0.95:
        return 1.96
    if confidence >= 0.90:
        return 1.64
    if confidence >= 0.80:
        return 1.28
    return 1.0


def _scenario_to_frame(
    baseline: Mapping[str, object],
    channel_cols: Sequence[str],
) -> pd.DataFrame:
    scenario = dict(baseline)
    scenario.setdefault(DATE_COL, pd.Timestamp.today().normalize())
    frame = pd.DataFrame([scenario])
    return _coerce_model_input(frame, channel_cols, require_revenue=False)


def _regression_metrics(actual: np.ndarray, predicted: np.ndarray) -> Dict[str, float]:
    residuals = actual - predicted
    mae = float(np.mean(np.abs(residuals)))
    rmse = float(np.sqrt(np.mean(residuals**2)))
    mape = float(np.mean(np.abs(residuals / np.maximum(np.abs(actual), 1.0))) * 100)
    total_variance = float(np.sum((actual - actual.mean()) ** 2))
    residual_variance = float(np.sum(residuals**2))
    r2 = 1 - residual_variance / total_variance if total_variance else 0.0
    return {"mae": mae, "rmse": rmse, "mape": mape, "r2": float(r2)}


def _saturated_effect(spend: np.ndarray, scale: float, ceiling: float) -> np.ndarray:
    return ceiling * (1 - np.exp(-np.maximum(spend, 0) / scale))


def _sum_budget(row: pd.Series, channel_cols: Sequence[str]) -> float:
    return float(sum(float(row[channel]) for channel in channel_cols))


def _safe_pct(delta: float, base: float) -> float:
    return float((delta / base) * 100) if base else 0.0
