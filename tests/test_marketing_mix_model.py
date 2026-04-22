import unittest

import numpy as np
import pandas as pd

from marketing_mix_model import (
    CHANNEL_LABELS,
    CUSTOMER_COL,
    DEFAULT_CHANNELS,
    apply_column_mapping,
    apply_adstock,
    build_response_curve,
    assess_data_readiness,
    compare_candidate_models,
    evaluate_model_against_baseline,
    estimate_channel_contribution,
    fit_bayesian_marketing_mix_model,
    fit_marketing_mix_model,
    generate_recommendations,
    generate_sample_marketing_data,
    get_baseline_scenario,
    normalize_marketing_data,
    optimize_budget,
    prepare_marketing_data,
    predict_with_interval,
    simulate_spend_change,
    suggest_column_mapping,
)


class MarketingMixModelTests(unittest.TestCase):
    def setUp(self):
        self.data = generate_sample_marketing_data(weeks=120, seed=7)
        self.model = fit_marketing_mix_model(self.data)
        self.baseline = get_baseline_scenario(self.data)

    def test_sample_data_has_required_mmx_columns(self):
        expected = {"date", "revenue", CUSTOMER_COL, *DEFAULT_CHANNELS}

        self.assertTrue(expected.issubset(self.data.columns))
        self.assertEqual(len(self.data), 120)

    def test_normalize_marketing_data_accepts_common_aliases(self):
        uploaded = pd.DataFrame(
            {
                "Week": ["2026-01-04"],
                "Sales": [100000],
                "Customers": [250],
                "Google Ads Spend": [10000],
                "Facebook Ads": [8000],
                "Instagram": [6000],
                "TV Spend": [12000],
                "Email": [2000],
                "Discounts": [3000],
            }
        )

        normalized = normalize_marketing_data(uploaded)

        self.assertTrue({"date", "revenue", CUSTOMER_COL, *DEFAULT_CHANNELS}.issubset(normalized.columns))

        prepared = prepare_marketing_data(uploaded)
        self.assertEqual(prepared["date"].dt.year.iloc[0], 2026)
        self.assertEqual(float(prepared["google_ads"].iloc[0]), 10000)
        self.assertEqual(float(prepared[CUSTOMER_COL].iloc[0]), 250)

    def test_auto_mapping_and_readiness_score_uploaded_columns(self):
        uploaded = pd.DataFrame(
            {
                "Week Start": ["2026-01-04", "2026-01-11"] * 30,
                "Sales": [100000, 110000] * 30,
                "Google Ads Spend": [10000, 12000] * 30,
                "Facebook Spend": [8000, 8500] * 30,
                "Instagram Budget": [6000, 6300] * 30,
                "TV Cost": [12000, 11000] * 30,
                "Email Spend": [2000, 2200] * 30,
                "Discounts": [3000, 3100] * 30,
                "Customers": [250, 270] * 30,
            }
        )

        mapping = suggest_column_mapping(uploaded)
        mapped = apply_column_mapping(uploaded, mapping)
        readiness = assess_data_readiness(mapped)

        self.assertEqual(mapping["google_ads"], "Google Ads Spend")
        self.assertGreaterEqual(readiness["score"], 65)

    def test_model_fits_demo_data_with_useful_accuracy(self):
        self.assertGreater(self.model.metrics["r2"], 0.75)
        self.assertLess(self.model.metrics["mape"], 5)

    def test_adstock_carries_spend_forward(self):
        adstocked = apply_adstock([100, 0, 0], decay=0.5)

        self.assertEqual(adstocked.tolist(), [100.0, 50.0, 25.0])

    def test_train_test_evaluation_compares_against_baseline(self):
        evaluation = evaluate_model_against_baseline(self.data)

        self.assertEqual(evaluation["train_rows"] + evaluation["test_rows"], len(self.data))
        self.assertIn("model_metrics", evaluation)
        self.assertIn("baseline_metrics", evaluation)
        self.assertEqual(len(evaluation["predictions"]), evaluation["test_rows"])

    def test_model_comparison_includes_bayesian_candidate(self):
        comparison = compare_candidate_models(self.data)

        self.assertIn("Bayesian MMM", comparison["Model"].tolist())
        self.assertEqual(comparison["Rank"].tolist(), sorted(comparison["Rank"].tolist()))

    def test_bayesian_model_returns_prediction_intervals(self):
        bayesian_model = fit_bayesian_marketing_mix_model(self.data)
        interval = predict_with_interval(bayesian_model, self.data.tail(3))

        self.assertEqual(len(interval), 3)
        self.assertTrue((interval["Lower"] <= interval["Prediction"]).all())
        self.assertTrue((interval["Prediction"] <= interval["Upper"]).all())

    def test_channel_contribution_returns_roi_for_each_channel(self):
        contribution = estimate_channel_contribution(self.model, self.data)

        self.assertEqual(set(contribution["channel"]), set(DEFAULT_CHANNELS))
        self.assertTrue(np.isfinite(contribution["ROI"]).all())
        self.assertTrue((contribution["Estimated Contribution"] > 0).all())

    def test_simulation_reports_budget_and_revenue_delta(self):
        changes = {channel: 0 for channel in DEFAULT_CHANNELS}
        changes["instagram_ads"] = 15
        simulation = simulate_spend_change(self.model, self.baseline, changes)

        self.assertGreater(simulation["scenario_budget"], simulation["current_budget"])
        self.assertIn("revenue_delta_pct", simulation)
        self.assertIn("revenue_delta_low", simulation)
        self.assertIn("revenue_delta_high", simulation)
        self.assertEqual(len(simulation["details"]), len(DEFAULT_CHANNELS))

    def test_response_curve_is_monotonic_for_strong_channel(self):
        curve = build_response_curve(self.model, self.baseline, "email_marketing")

        self.assertEqual(len(curve), 21)
        self.assertGreaterEqual(
            curve["Predicted Revenue"].iloc[-1],
            curve["Predicted Revenue"].iloc[0],
        )

    def test_optimizer_returns_recommended_allocation(self):
        optimization = optimize_budget(self.model, self.baseline)

        self.assertEqual(set(optimization["allocation"]["Channel"]), set(CHANNEL_LABELS.values()))
        self.assertGreaterEqual(optimization["optimized_revenue"], optimization["current_revenue"])
        self.assertLess(abs(optimization["recommended_budget"] - optimization["current_budget"]), 1_000)

    def test_recommendations_are_business_actions(self):
        contribution = estimate_channel_contribution(self.model, self.data)
        optimization = optimize_budget(self.model, self.baseline)
        simulation = simulate_spend_change(self.model, self.baseline, {"google_ads": 10})

        recommendations = generate_recommendations(contribution, optimization, simulation)

        self.assertGreaterEqual(len(recommendations), 3)
        self.assertTrue(any("ROI" in item for item in recommendations))


if __name__ == "__main__":
    unittest.main()
