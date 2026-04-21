import unittest
from datetime import date, timedelta

import pandas as pd

from auth import validate_password, validate_username
from dashboard_metrics import (
    build_dashboard_trend_badges,
    build_fit_outcome_df,
    build_follow_up_timeline_df,
    build_pipeline_funnel_df,
    build_resume_performance_df,
    build_weekly_activity_df,
    build_weekly_momentum_summary,
    filter_tracker_rows,
    pick_best_resume_summary,
)
from db_store import merge_visible_tracker_edits
from tools import recommend_follow_up_action


class TrackerEditMergeTests(unittest.TestCase):
    def test_filtered_edits_preserve_hidden_rows(self):
        original = pd.DataFrame(
            [
                {"_row_id": "r1", "Company": "Acme", "Status": "Applied", "Next Action": "Wait"},
                {"_row_id": "r2", "Company": "Beta", "Status": "Interview", "Next Action": "Prep"},
                {"_row_id": "r3", "Company": "Core", "Status": "Offer", "Next Action": "Review"},
            ]
        )
        edited_visible = pd.DataFrame(
            [{"_row_id": "r1", "Company": "Acme", "Status": "Interview"}]
        )

        merged = merge_visible_tracker_edits(original, edited_visible, {"r1"})

        self.assertEqual(set(merged["_row_id"]), {"r1", "r2", "r3"})
        self.assertEqual(merged.loc[merged["_row_id"] == "r1", "Status"].iloc[0], "Interview")
        self.assertEqual(merged.loc[merged["_row_id"] == "r2", "Company"].iloc[0], "Beta")

    def test_filtered_delete_only_removes_visible_deleted_row(self):
        original = pd.DataFrame(
            [
                {"_row_id": "r1", "Company": "Acme", "Status": "Applied"},
                {"_row_id": "r2", "Company": "Beta", "Status": "Interview"},
            ]
        )
        edited_visible = pd.DataFrame(
            [{"_row_id": "r1", "Company": "Acme", "Status": "Applied", "Delete": True}]
        )

        merged = merge_visible_tracker_edits(original, edited_visible, {"r1"})

        self.assertEqual(merged["_row_id"].tolist(), ["r2"])


class DashboardMetricTests(unittest.TestCase):
    def setUp(self):
        self.df = pd.DataFrame(
            [
                {
                    "Company": "Acme",
                    "Role": "Data Analyst Intern",
                    "Status": "Interested",
                    "Date Applied": "2026-04-07",
                    "Follow-up Date": "2026-04-14",
                    "Fit Score": "82",
                    "Priority": "Apply now",
                    "Resume Used": "Analytics Resume",
                },
                {
                    "Company": "Beta",
                    "Role": "Data Engineer Intern",
                    "Status": "Applied",
                    "Date Applied": "2026-04-08",
                    "Follow-up Date": "2026-04-10",
                    "Fit Score": "74",
                    "Priority": "Strong consider",
                    "Resume Used": "Data Resume",
                },
                {
                    "Company": "Core",
                    "Role": "Analytics Engineer Intern",
                    "Status": "Interview",
                    "Date Applied": "2026-04-01",
                    "Follow-up Date": "2026-04-09",
                    "Fit Score": "91",
                    "Priority": "Apply now",
                    "Resume Used": "Data Resume",
                },
                {
                    "Company": "Delta",
                    "Role": "BI Analyst",
                    "Status": "Offer",
                    "Date Applied": "2026-03-30",
                    "Follow-up Date": "2026-04-15",
                    "Fit Score": "88",
                    "Priority": "Apply now",
                    "Resume Used": "Analytics Resume",
                },
                {
                    "Company": "Echo",
                    "Role": "Product Analyst",
                    "Status": "Rejected",
                    "Date Applied": "2026-04-05",
                    "Follow-up Date": "2026-04-05",
                    "Fit Score": "55",
                    "Priority": "Low priority",
                    "Resume Used": "Analytics Resume",
                },
            ]
        )

    def test_pipeline_funnel_counts_stages_in_order(self):
        funnel = build_pipeline_funnel_df(self.df)

        self.assertEqual(funnel["Stage"].tolist(), ["Interested", "Applied", "OA", "Interview", "Offer"])
        self.assertEqual(funnel["Count"].tolist(), [1, 1, 0, 1, 1])

    def test_weekly_momentum_summary_uses_current_week(self):
        summary = build_weekly_momentum_summary(self.df, today=date(2026, 4, 10))

        self.assertEqual(summary["applications_this_week"], 2)
        self.assertEqual(summary["followups_this_week"], 2)
        self.assertEqual(summary["interviews_active"], 1)
        self.assertEqual(summary["offers_total"], 1)

    def test_weekly_activity_fills_missing_weeks(self):
        activity = build_weekly_activity_df(self.df, today=date(2026, 4, 10), weeks=4)

        self.assertEqual(len(activity), 4)
        self.assertEqual(int(activity["Applications"].sum()), 5)

    def test_fit_outcome_df_builds_stage_depth_and_priority_score(self):
        fit_df = build_fit_outcome_df(self.df)
        interview_row = fit_df.loc[fit_df["Company"] == "Core"].iloc[0]

        self.assertEqual(interview_row["Stage Depth"], 4)
        self.assertEqual(interview_row["Priority Score"], 4)

    def test_resume_performance_summary_counts_interviews_and_offers(self):
        resume_df = build_resume_performance_df(self.df)
        data_resume = resume_df.loc[resume_df["Resume"] == "Data Resume"].iloc[0]

        self.assertEqual(int(data_resume["Applications"]), 2)
        self.assertEqual(int(data_resume["Interviews"]), 1)
        self.assertEqual(int(data_resume["Offers"]), 0)
        self.assertEqual(float(data_resume["Interview Rate"]), 50.0)

    def test_follow_up_timeline_groups_dates_into_buckets(self):
        timeline = build_follow_up_timeline_df(self.df, today=date(2026, 4, 10))
        counts = {row["Bucket"]: int(row["Count"]) for _, row in timeline.iterrows()}

        self.assertEqual(counts["Overdue"], 1)
        self.assertEqual(counts["Today"], 1)
        self.assertEqual(counts["Next 3 days"], 0)
        self.assertEqual(counts["Next 7 days"], 1)

    def test_pick_best_resume_summary_prefers_reliable_resume(self):
        resume_df = build_resume_performance_df(self.df)

        summary = pick_best_resume_summary(resume_df)

        self.assertEqual(summary["Resume"], "Data Resume")
        self.assertEqual(summary["Applications"], 2)
        self.assertEqual(summary["Interview Rate"], 50.0)

    def test_build_dashboard_trend_badges_returns_core_badges(self):
        weekly = build_weekly_momentum_summary(self.df, today=date(2026, 4, 10))
        funnel = build_pipeline_funnel_df(self.df)
        timeline = build_follow_up_timeline_df(self.df, today=date(2026, 4, 10))
        resume_df = build_resume_performance_df(self.df)

        badges = build_dashboard_trend_badges(weekly, funnel, timeline, resume_df)
        labels = [badge["label"] for badge in badges]

        self.assertEqual(labels, ["Application pace", "Follow-up pressure", "Applied to interview", "Next 7 days", "Best resume"])
        self.assertEqual(badges[0]["value"], "Down 1 vs last week")
        self.assertEqual(badges[3]["value"], "3 follow-up item(s)")

    def test_filter_tracker_rows_supports_followup_and_fit_bands(self):
        filtered = filter_tracker_rows(
            self.df.assign(
                _overdue=[False, False, True, False, True],
                _due_today=[False, True, False, False, False],
            ),
            status_filter=["Applied", "Interview", "Rejected"],
            followup_filter="Overdue only",
            fit_filter="Needs review (<65)",
        )

        self.assertEqual(filtered["Company"].tolist(), ["Echo"])


class AuthValidationTests(unittest.TestCase):
    def test_username_validation_rejects_spaces(self):
        with self.assertRaises(ValueError):
            validate_username("bad user")

    def test_password_validation_requires_minimum_length(self):
        with self.assertRaises(ValueError):
            validate_password("short")


class FollowUpActionTests(unittest.TestCase):
    def test_follow_up_action_for_overdue_application(self):
        yesterday = date.today() - timedelta(days=1)
        self.assertEqual(
            recommend_follow_up_action("Applied", str(yesterday), today=date.today()),
            "Send follow-up now",
        )


if __name__ == "__main__":
    unittest.main()
