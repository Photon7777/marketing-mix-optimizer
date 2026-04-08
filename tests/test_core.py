import unittest
from datetime import date, timedelta

import pandas as pd

from auth import validate_password, validate_username
from agent import _parse_json_object
from company_discovery import (
    clean_company_name,
    company_from_search_result,
    discovery_score,
    role_company_queries,
    role_specific_fallbacks,
    role_specific_startup_fallbacks,
    source_label_from_url,
    sponsorship_signal_for_company,
)
from db_store import merge_visible_tracker_edits
from gmail_sender import GmailSender
from hunter_helper import HunterClient
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


class AuthValidationTests(unittest.TestCase):
    def test_username_validation_rejects_spaces(self):
        with self.assertRaises(ValueError):
            validate_username("bad user")

    def test_password_validation_requires_minimum_length(self):
        with self.assertRaises(ValueError):
            validate_password("short")


class OutreachHelperTests(unittest.TestCase):
    def test_company_extraction_from_greenhouse_url(self):
        company = company_from_search_result(
            "Data Analyst Intern - Acme Careers",
            "https://boards.greenhouse.io/acme/jobs/123",
            "Data Analyst Intern",
        )

        self.assertEqual(company, "Acme")

    def test_company_extraction_from_yc_startup_url(self):
        company = company_from_search_result(
            "Data Engineer Intern at Example AI",
            "https://www.ycombinator.com/companies/example-ai/jobs/abc-data-engineer-intern",
            "Data Engineer Intern",
        )

        self.assertEqual(company, "Example Ai")

    def test_clean_company_name_removes_role_terms(self):
        self.assertEqual(clean_company_name("Data Analyst Intern - Visa Careers", "Data Analyst Intern"), "Visa")

    def test_role_specific_fallbacks_use_role_keywords(self):
        self.assertIn("Databricks", role_specific_fallbacks("Data Engineering Intern"))

    def test_role_specific_startup_fallbacks_use_role_keywords(self):
        self.assertIn("Fivetran", role_specific_startup_fallbacks("Data Engineering Intern"))

    def test_sponsorship_signal_marks_known_sponsor(self):
        signal = sponsorship_signal_for_company("Databricks")

        self.assertEqual(signal["sponsorship_signal"], "Likely historical H-1B sponsor")

    def test_sponsorship_signal_marks_unknown_for_verify(self):
        signal = sponsorship_signal_for_company("Tiny Unknown Startup")

        self.assertEqual(signal["sponsorship_signal"], "Unknown / verify")

    def test_startup_queries_are_included(self):
        queries = role_company_queries("Data Engineer Intern", "Remote", include_startups=True)

        self.assertTrue(any("workatastartup" in query for query in queries))

    def test_source_label_identifies_startup_source(self):
        self.assertEqual(
            source_label_from_url("https://www.ycombinator.com/companies/example-ai/jobs/123"),
            "yc-startup-open-role",
        )

    def test_discovery_score_rewards_open_role_sources(self):
        score = discovery_score(
            "Data Engineer Intern at Example AI",
            "Apply to this internship role",
            "https://jobs.lever.co/example/123",
            "Data Engineer Intern",
            "lever-open-role",
        )

        self.assertGreater(score, 20)

    def test_outreach_json_parser_handles_wrapped_json(self):
        parsed = _parse_json_object('draft:\n{"subject": "Hi", "body": "Body", "followup": "Later"}')

        self.assertEqual(parsed["subject"], "Hi")

    def test_hunter_seniority_mapping(self):
        self.assertEqual(
            HunterClient.normalize_seniorities(["intern", "manager", "vp", "unknown"]),
            ["junior", "senior", "executive"],
        )

    def test_gmail_recipient_normalization(self):
        self.assertEqual(
            GmailSender._normalize_recipients("a@example.com, b@example.com"),
            ["a@example.com", "b@example.com"],
        )

    def test_gmail_attachment_preserves_mime_type(self):
        part = GmailSender._attachment_part(b"resume", "resume.txt", "text/plain")

        self.assertEqual(part.get_content_type(), "text/plain")

    def test_follow_up_action_for_overdue_application(self):
        yesterday = date.today() - timedelta(days=1)
        self.assertEqual(
            recommend_follow_up_action("Applied", str(yesterday), today=date.today()),
            "Send follow-up now",
        )


if __name__ == "__main__":
    unittest.main()
