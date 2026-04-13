import os
import sys
import unittest
from datetime import date, timedelta
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from auth import (
    decrypt_user_gmail_token,
    encrypt_user_gmail_token,
    gmail_oauth_state_matches,
    validate_password,
    validate_username,
)
from agent import _parse_json_object
from company_discovery import (
    build_muse_candidate,
    clean_company_name,
    company_from_search_result,
    discovery_score,
    role_family_for_role,
    role_company_queries,
    role_search_terms,
    role_specific_fallbacks,
    role_specific_startup_fallbacks,
    source_label_from_url,
    sponsorship_signal_for_company,
)
from dashboard_metrics import (
    build_fit_outcome_df,
    build_follow_up_timeline_df,
    build_pipeline_funnel_df,
    build_resume_performance_df,
    build_weekly_activity_df,
    build_weekly_momentum_summary,
)
from db_store import merge_visible_tracker_edits
from gmail_sender import GMAIL_CONNECT_SCOPES, GmailSender
from hunter_helper import HunterClient
from outreach_guardrails import (
    build_outreach_tracker_row,
    classify_outreach_thread,
    contact_identity_keys,
    fallback_outreach_next_step,
    filter_new_contacts,
    normalize_outreach_email,
    tracker_outreach_history,
    unique_send_rows,
)
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


class AuthValidationTests(unittest.TestCase):
    def test_username_validation_rejects_spaces(self):
        with self.assertRaises(ValueError):
            validate_username("bad user")

    def test_password_validation_requires_minimum_length(self):
        with self.assertRaises(ValueError):
            validate_password("short")

    def test_gmail_token_encryption_round_trip(self):
        previous = os.environ.get("GMAIL_TOKEN_ENCRYPTION_KEY")
        os.environ["GMAIL_TOKEN_ENCRYPTION_KEY"] = "unit-test-gmail-encryption-secret"
        try:
            encrypted = encrypt_user_gmail_token('{"refresh_token":"abc"}')
            decrypted = decrypt_user_gmail_token(encrypted)
        finally:
            if previous is None:
                os.environ.pop("GMAIL_TOKEN_ENCRYPTION_KEY", None)
            else:
                os.environ["GMAIL_TOKEN_ENCRYPTION_KEY"] = previous

        self.assertEqual(decrypted, '{"refresh_token":"abc"}')

    def test_gmail_token_encryption_falls_back_to_database_url(self):
        previous_token = os.environ.get("GMAIL_TOKEN_ENCRYPTION_KEY")
        previous_app = os.environ.get("APP_ENCRYPTION_KEY")
        previous_nextrole = os.environ.get("NEXTROLE_APP_ENCRYPTION_KEY")
        previous_db = os.environ.get("DATABASE_URL")
        os.environ.pop("GMAIL_TOKEN_ENCRYPTION_KEY", None)
        os.environ.pop("APP_ENCRYPTION_KEY", None)
        os.environ.pop("NEXTROLE_APP_ENCRYPTION_KEY", None)
        os.environ["DATABASE_URL"] = "postgresql://unit-test-user:secret@localhost:5432/testdb"
        try:
            encrypted = encrypt_user_gmail_token('{"refresh_token":"db-fallback"}')
            decrypted = decrypt_user_gmail_token(encrypted)
        finally:
            if previous_token is None:
                os.environ.pop("GMAIL_TOKEN_ENCRYPTION_KEY", None)
            else:
                os.environ["GMAIL_TOKEN_ENCRYPTION_KEY"] = previous_token
            if previous_app is None:
                os.environ.pop("APP_ENCRYPTION_KEY", None)
            else:
                os.environ["APP_ENCRYPTION_KEY"] = previous_app
            if previous_nextrole is None:
                os.environ.pop("NEXTROLE_APP_ENCRYPTION_KEY", None)
            else:
                os.environ["NEXTROLE_APP_ENCRYPTION_KEY"] = previous_nextrole
            if previous_db is None:
                os.environ.pop("DATABASE_URL", None)
            else:
                os.environ["DATABASE_URL"] = previous_db

        self.assertEqual(decrypted, '{"refresh_token":"db-fallback"}')

    def test_gmail_oauth_state_matches_db_fallback(self):
        self.assertTrue(
            gmail_oauth_state_matches(
                "state-123",
                session_state="",
                session_user="",
                db_state="state-123",
                user_id="ram2112",
            )
        )

    def test_gmail_oauth_state_rejects_wrong_user_and_state(self):
        self.assertFalse(
            gmail_oauth_state_matches(
                "state-123",
                session_state="state-123",
                session_user="other_user",
                db_state="different-state",
                user_id="ram2112",
            )
        )


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

    def test_role_family_maps_related_titles(self):
        self.assertEqual(role_family_for_role("Analytics Engineer Intern"), "data_engineering")

    def test_role_search_terms_expand_role_family(self):
        terms = role_search_terms("Data Engineering Intern", limit=4)

        self.assertIn("analytics engineer", [term.lower() for term in terms])

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

    def test_build_muse_candidate_uses_matching_job(self):
        job = {
            "name": "Data Engineer Intern",
            "contents": "<p>Work on ETL pipelines, warehouse models, and analytics infrastructure.</p>",
            "publication_date": "2026-04-08T00:00:00Z",
            "refs": {"landing_page": "https://www.themuse.com/jobs/example/data-engineer-intern"},
            "company": {"name": "Example AI", "id": 10},
            "categories": [{"name": "Data and Analytics"}],
            "locations": [{"name": "Remote"}],
        }
        company_profile = {
            "name": "Example AI",
            "refs": {"landing_page": "https://www.example.ai"},
            "industries": [{"name": "Internet and Software"}],
        }

        candidate = build_muse_candidate(job, company_profile, "Data Engineering Intern", "2026-04-08")

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.company, "Example AI")
        self.assertEqual(candidate.source, "the-muse-open-role")

    def test_outreach_json_parser_handles_wrapped_json(self):
        parsed = _parse_json_object('draft:\n{"subject": "Hi", "body": "Body", "followup": "Later"}')

        self.assertEqual(parsed["subject"], "Hi")

    def test_hunter_seniority_mapping(self):
        self.assertEqual(
            HunterClient.normalize_seniorities(["intern", "manager", "vp", "unknown"]),
            ["junior", "senior", "executive"],
        )

    def test_hunter_client_reads_api_key_from_streamlit_secrets(self):
        previous = os.environ.get("HUNTER_API_KEY")
        os.environ.pop("HUNTER_API_KEY", None)
        fake_streamlit = SimpleNamespace(secrets={"HUNTER_API_KEY": "hunter-from-secrets"})
        try:
            with mock.patch.dict(sys.modules, {"streamlit": fake_streamlit}):
                client = HunterClient()
        finally:
            if previous is None:
                os.environ.pop("HUNTER_API_KEY", None)
            else:
                os.environ["HUNTER_API_KEY"] = previous

        self.assertEqual(client.api_key, "hunter-from-secrets")

    def test_gmail_recipient_normalization(self):
        self.assertEqual(
            GmailSender._normalize_recipients("a@example.com, b@example.com"),
            ["a@example.com", "b@example.com"],
        )

    def test_gmail_connect_scopes_include_inbox_read(self):
        self.assertIn("https://www.googleapis.com/auth/gmail.readonly", GMAIL_CONNECT_SCOPES)

    def test_gmail_attachment_preserves_mime_type(self):
        part = GmailSender._attachment_part(b"resume", "resume.txt", "text/plain")

        self.assertEqual(part.get_content_type(), "text/plain")

    def test_gmail_oauth_redirect_uri_uses_env_override(self):
        previous = os.environ.get("GMAIL_OAUTH_REDIRECT_URI")
        os.environ["GMAIL_OAUTH_REDIRECT_URI"] = "http://localhost:8501"
        try:
            redirect_uri = GmailSender.oauth_redirect_uri()
        finally:
            if previous is None:
                os.environ.pop("GMAIL_OAUTH_REDIRECT_URI", None)
            else:
                os.environ["GMAIL_OAUTH_REDIRECT_URI"] = previous

        self.assertEqual(redirect_uri, "http://localhost:8501")

    def test_follow_up_action_for_overdue_application(self):
        yesterday = date.today() - timedelta(days=1)
        self.assertEqual(
            recommend_follow_up_action("Applied", str(yesterday), today=date.today()),
            "Send follow-up now",
        )

    def test_tracker_outreach_history_extracts_logged_email(self):
        history = tracker_outreach_history(
            [
                {
                    "Company": "Acme",
                    "Contact Name": "Jane Doe",
                    "Email": "jane@acme.com",
                    "Notes": "Subject: hello",
                }
            ]
        )

        self.assertIn("jane@acme.com", history["emails"])
        self.assertIn("company_email:acme|jane@acme.com", history["identities"])

    def test_filter_new_contacts_skips_previously_contacted_and_internal_duplicates(self):
        contacts = [
            {"name": "Jane Doe", "email": "jane@acme.com"},
            {"name": "Jane Doe", "email": "jane@acme.com"},
            {"name": "John Smith", "email": "john@acme.com"},
        ]

        filtered, skipped = filter_new_contacts(
            "Acme",
            contacts,
            blocked_emails={"jane@acme.com"},
            blocked_identity_keys=contact_identity_keys("Acme", "Jane Doe", "jane@acme.com"),
        )

        self.assertEqual(skipped, 2)
        self.assertEqual([contact["email"] for contact in filtered], ["john@acme.com"])

    def test_unique_send_rows_skips_prior_and_batch_duplicates(self):
        rows = [
            {"Email": "a@example.com"},
            {"Email": "a@example.com"},
            {"Email": "b@example.com"},
        ]

        unique_rows, skipped = unique_send_rows(
            rows,
            already_contacted_emails={"b@example.com"},
        )

        self.assertEqual([normalize_outreach_email(row["Email"]) for row in unique_rows], ["a@example.com"])
        self.assertEqual({item["reason"] for item in skipped}, {"duplicate_in_batch", "already_contacted"})

    def test_build_outreach_tracker_row_marks_sent_state(self):
        row = build_outreach_tracker_row(
            company="Acme",
            preferred_role="Data Engineer Intern",
            target_location="Remote",
            contact_name="Jane Doe",
            contact_link="",
            email="jane@acme.com",
            subject="Acme: interest in Data Engineer Intern",
            personalization="Matched to Acme's data pipeline role.",
            sponsorship_signal="Unknown / verify",
            resume_name="Data Resume",
            send_source="bulk_campaign",
            role_source_url="https://example.com/jobs/123",
            gmail_message_id="msg-123",
            gmail_thread_id="thread-123",
        )

        self.assertEqual(row["Company"], "Acme")
        self.assertEqual(row["Status"], "Sent")
        self.assertEqual(row["Reply Status"], "No reply")
        self.assertEqual(row["Email"], "jane@acme.com")
        self.assertEqual(row["Send Source"], "bulk_campaign")
        self.assertEqual(row["Gmail Message ID"], "msg-123")
        self.assertEqual(row["Gmail Thread ID"], "thread-123")
        self.assertIn("Outreach State: sent", row["Notes"])
        self.assertIn("Send Source: bulk_campaign", row["Notes"])

    def test_classify_outreach_thread_marks_reply_and_pauses_followup(self):
        updates = classify_outreach_thread(
            {
                "has_inbound_reply": True,
                "is_bounce": False,
                "latest_message_at": "2026-04-10T12:30:00+00:00",
                "latest_message_from": "Jane Doe <jane@acme.com>",
                "latest_snippet": "Happy to chat next week.",
            },
            follow_up_date="2026-04-15",
            today=date(2026, 4, 10),
        )

        self.assertEqual(updates["Status"], "Replied")
        self.assertEqual(updates["Reply Status"], "Replied")
        self.assertEqual(updates["Paused Reason"], "reply_received")
        self.assertEqual(updates["Next Follow-up At"], "")

    def test_classify_outreach_thread_marks_followup_due_without_reply(self):
        updates = classify_outreach_thread(
            {
                "has_inbound_reply": False,
                "is_bounce": False,
                "latest_message_at": "",
                "latest_message_from": "",
                "latest_snippet": "",
            },
            follow_up_date="2026-04-05",
            today=date(2026, 4, 10),
        )

        self.assertEqual(updates["Status"], "Sent")
        self.assertEqual(updates["Reply Status"], "Needs follow-up")
        self.assertEqual(updates["Next Follow-up At"], "2026-04-05")

    def test_fallback_outreach_next_step_builds_reply_for_inbound_response(self):
        suggestion = fallback_outreach_next_step(
            reply_status="Replied",
            company="Acme",
            role="Data Engineer Intern",
            contact_name="Jane Doe",
            sender_name="Ram",
            latest_reply_snippet="Happy to chat next week. Feel free to send times.",
            candidate_summary="MS in Information Systems student with analytics and SQL experience.",
            personalization="The data platform work looked closely aligned with my background",
        )

        self.assertIn("availability", suggestion["suggested_action"].lower())
        self.assertIn("Hi Jane", suggestion["suggested_followup"])

    def test_fallback_outreach_next_step_builds_gentle_followup(self):
        suggestion = fallback_outreach_next_step(
            reply_status="Needs follow-up",
            company="Example AI",
            role="Analytics Engineer Intern",
            contact_name="Taylor Smith",
            sender_name="Ram",
            candidate_summary="Graduate student with experience in SQL, dashboards, and analytics workflows.",
            personalization="The analytics infrastructure work looked especially relevant",
        )

        self.assertIn("gentle follow-up", suggestion["suggested_action"].lower())
        self.assertIn("Following up", suggestion["suggested_followup"])


if __name__ == "__main__":
    unittest.main()
