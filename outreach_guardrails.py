from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Set, Tuple


EMAIL_IN_NOTES_RE = re.compile(r"Email:\s*([A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,})", re.I)


def normalize_outreach_email(value: str) -> str:
    return (value or "").strip().lower()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def extract_outreach_email_from_notes(notes: str) -> str:
    match = EMAIL_IN_NOTES_RE.search(notes or "")
    if not match:
        return ""
    return normalize_outreach_email(match.group(1))


def contact_identity_keys(company: str, contact_name: str, email: str) -> Set[str]:
    keys: Set[str] = set()
    normalized_email = normalize_outreach_email(email)
    normalized_company = _normalize_text(company)
    normalized_name = _normalize_text(contact_name)

    if normalized_email:
        keys.add(f"email:{normalized_email}")
    if normalized_company and normalized_name:
        keys.add(f"company_name:{normalized_company}|{normalized_name}")
    if normalized_company and normalized_email:
        keys.add(f"company_email:{normalized_company}|{normalized_email}")
    return keys


def _row_records(rows: Any) -> List[Mapping[str, Any]]:
    if hasattr(rows, "to_dict"):
        return rows.to_dict("records")
    return list(rows or [])


def tracker_outreach_history(rows: Any) -> dict[str, Set[str]]:
    emails: Set[str] = set()
    identities: Set[str] = set()

    for row in _row_records(rows):
        company = str(row.get("Company", "") or "")
        contact_name = str(row.get("Contact Name", "") or "")
        notes = str(row.get("Notes", "") or "")
        email = normalize_outreach_email(str(row.get("Email", "") or "")) or extract_outreach_email_from_notes(notes)

        if email:
            emails.add(email)
        identities.update(contact_identity_keys(company, contact_name, email))

    return {"emails": emails, "identities": identities}


def is_duplicate_contact(
    company: str,
    contact_name: str,
    email: str,
    *,
    blocked_emails: Optional[Iterable[str]] = None,
    blocked_identity_keys: Optional[Iterable[str]] = None,
) -> bool:
    normalized_email = normalize_outreach_email(email)
    blocked_email_set = {normalize_outreach_email(item) for item in (blocked_emails or []) if normalize_outreach_email(item)}
    if normalized_email and normalized_email in blocked_email_set:
        return True

    identity_keys = contact_identity_keys(company, contact_name, email)
    blocked_identity_set = {str(item) for item in (blocked_identity_keys or []) if str(item)}
    return bool(identity_keys & blocked_identity_set)


def filter_new_contacts(
    company: str,
    contacts: Sequence[Mapping[str, Any]],
    *,
    blocked_emails: Optional[Iterable[str]] = None,
    blocked_identity_keys: Optional[Iterable[str]] = None,
) -> Tuple[List[Mapping[str, Any]], int]:
    seen_emails = {normalize_outreach_email(item) for item in (blocked_emails or []) if normalize_outreach_email(item)}
    seen_identity_keys = {str(item) for item in (blocked_identity_keys or []) if str(item)}

    filtered: List[Mapping[str, Any]] = []
    skipped = 0

    for contact in contacts:
        email = normalize_outreach_email(str(contact.get("email", "") or ""))
        identity_keys = contact_identity_keys(
            company,
            str(contact.get("name", "") or ""),
            email,
        )

        if email and email in seen_emails:
            skipped += 1
            continue
        if identity_keys and identity_keys & seen_identity_keys:
            skipped += 1
            continue

        filtered.append(contact)
        if email:
            seen_emails.add(email)
        seen_identity_keys.update(identity_keys)

    return filtered, skipped


def unique_send_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    already_contacted_emails: Optional[Iterable[str]] = None,
) -> Tuple[List[Mapping[str, Any]], List[dict[str, str]]]:
    existing = {normalize_outreach_email(item) for item in (already_contacted_emails or []) if normalize_outreach_email(item)}
    seen = set(existing)
    unique_rows: List[Mapping[str, Any]] = []
    skipped: List[dict[str, str]] = []

    for row in rows:
        email = normalize_outreach_email(str(row.get("Email", "") or ""))
        if not email:
            skipped.append({"email": "", "reason": "missing_email"})
            continue
        if email in seen:
            reason = "already_contacted" if email in existing else "duplicate_in_batch"
            skipped.append({"email": email, "reason": reason})
            continue
        seen.add(email)
        unique_rows.append(row)

    return unique_rows, skipped


def build_outreach_tracker_row(
    *,
    company: str,
    preferred_role: str,
    target_location: str,
    contact_name: str,
    contact_link: str,
    email: str,
    subject: str,
    personalization: str,
    sponsorship_signal: str,
    resume_name: str,
    send_source: str,
    role_source_url: str = "",
    sent_on: Optional[date] = None,
) -> dict[str, str]:
    today = sent_on or date.today()
    follow_up = today + timedelta(days=5)
    notes_lines = [
        "Outreach State: sent",
        f"Send Source: {send_source}",
        f"Sent On: {today.isoformat()}",
    ]
    if role_source_url:
        notes_lines.append(f"Role Source URL: {role_source_url}")

    return {
        "Date": today.isoformat(),
        "Company": company,
        "Role": preferred_role or "Cold outreach",
        "Location": target_location,
        "Status": "Sent",
        "Resume Used": resume_name,
        "Follow-up Date": follow_up.isoformat(),
        "Contact Name": contact_name,
        "Contact Link": contact_link,
        "Email": email,
        "Subject": subject,
        "Personalization": personalization,
        "Sponsorship Signal": sponsorship_signal or "Unknown / verify",
        "Send Source": send_source,
        "Role Source URL": role_source_url,
        "Notes": "\n".join(notes_lines),
    }
