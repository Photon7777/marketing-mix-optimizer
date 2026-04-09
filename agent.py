import os
import json
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=_OPENAI_API_KEY) if _OPENAI_API_KEY else None
APPLICATION_MODEL = os.getenv("OPENAI_APPLICATION_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")
OUTREACH_MODEL = os.getenv("OPENAI_OUTREACH_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")


def _client() -> OpenAI:
    if client is None:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    return client

SYSTEM = """You are an Internship Application Agent for Data/Analytics roles.
Your goal: help the user apply faster with higher-quality materials.

Return output in this EXACT structure with clear headings:

1) ROLE SUMMARY (2-3 bullets)
2) MUST-HAVE SKILLS (bullets)
3) NICE-TO-HAVE SKILLS (bullets)
4) RESUME GAP CHECK (bullets: Missing / Weak / Strong matches)
5) TAILORED RESUME BULLETS (5-7 bullets; quantifiable; ATS-friendly)
6) 120-WORD COVER LETTER PARAGRAPH (1 paragraph)
7) NETWORKING MESSAGE (LinkedIn DM <= 80 words)
8) INTERVIEW TALKING POINTS (4 bullets)

Rules:
- If job post is messy/partial, still produce best-effort.
- Be specific: use keywords from the job post.
- Never invent experiences; if unsure, phrase as suggestions.
"""

def generate_application_materials(job_post: str, resume_text: str, extra_notes: str = "") -> str:
    user = f"""
JOB POST:
{job_post}

RESUME (USER-PROVIDED):
{resume_text}

EXTRA NOTES:
{extra_notes}
"""
    resp = _client().chat.completions.create(
        model=APPLICATION_MODEL,
        messages=[
            {"role": "system", "content": SYSTEM},
            {"role": "user", "content": user},
        ],
        temperature=0.4,
    )
    return resp.choices[0].message.content


OUTREACH_SYSTEM = """You write concise, ethical cold outreach emails for internship or early-career job search.
Return ONLY valid JSON with these exact keys:
subject, body, followup, personalization_angle

Rules:
- Make the message specific to the company, recipient role, target role, and candidate background.
- Prefer the strongest concrete company angle in this order: matched_role, primary_focus, company_reason, company_context.
- If matched_role is present, anchor the opening to that role instead of generic curiosity language.
- Use one concrete candidate angle from candidate_summary or resume_highlights_source.
- Connect the candidate angle to the target role in a natural way.
- Vary the opening and subject line; do not reuse "I came across..." as the default opening.
- Use a crisp structure: company/role fit, candidate proof, recipient-aware ask.
- If recipient_title is recruiting/talent, ask for the right opening or process; if it is a manager, ask for guidance on relevant teams or skills.
- Do not invent candidate experience, company facts, openings, referrals, or prior contact.
- If company context is weak, use a cautious angle and do not pretend to know details.
- If discovery_source or role_source_url suggests where the role was found, you may reference that a matching role was discovered, but never claim the user already applied.
- Keep the first email under 150 words and the follow-up under 95 words.
- Avoid hype, generic flattery, spammy wording, and identical phrasing across companies.
- Keep the tone warm, specific, and direct. No long paragraphs.
- Include LinkedIn and portfolio links only if provided.
- personalization_angle should be a short phrase explaining the exact niche angle used.
"""


def _parse_json_object(raw: str) -> dict:
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start : end + 1])
    raise ValueError("Model did not return valid JSON.")


def generate_outreach_materials(
    *,
    candidate_summary: str,
    resume_text: str,
    company_name: str,
    company_context: str,
    company_reason: str,
    matched_role: str,
    primary_focus: str,
    discovery_source: str,
    role_source_url: str,
    role_title: str,
    recipient_name: str,
    recipient_title: str,
    linkedin_url: str,
    portfolio_url: str,
    sender_name: str,
    fallback_subject: str,
    fallback_body: str,
    fallback_followup: str,
) -> dict:
    payload = {
        "quality_bar": (
            "Write a niche email that could not be sent unchanged to a different company. "
            "If the company signal is weak, make the role/candidate fit specific instead."
        ),
        "candidate_summary": candidate_summary,
        "resume_highlights_source": (resume_text or "")[:6000],
        "company_name": company_name,
        "company_context": (company_context or "")[:2500],
        "company_reason": company_reason,
        "matched_role": matched_role,
        "primary_focus": primary_focus,
        "discovery_source": discovery_source,
        "role_source_url": role_source_url,
        "target_role": role_title,
        "recipient_name": recipient_name,
        "recipient_title": recipient_title,
        "linkedin_url": linkedin_url,
        "portfolio_url": portfolio_url,
        "sender_name": sender_name,
        "fallback_subject": fallback_subject,
        "fallback_body": fallback_body,
        "fallback_followup": fallback_followup,
    }
    resp = _client().chat.completions.create(
        model=OUTREACH_MODEL,
        messages=[
            {"role": "system", "content": OUTREACH_SYSTEM},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=True)},
        ],
        temperature=0.72,
    )
    parsed = _parse_json_object(resp.choices[0].message.content.strip())
    return {
        "subject": str(parsed.get("subject") or fallback_subject).strip(),
        "body": str(parsed.get("body") or fallback_body).strip(),
        "followup": str(parsed.get("followup") or fallback_followup).strip(),
        "personalization_angle": str(parsed.get("personalization_angle") or company_reason).strip(),
    }
