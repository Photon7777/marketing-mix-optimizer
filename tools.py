import re
from datetime import date
from typing import List, Dict, Optional

import requests
from bs4 import BeautifulSoup


# -------------------------
# BASIC HELPERS
# -------------------------
def fetch_job_post(url: str, timeout: int = 15) -> str:
    """
    Basic HTML fetch + text extraction.
    Works for many sites, but some (LinkedIn, etc.) may block scraping.
    """
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    text = soup.get_text(separator="\n")
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text[:20000]


def safe_clip(text: str, limit: int = 20000) -> str:
    return (text or "")[:limit]


# -------------------------
# FIT ANALYSIS
# -------------------------
STOPWORDS = {
    "the", "and", "for", "with", "from", "that", "this", "will", "your", "you",
    "our", "are", "job", "role", "team", "work", "have", "has", "had", "who",
    "what", "when", "where", "why", "how", "about", "into", "onto", "their",
    "them", "they", "his", "her", "its", "can", "may", "should", "would",
    "could", "ability", "skills", "skill", "experience", "preferred", "plus",
    "using", "use", "used", "build", "building", "develop", "development",
    "support", "supporting", "strong", "knowledge", "understanding", "including",
    "intern", "internship", "candidate", "responsibilities", "requirements",
    "qualification", "qualifications", "position", "opportunity", "company",
    "data", "business"
}

KEY_PHRASES = [
    "python", "sql", "r", "java", "c++", "javascript", "typescript",
    "machine learning", "deep learning", "natural language processing", "nlp",
    "data analysis", "data analytics", "data engineering", "data science",
    "statistics", "statistical analysis", "experimentation", "a/b testing",
    "dashboarding", "visualization", "tableau", "power bi", "excel",
    "pandas", "numpy", "scikit-learn", "tensorflow", "pytorch", "xgboost",
    "spark", "hadoop", "airflow", "dbt", "etl", "elt",
    "postgres", "postgresql", "mysql", "mongodb", "neo4j",
    "aws", "azure", "gcp", "cloud", "docker", "kubernetes", "git",
    "streamlit", "api", "apis", "rest", "flask", "fastapi",
    "product analytics", "business intelligence", "bi",
    "communication", "stakeholder management", "problem solving"
]


def _normalize_text(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("postgresql", "postgres")
    text = text.replace("powerbi", "power bi")
    text = text.replace("scikit learn", "scikit-learn")
    text = re.sub(r"[^a-z0-9\+\#\.\-/\s]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_keywords(text: str) -> List[str]:
    """
    Hybrid keyword extraction:
    1. Keep known key phrases if present.
    2. Add informative single tokens.
    """
    text_norm = _normalize_text(text)
    found = []

    for phrase in KEY_PHRASES:
        if phrase in text_norm:
            found.append(phrase)

    tokens = re.findall(r"\b[a-zA-Z][a-zA-Z0-9\+\#\.-]{1,}\b", text_norm)
    token_counts = {}

    for tok in tokens:
        tok = tok.lower().strip()
        if tok in STOPWORDS:
            continue
        if len(tok) < 3 and tok not in {"r", "bi"}:
            continue
        if tok.isdigit():
            continue
        token_counts[tok] = token_counts.get(tok, 0) + 1

    extra = sorted(token_counts.items(), key=lambda x: (-x[1], x[0]))
    for tok, _ in extra[:40]:
        if tok not in found:
            found.append(tok)

    return found


def _priority_from_score(score: int) -> str:
    if score >= 75:
        return "Apply now"
    if score >= 55:
        return "Strong consider"
    if score >= 35:
        return "Apply if strategic"
    return "Low priority"


def analyze_fit(job_post_text: str, resume_text: str) -> Dict:
    """
    Heuristic keyword-based fit scoring.
    Returns:
      {
        "score": int,
        "priority": str,
        "summary": str,
        "matched_skills": list[str],
        "missing_skills": list[str],
      }
    """
    job_keywords = _extract_keywords(job_post_text)
    resume_keywords = set(_extract_keywords(resume_text))

    matched = [kw for kw in job_keywords if kw in resume_keywords]
    missing = [kw for kw in job_keywords if kw not in resume_keywords]

    important_job_terms = job_keywords[:20] if job_keywords else []
    denom = max(len(important_job_terms), 1)
    important_matches = len([kw for kw in important_job_terms if kw in resume_keywords])

    score = round((important_matches / denom) * 100)

    broad_overlap = len(set(job_keywords[:40]).intersection(resume_keywords))
    if broad_overlap >= 10:
        score = min(100, score + 10)
    elif broad_overlap >= 6:
        score = min(100, score + 5)

    priority = _priority_from_score(score)

    if score >= 75:
        summary = "Strong alignment between the job requirements and your resume. This looks like a high-priority application."
    elif score >= 55:
        summary = "Good alignment overall. You match several core requirements, though a few skills may need stronger framing."
    elif score >= 35:
        summary = "Partial alignment. This role may still be worth applying to if it is strategic or you can tailor your story carefully."
    else:
        summary = "Limited alignment based on the visible keywords. Consider applying only if the role is especially important or adjacent to your goals."

    return {
        "score": int(score),
        "priority": priority,
        "summary": summary,
        "matched_skills": matched[:12],
        "missing_skills": missing[:12],
    }


# -------------------------
# RESUME RANKING
# -------------------------
def rank_resume_versions(job_post_text: str, resume_payloads: List[Dict]) -> List[Dict]:
    """
    Input:
      resume_payloads = [
        {"resume_id": "...", "name": "...", "resume_text": "..."},
        ...
      ]
    Output:
      sorted list with score/priority/summary added
    """
    ranked = []

    for item in resume_payloads or []:
        resume_text = item.get("resume_text", "") or ""
        fit = analyze_fit(job_post_text, resume_text)

        ranked.append(
            {
                "resume_id": item.get("resume_id", ""),
                "name": item.get("name", "Resume"),
                "resume_text": resume_text,
                "score": fit["score"],
                "priority": fit["priority"],
                "summary": fit["summary"],
                "matched_skills": fit["matched_skills"],
                "missing_skills": fit["missing_skills"],
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


# -------------------------
# NEXT ACTION / FOLLOW-UP
# -------------------------
def _parse_date_maybe(value: str):
    try:
        return date.fromisoformat(str(value).strip())
    except Exception:
        return None


def recommend_follow_up_action(
    status: str,
    follow_up_date: str,
    today: Optional[date] = None,
) -> str:
    today = today or date.today()
    status_clean = (status or "").strip()
    follow_dt = _parse_date_maybe(follow_up_date)

    if status_clean == "Interested":
        return "Tailor resume and apply"

    if status_clean == "Applied":
        if follow_dt:
            if follow_dt < today:
                return "Send follow-up now"
            if follow_dt == today:
                return "Follow up today"
        return "Wait and prepare next application step"

    if status_clean == "OA":
        return "Complete assessment / prepare for next round"

    if status_clean == "Interview":
        return "Prepare stories, company research, and questions"

    if status_clean == "Offer":
        return "Review offer details and respond"

    if status_clean == "Rejected":
        return "Archive and reflect on fit gaps"

    if status_clean == "Ghosted":
        if follow_dt and follow_dt <= today:
            return "Send final follow-up"
        return "Consider closing out this application"

    if follow_dt:
        if follow_dt < today:
            return "Follow up now"
        if follow_dt == today:
            return "Follow up today"

    return "Review status and decide next step"
