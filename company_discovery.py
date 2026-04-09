from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from urllib.parse import parse_qs, quote_plus, unquote, urlparse

import requests
from bs4 import BeautifulSoup


GENERIC_COMPANY_WORDS = {
    "careers",
    "career",
    "jobs",
    "job",
    "hiring",
    "intern",
    "internship",
    "early career",
    "students",
    "student",
    "university",
    "apply",
    "workday",
    "greenhouse",
    "lever",
    "ashby",
    "smartrecruiters",
    "linkedin",
    "indeed",
    "glassdoor",
    "builtin",
    "wellfound",
    "simplify",
}

GENERIC_DOMAIN_LABELS = {
    "www",
    "careers",
    "jobs",
    "job",
    "boards",
    "apply",
    "app",
    "ats",
    "wd1",
    "wd3",
    "wd5",
    "job-boards",
}

STARTUP_SOURCE_HOSTS = {
    "workatastartup.com",
    "ycombinator.com",
    "wellfound.com",
    "startup.jobs",
    "simplify.jobs",
    "builtin.com",
}

ATS_SOURCE_HOSTS = {
    "greenhouse.io",
    "lever.co",
    "ashbyhq.com",
    "smartrecruiters.com",
    "myworkdayjobs.com",
}

ROLE_FAMILY_PROFILES = {
    "data_engineering": {
        "keywords": ("data engineer", "data engineering", "analytics engineer", "etl", "pipeline", "warehouse"),
        "search_terms": ["data engineer", "analytics engineer", "etl engineer", "data platform"],
        "focus": "data pipelines, warehousing, and analytics infrastructure",
        "signals": ("pipeline", "pipelines", "etl", "warehouse", "warehousing", "dbt", "spark", "airflow", "snowflake", "bigquery", "data platform"),
    },
    "data_analytics": {
        "keywords": ("data analyst", "business analyst", "business intelligence", "analytics", "reporting", "insights", "bi analyst", "product analyst"),
        "search_terms": ["data analyst", "business analyst", "business intelligence", "product analyst"],
        "focus": "analytics, reporting, and business decision support",
        "signals": ("analytics", "analyst", "sql", "dashboard", "reporting", "insight", "kpi", "metrics", "business intelligence"),
    },
    "ai_ml": {
        "keywords": ("machine learning", "ml", "ai", "data science", "data scientist", "applied scientist", "research scientist", "ai engineer", "ml engineer"),
        "search_terms": ["machine learning", "data science", "applied scientist", "ai engineer"],
        "focus": "applied AI, machine learning, and data science",
        "signals": ("machine learning", "artificial intelligence", "ai", "ml", "model", "llm", "rag", "retrieval", "data science", "applied scientist"),
    },
    "software": {
        "keywords": ("software engineer", "software", "backend", "frontend", "full stack", "developer", "swe", "platform engineer"),
        "search_terms": ["software engineer", "backend engineer", "full stack engineer", "platform engineer"],
        "focus": "product engineering, backend systems, and developer infrastructure",
        "signals": ("backend", "frontend", "full stack", "api", "platform", "distributed systems", "developer tools", "engineering"),
    },
    "product": {
        "keywords": ("product manager", "product management", "associate product manager", "apm", "product analyst"),
        "search_terms": ["product analyst", "associate product manager", "product manager", "product"],
        "focus": "product strategy, experimentation, and product analytics",
        "signals": ("product", "roadmap", "customer", "experimentation", "metrics", "user research", "go-to-market", "product analytics"),
    },
}

DEFAULT_ROLE_FAMILY = "data_analytics"

ROLE_FALLBACKS = [
    (
        ("data engineer", "data engineering", "etl", "analytics engineer"),
        ["Databricks", "Snowflake", "Capital One", "Bloomberg", "DoorDash", "Stripe", "Visa", "ServiceNow", "MongoDB", "Palantir"],
    ),
    (
        ("data scientist", "machine learning", "ml", "ai engineer", "research scientist"),
        ["Databricks", "NVIDIA", "Meta", "Adobe", "Spotify", "Uber", "Airbnb", "Instacart", "OpenAI", "Anthropic"],
    ),
    (
        ("data analyst", "business analyst", "analytics", "bi analyst", "business intelligence"),
        ["Capital One", "Visa", "Bloomberg", "Deloitte", "Accenture", "Adobe", "Amazon", "American Express", "Intuit", "Workday"],
    ),
    (
        ("software", "backend", "frontend", "full stack", "swe", "developer"),
        ["Microsoft", "Amazon", "ServiceNow", "Cisco", "Datadog", "Stripe", "Atlassian", "Adobe", "MongoDB", "Cloudflare"],
    ),
    (
        ("product", "product analyst", "product manager", "apm"),
        ["Microsoft", "Adobe", "Intuit", "Atlassian", "ServiceNow", "Capital One", "Visa", "Datadog", "Dropbox", "HubSpot"],
    ),
]

DEFAULT_FALLBACKS = [
    "Amazon",
    "Microsoft",
    "Adobe",
    "Capital One",
    "Visa",
    "Bloomberg",
    "ServiceNow",
    "Cisco",
    "Datadog",
    "Intuit",
]

STARTUP_ROLE_FALLBACKS = [
    (
        ("data engineer", "data engineering", "etl", "analytics engineer"),
        ["Fivetran", "dbt Labs", "Hightouch", "Census", "MotherDuck", "Monte Carlo", "Hex", "Airbyte", "Prefect", "ClickHouse", "Dagster Labs", "Astronomer"],
    ),
    (
        ("data scientist", "machine learning", "ml", "ai engineer", "research scientist"),
        ["Scale AI", "Hugging Face", "Weights & Biases", "Pinecone", "Anyscale", "Cohere", "Perplexity AI", "Runway", "Baseten", "Anthropic", "Mistral AI", "Abridge"],
    ),
    (
        ("data analyst", "business analyst", "analytics", "bi analyst", "business intelligence"),
        ["Ramp", "Brex", "Mercury", "Modern Treasury", "Plaid", "Hightouch", "Figma", "Notion", "Rippling", "Gusto", "Airtable", "Vanta"],
    ),
    (
        ("software", "backend", "frontend", "full stack", "swe", "developer"),
        ["Rippling", "Figma", "Notion", "Ramp", "Vercel", "Linear", "Retool", "Supabase", "Vanta", "Cursor", "Render", "Clerk"],
    ),
    (
        ("product", "product analyst", "product manager", "apm"),
        ["Ramp", "Notion", "Figma", "Linear", "Airtable", "Retool", "Rippling", "Vanta", "Mercury", "Brex", "Amplitude", "PostHog"],
    ),
]

KNOWN_HISTORICAL_H1B_SPONSOR_SIGNAL = {
    "adobe",
    "airbnb",
    "airtable",
    "amazon",
    "americanexpress",
    "anthropic",
    "atlassian",
    "bloomberg",
    "brex",
    "capitalone",
    "chime",
    "cisco",
    "cohere",
    "databricks",
    "datadog",
    "doordash",
    "dropbox",
    "figma",
    "fivetran",
    "google",
    "gusto",
    "hightouch",
    "huggingface",
    "instacart",
    "intuit",
    "meta",
    "microsoft",
    "moderntreasury",
    "mongodb",
    "notion",
    "nvidia",
    "openai",
    "perplexityai",
    "plaid",
    "ramp",
    "rippling",
    "scaleai",
    "servicenow",
    "snowflake",
    "spotify",
    "stripe",
    "uber",
    "visa",
    "workday",
}

NEGATIVE_TITLE_PATTERNS = [
    re.compile(pattern, re.I)
    for pattern in (
        r"\bsales\b",
        r"\bmarketing\b",
        r"\bconstruction\b",
        r"\bstore\b",
        r"\bmerchandising\b",
        r"\bpharmacy\b",
        r"\bnurse\b",
        r"\bclinical\b",
        r"\bhuman resources\b",
        r"\brecruit",
        r"\baccount executive\b",
    )
]

HARD_REJECT_TITLE_PATTERNS = [
    re.compile(pattern, re.I)
    for pattern in (
        r"\b2025\b",
        r"\bphd\b",
        r"\bunpaid\b",
        r"\bsenior director\b",
    )
]


def _safe_get(url: str, timeout: int = 10) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.ok:
            return resp.text[:180000]
    except Exception:
        return ""
    return ""


def company_key(company: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (company or "").lower())


def sponsorship_signal_for_company(company: str) -> dict[str, str]:
    key = company_key(company)
    likely = key in KNOWN_HISTORICAL_H1B_SPONSOR_SIGNAL
    query = quote_plus(f'{company} H-1B sponsor USCIS Employer Data Hub')
    return {
        "sponsorship_signal": "Likely historical H-1B sponsor" if likely else "Unknown / verify",
        "sponsorship_source": "local historical sponsor signal" if likely else "not in local sponsor signal list",
        "sponsorship_lookup_url": f"https://www.google.com/search?q={query}",
    }


def decode_duckduckgo_href(href: str) -> str:
    if not href:
        return ""
    if href.startswith("//"):
        href = "https:" + href
    parsed = urlparse(href)
    query = parse_qs(parsed.query)
    if "uddg" in query and query["uddg"]:
        return unquote(query["uddg"][0])
    return href


def _role_tokens(preferred_role: str) -> list[str]:
    return [tok.lower() for tok in re.findall(r"[A-Za-z][A-Za-z0-9+#.-]*", preferred_role or "") if len(tok) > 2]


def role_family_for_role(preferred_role: str) -> str:
    lowered = (preferred_role or "").lower()
    for family, profile in ROLE_FAMILY_PROFILES.items():
        if any(keyword in lowered for keyword in profile["keywords"]):
            return family
    return DEFAULT_ROLE_FAMILY


def role_search_terms(preferred_role: str, limit: int = 4) -> list[str]:
    family = role_family_for_role(preferred_role)
    profile = ROLE_FAMILY_PROFILES[family]
    candidates = [preferred_role.strip()] if preferred_role.strip() else []
    candidates.extend(profile["search_terms"])

    if "intern" not in (preferred_role or "").lower():
        candidates.extend([f"{term} intern" for term in profile["search_terms"][:2]])

    seen = set()
    terms = []
    for candidate in candidates:
        cleaned = re.sub(r"\s+", " ", candidate).strip()
        if not cleaned:
            continue
        key = cleaned.lower()
        if key in seen:
            continue
        seen.add(key)
        terms.append(cleaned)
        if len(terms) >= max(limit, 1):
            break
    return terms


def role_specific_fallbacks(preferred_role: str) -> list[str]:
    role = (preferred_role or "").lower()
    for keywords, companies in ROLE_FALLBACKS:
        if any(keyword in role for keyword in keywords):
            return companies
    return DEFAULT_FALLBACKS


def role_specific_startup_fallbacks(preferred_role: str) -> list[str]:
    role = (preferred_role or "").lower()
    for keywords, companies in STARTUP_ROLE_FALLBACKS:
        if any(keyword in role for keyword in keywords):
            return companies
    return ["Ramp", "Rippling", "Figma", "Notion", "Vercel", "Retool", "Mercury", "Brex", "Linear", "Vanta"]


def clean_company_name(value: str, preferred_role: str = "") -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    text = re.sub(r"https?://\S+", " ", text)
    text = text.replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip(" -|:,.")

    cleanup_patterns = [
        r"\b(careers?|jobs?|job openings?|open roles?|hiring|apply now)\b",
        r"\b(internships?|intern|new grad|students?|student|university|campus)\b",
        r"\b(remote|hybrid|onsite|full[- ]?time|part[- ]?time)\b",
        r"\b(united states|usa|us|canada|india)\b",
    ]
    for pattern in cleanup_patterns:
        text = re.sub(pattern, " ", text, flags=re.I)

    for token in _role_tokens(preferred_role):
        text = re.sub(rf"\b{re.escape(token)}\b", " ", text, flags=re.I)

    text = re.sub(r"\s+", " ", text).strip(" -|:,.")
    text = re.sub(r"^(at|for|with|from)\s+", "", text, flags=re.I).strip()

    if not text or len(text) < 2 or len(text) > 60:
        return ""
    if text.lower() in GENERIC_COMPANY_WORDS:
        return ""
    if re.fullmatch(r"[0-9\W_]+", text):
        return ""
    return text if any(ch.isupper() for ch in text[1:]) else text.title()


def _company_from_domain(url: str, preferred_role: str = "") -> str:
    parsed = urlparse(url)
    host = parsed.netloc.lower().replace("www.", "")
    parts = [part for part in parsed.path.split("/") if part]

    if "greenhouse.io" in host and parts:
        return clean_company_name(parts[0].replace("-", " "), preferred_role)
    if "lever.co" in host and parts:
        return clean_company_name(parts[0].replace("-", " "), preferred_role)
    if "ashbyhq.com" in host and parts:
        return clean_company_name(parts[0].replace("-", " "), preferred_role)
    if "smartrecruiters.com" in host and parts:
        return clean_company_name(parts[0].replace("-", " "), preferred_role)
    if "workatastartup.com" in host and "companies" in parts:
        idx = parts.index("companies")
        if len(parts) > idx + 1:
            return clean_company_name(parts[idx + 1].replace("-", " "), preferred_role)
    if "ycombinator.com" in host and "companies" in parts:
        idx = parts.index("companies")
        if len(parts) > idx + 1:
            return clean_company_name(parts[idx + 1].replace("-", " "), preferred_role)
    if "wellfound.com" in host and "company" in parts:
        idx = parts.index("company")
        if len(parts) > idx + 1:
            return clean_company_name(parts[idx + 1].replace("-", " "), preferred_role)
    if "myworkdayjobs.com" in host:
        label = host.split(".")[0]
        label = re.sub(r"wd\d*$", "", label)
        cleaned = clean_company_name(label.replace("-", " "), preferred_role)
        if cleaned:
            return cleaned

    labels = [label for label in host.split(".") if label and label not in GENERIC_DOMAIN_LABELS]
    if len(labels) >= 2:
        return clean_company_name(labels[-2].replace("-", " "), preferred_role)
    if labels:
        return clean_company_name(labels[0].replace("-", " "), preferred_role)
    return ""


def company_from_search_result(title: str, href: str, preferred_role: str = "") -> str:
    decoded_url = decode_duckduckgo_href(href)
    candidates: list[str] = []

    title = title or ""
    at_match = re.search(r"(?:\bat\b|@)\s+([A-Za-z0-9&.,' -]{2,60})", title)
    if at_match:
        candidates.append(at_match.group(1))

    hiring_match = re.search(r"^([A-Za-z0-9&.,' -]{2,60})\s+(?:is\s+)?hiring\b", title, flags=re.I)
    if hiring_match:
        candidates.append(hiring_match.group(1))

    for_match = re.search(r"\bfor\s+([A-Za-z0-9&.,' -]{2,60})$", title, flags=re.I)
    if for_match:
        candidates.append(for_match.group(1))

    for part in re.split(r"\s+[-|–—:·]\s+", title):
        candidates.append(part)

    domain_candidate = _company_from_domain(decoded_url, preferred_role)
    if domain_candidate:
        candidates.insert(0, domain_candidate)

    best = ""
    best_score = -999
    role_terms = set(_role_tokens(preferred_role))
    for candidate in candidates:
        cleaned = clean_company_name(candidate, preferred_role)
        if not cleaned:
            continue
        lower = cleaned.lower()
        score = 0
        if domain_candidate and lower == domain_candidate.lower():
            score += 4
        if any(term in lower.split() for term in role_terms):
            score -= 4
        if any(word in lower for word in GENERIC_COMPANY_WORDS):
            score -= 5
        if 1 <= len(cleaned.split()) <= 4:
            score += 1
        if score > best_score:
            best = cleaned
            best_score = score
    return best


def source_label_from_url(url: str) -> str:
    host = urlparse(url or "").netloc.lower().replace("www.", "")
    if any(source in host for source in ("workatastartup.com", "ycombinator.com")):
        return "yc-startup-open-role"
    if "wellfound.com" in host:
        return "wellfound-startup-open-role"
    if "startup.jobs" in host:
        return "startupjobs-open-role"
    if "simplify.jobs" in host:
        return "simplify-open-role"
    if "builtin.com" in host:
        return "builtin-open-role"
    if "themuse.com" in host:
        return "the-muse-open-role"
    if "greenhouse.io" in host:
        return "greenhouse-open-role"
    if "lever.co" in host:
        return "lever-open-role"
    if "ashbyhq.com" in host:
        return "ashby-open-role"
    if "smartrecruiters.com" in host:
        return "smartrecruiters-open-role"
    if "myworkdayjobs.com" in host:
        return "workday-open-role"
    return "role-web"


def discovery_score(title: str, snippet: str, source_url: str, preferred_role: str, source: str) -> int:
    text = f"{title} {snippet} {source_url}".lower()
    search_terms = [term.lower() for term in role_search_terms(preferred_role, limit=4)]
    token_terms = [tok for tok in _role_tokens(preferred_role) if tok not in {"intern", "new", "grad"}]
    family = role_family_for_role(preferred_role)
    family_terms = [term.lower() for term in ROLE_FAMILY_PROFILES[family]["signals"]]

    score = 0
    score += min(sum(1 for term in search_terms if term in text) * 9, 30)
    score += min(sum(1 for tok in token_terms if tok in text) * 3, 15)
    score += min(sum(1 for term in family_terms if term in text) * 2, 10)
    if any(word in text for word in ("intern", "internship", "new grad", "university", "campus")):
        score += 8
    if any(word in text for word in ("job", "jobs", "careers", "opening", "apply", "hiring")):
        score += 8
    if "open-role" in source:
        score += 12
    if any(startup_host in source_url for startup_host in STARTUP_SOURCE_HOSTS):
        score += 12
    if any(ats_host in source_url for ats_host in ATS_SOURCE_HOSTS):
        score += 8
    if any(noisy in text for noisy in ("salary", "glassdoor", "indeed", "linkedin")):
        score -= 8
    return score


def role_company_queries(
    preferred_role: str,
    location: str = "",
    *,
    include_startups: bool = True,
    only_open_roles: bool = True,
) -> list[str]:
    loc = (location or "United States Remote").strip()
    queries: list[str] = []

    for role in role_search_terms(preferred_role, limit=3):
        queries.extend(
            [
                f'site:greenhouse.io "{role}" "{loc}"',
                f'site:jobs.lever.co "{role}" "{loc}"',
                f'site:jobs.ashbyhq.com "{role}" "{loc}"',
                f'site:boards.greenhouse.io "{role}" "intern"',
                f'site:job-boards.greenhouse.io "{role}" "intern"',
                f'site:jobs.lever.co "{role}" "intern"',
                f'site:jobs.ashbyhq.com "{role}" "intern"',
                f'site:myworkdayjobs.com "{role}" "{loc}"',
                f'site:themuse.com/jobs "{role}" "{loc}"',
            ]
        )
        if include_startups:
            queries.extend(
                [
                    f'site:workatastartup.com/jobs "{role}"',
                    f'site:ycombinator.com/companies "{role}" "jobs"',
                    f'site:wellfound.com/jobs "{role}" "intern"',
                    f'site:startup.jobs "{role}" "intern"',
                    f'site:simplify.jobs "{role}" "intern"',
                    f'site:builtin.com/jobs "{role}" "{loc}"',
                    f'"{role}" startup internship jobs "{loc}"',
                ]
            )
        if not only_open_roles:
            queries.extend(
                [
                    f'"{role}" internship careers companies "{loc}"',
                    f'"{role}" "intern" "careers" "{loc}"',
                    f'"{role}" "new grad" "careers" "{loc}"',
                ]
            )

    unique_queries = []
    seen = set()
    max_queries = 18 if include_startups else 12
    for query in queries:
        key = query.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_queries.append(query)
        if len(unique_queries) >= max_queries:
            break
    return unique_queries


def _with_sponsorship(item: dict[str, Any]) -> dict[str, Any]:
    return {**item, **sponsorship_signal_for_company(item.get("company", ""))}


def _is_startup_item(item: dict[str, Any]) -> bool:
    return "startup" in str(item.get("source", "")).lower()


def _startup_count(items: list[dict[str, Any]]) -> int:
    return sum(1 for item in items if _is_startup_item(item))


def _ensure_startup_mix(items: list[dict[str, Any]], preferred_role: str, max_results: int, include_startups: bool) -> list[dict[str, Any]]:
    if not include_startups or max_results < 8:
        return items[:max_results]

    target_startups = min(5, max(3, max_results // 4))
    selected = items[:max_results]
    if _startup_count(selected) >= target_startups:
        return selected

    selected_keys = {company_key(item.get("company", "")) for item in selected}
    needed = target_startups - _startup_count(selected)
    additions = []
    for company in role_specific_startup_fallbacks(preferred_role):
        key = company_key(company)
        if key in selected_keys:
            continue
        selected_keys.add(key)
        additions.append(
            _with_sponsorship(
                {
                    "company": company,
                    "source": "startup-curated-verify-open-role",
                    "score": 18,
                    "primary_focus": ROLE_FAMILY_PROFILES[role_family_for_role(preferred_role)]["focus"],
                    "example_role": preferred_role,
                    "role_evidence": f"Startup target for {preferred_role or 'this role family'}; verify current opening from source URL/search.",
                    "snippet": "",
                    "discovery_query": preferred_role,
                    "source_url": f"https://www.google.com/search?q={quote_plus(company + ' ' + preferred_role + ' internship jobs')}",
                    "company_website": "",
                    "company_domain": "",
                }
            )
        )
        if len(additions) >= needed:
            break

    if not additions:
        return selected
    return (selected[: max_results - len(additions)] + additions)[:max_results]


def _blend_discovery_mix(items: list[dict[str, Any]], preferred_role: str, max_results: int, include_startups: bool) -> list[dict[str, Any]]:
    selected = _ensure_startup_mix(items, preferred_role, max_results, include_startups)
    if not include_startups:
        return selected[:max_results]

    startups = [item for item in selected if _is_startup_item(item)]
    others = [item for item in selected if not _is_startup_item(item)]
    if not startups:
        return selected[:max_results]

    result = []
    insertion_points = {1, 4, 7, 10, 13}
    promoted = startups[: min(len(startups), min(5, max(3, max_results // 4)))]
    remaining_startups = startups[len(promoted) :]

    while len(result) < max_results and (others or promoted or remaining_startups):
        if len(result) in insertion_points and promoted:
            result.append(promoted.pop(0))
            continue
        if others:
            result.append(others.pop(0))
            continue
        if promoted:
            result.append(promoted.pop(0))
            continue
        if remaining_startups:
            result.append(remaining_startups.pop(0))

    return result[:max_results]


def _strip_html(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    return re.sub(r"\s+", " ", text).strip()


def _unique_lower(values: list[str]) -> list[str]:
    seen = set()
    output = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def _muse_location_candidates(location: str) -> list[str]:
    raw = (location or "").strip()
    if not raw:
        return [""]

    candidates = [raw]
    if "," in raw:
        candidates.extend(part.strip() for part in raw.split(",") if part.strip())
    if "remote" in raw.lower():
        candidates.extend(["Remote", "United States", ""])
    else:
        candidates.append("")
    return _unique_lower([candidate for candidate in candidates if candidate or candidate == ""])


def _website_from_company_profile(company_profile: dict[str, Any]) -> str:
    refs = company_profile.get("refs") or {}
    for key in ("landing_page", "company_page"):
        value = str(refs.get(key) or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
    for key in ("website", "company_website"):
        value = str(company_profile.get(key) or "").strip()
        if value.startswith("http://") or value.startswith("https://"):
            return value
    return ""


def _domain_from_website(url: str) -> str:
    if not url:
        return ""
    host = urlparse(url).netloc.lower().replace("www.", "")
    return host.strip("/")


def _muse_signal_count(text: str, signals: tuple[str, ...]) -> int:
    return sum(1 for signal in signals if signal in text)


def _quick_muse_title_score(job: dict[str, Any], preferred_role: str) -> float:
    title = str(job.get("name") or "").lower()
    if not title:
        return 0.0
    if any(pattern.search(title) for pattern in HARD_REJECT_TITLE_PATTERNS):
        return 0.0

    search_terms = [term.lower() for term in role_search_terms(preferred_role, limit=4)]
    family = role_family_for_role(preferred_role)
    family_terms = ROLE_FAMILY_PROFILES[family]["signals"]

    score = 0.0
    preferred_phrase = (preferred_role or "").strip().lower()
    if preferred_phrase and preferred_phrase in title:
        score = max(score, 64.0)
    for idx, term in enumerate(search_terms):
        if term in title:
            score = max(score, 60.0 - (idx * 2))
    if _muse_signal_count(title, family_terms):
        score = max(score, 52.0)
    if "intern" in title or "internship" in title:
        score += 8.0
    if any(pattern.search(title) for pattern in NEGATIVE_TITLE_PATTERNS) and score < 70.0:
        score -= 30.0
    return score


def _score_muse_job(job: dict[str, Any], company_profile: dict[str, Any], preferred_role: str) -> tuple[float, str]:
    family = role_family_for_role(preferred_role)
    profile = ROLE_FAMILY_PROFILES[family]
    title = str(job.get("name") or "").strip()
    title_lower = title.lower()
    description = _strip_html(str(job.get("contents") or ""))
    description_lower = description.lower()
    categories = " ".join(str(item.get("name") or "") for item in (job.get("categories") or [])).lower()
    industries = " ".join(str(item.get("name") or "") for item in (company_profile.get("industries") or [])).lower()
    locations = " ".join(str(item.get("name") or "") for item in (job.get("locations") or [])).lower()
    combined = " ".join([title_lower, description_lower, categories, industries])

    score = _quick_muse_title_score(job, preferred_role)
    if score <= 0:
        return 0.0, ""

    search_terms = [term.lower() for term in role_search_terms(preferred_role, limit=4)]
    preferred_phrase = (preferred_role or "").strip().lower()
    if preferred_phrase and preferred_phrase in description_lower:
        score += 10.0
    score += min(sum(1 for term in search_terms if term in description_lower) * 6.0, 18.0)
    score += min(_muse_signal_count(combined, profile["signals"]) * 3.0, 18.0)

    if any(word in combined for word in ("intern", "internship", "summer 2026", "student", "campus")):
        score += 6.0
    if "remote" in locations:
        score += 3.0
    if any(pattern.search(title_lower) for pattern in NEGATIVE_TITLE_PATTERNS) and score < 78.0:
        score -= 22.0

    return score, profile["focus"]


def _notes_for_muse_job(job: dict[str, Any], company_profile: dict[str, Any], publication_date: str) -> str:
    industries = ", ".join(str(item.get("name") or "").strip() for item in (company_profile.get("industries") or []) if str(item.get("name") or "").strip())
    locations = ", ".join(str(item.get("name") or "").strip() for item in (job.get("locations") or []) if str(item.get("name") or "").strip())
    parts = [
        f"Discovered via The Muse on {publication_date}.",
        f"Example internship role: {str(job.get('name') or '').strip()}.",
    ]
    if locations:
        parts.append(f"Job locations: {locations}.")
    if industries:
        parts.append(f"Company industries: {industries}.")
    return " ".join(parts)


@dataclass
class MuseCandidate:
    company: str
    source: str
    score: int
    primary_focus: str
    example_role: str
    role_evidence: str
    snippet: str
    discovery_query: str
    source_url: str
    company_website: str
    company_domain: str

    def to_item(self) -> dict[str, Any]:
        item = {
            "company": self.company,
            "source": self.source,
            "score": self.score,
            "primary_focus": self.primary_focus,
            "example_role": self.example_role,
            "role_evidence": self.role_evidence,
            "snippet": self.snippet,
            "discovery_query": self.discovery_query,
            "source_url": self.source_url,
            "company_website": self.company_website,
            "company_domain": self.company_domain,
        }
        return _with_sponsorship(item)


def build_muse_candidate(job: dict[str, Any], company_profile: dict[str, Any], preferred_role: str, discovery_date: str) -> MuseCandidate | None:
    company_name = str((job.get("company") or {}).get("name") or company_profile.get("name") or "").strip()
    if not company_name:
        return None

    score, focus = _score_muse_job(job, company_profile, preferred_role)
    if score < 56.0:
        return None

    job_url = str((job.get("refs") or {}).get("landing_page") or "").strip()
    company_website = _website_from_company_profile(company_profile)
    source_url = job_url or company_website
    if not source_url:
        return None

    publication_date = str(job.get("publication_date") or "").strip()[:10] or discovery_date
    return MuseCandidate(
        company=company_name,
        source="the-muse-open-role",
        score=int(round(score)),
        primary_focus=focus,
        example_role=str(job.get("name") or "").strip(),
        role_evidence=str(job.get("name") or "").strip(),
        snippet=_notes_for_muse_job(job, company_profile, publication_date),
        discovery_query=preferred_role,
        source_url=source_url,
        company_website=company_website,
        company_domain=_domain_from_website(company_website),
    )


class MuseClient:
    BASE_URL = "https://www.themuse.com/api/public"

    def __init__(self, timeout_seconds: int = 20) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({"user-agent": "Mozilla/5.0"})
        self._company_cache: dict[int, dict[str, Any]] = {}

    def list_jobs(self, *, page: int, level: str = "Internship", location: str = "") -> dict[str, Any]:
        params: dict[str, Any] = {"page": page, "level": level}
        if location:
            params["location"] = location
        response = self.session.get(
            f"{self.BASE_URL}/jobs",
            params=params,
            timeout=self.timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    def get_company(self, company_id: int) -> dict[str, Any]:
        if company_id in self._company_cache:
            return self._company_cache[company_id]
        response = self.session.get(
            f"{self.BASE_URL}/companies/{company_id}",
            timeout=self.timeout_seconds,
        )
        if response.status_code == 404:
            payload: dict[str, Any] = {}
        else:
            response.raise_for_status()
            payload = response.json()
        self._company_cache[company_id] = payload
        return payload


def discover_companies_from_muse(preferred_role: str, location: str = "", max_results: int = 12, max_pages: int = 6) -> list[dict[str, Any]]:
    muse = MuseClient()
    discovery_date = datetime.now().date().isoformat()
    seen = set()
    discovered: list[dict[str, Any]] = []
    internal_target = max(max_results + 6, 14)

    for location_hint in _muse_location_candidates(location):
        for page in range(1, max_pages + 1):
            try:
                payload = muse.list_jobs(page=page, level="Internship", location=location_hint)
            except Exception:
                break

            jobs = payload.get("results") or []
            if not jobs:
                break

            page_candidates: list[dict[str, Any]] = []
            for job in jobs:
                if _quick_muse_title_score(job, preferred_role) < 46.0:
                    continue
                company_payload = job.get("company") or {}
                company_id = company_payload.get("id")
                if not company_id:
                    continue
                try:
                    company_profile = muse.get_company(int(company_id))
                except Exception:
                    company_profile = {}
                candidate = build_muse_candidate(job, company_profile, preferred_role, discovery_date)
                if not candidate:
                    continue
                key = company_key(candidate.company)
                if key in seen:
                    continue
                page_candidates.append(candidate.to_item())

            page_candidates.sort(key=lambda item: item.get("score", 0), reverse=True)
            for candidate in page_candidates:
                key = company_key(candidate.get("company", ""))
                if key in seen:
                    continue
                seen.add(key)
                discovered.append(candidate)
                if len(discovered) >= internal_target:
                    return discovered

        if len(discovered) >= max_results:
            break

    return discovered


def discover_companies_from_web(
    preferred_role: str,
    location: str = "",
    max_results: int = 12,
    *,
    include_startups: bool = True,
    only_open_roles: bool = True,
) -> list[dict[str, Any]]:
    by_key: dict[str, dict[str, Any]] = {}

    for item in discover_companies_from_muse(preferred_role, location, max_results=max_results):
        key = company_key(item.get("company", ""))
        existing = by_key.get(key)
        if not existing or item.get("score", 0) > existing.get("score", 0):
            by_key[key] = item

    for query in role_company_queries(
        preferred_role,
        location,
        include_startups=include_startups,
        only_open_roles=only_open_roles,
    ):
        html = _safe_get(f"https://duckduckgo.com/html/?q={quote_plus(query)}", timeout=10)
        if not html:
            continue

        soup = BeautifulSoup(html, "html.parser")
        for result in soup.select(".result")[: max_results * 5]:
            link = result.select_one("a.result__a")
            if not link:
                continue
            title = link.get_text(" ", strip=True)
            snippet_el = result.select_one(".result__snippet")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
            source_url = decode_duckduckgo_href(link.get("href", ""))
            company = company_from_search_result(title, source_url, preferred_role)
            if not company:
                continue

            source = source_label_from_url(source_url)
            item = {
                "company": company,
                "source": source,
                "score": discovery_score(title, snippet, source_url, preferred_role, source),
                "primary_focus": ROLE_FAMILY_PROFILES[role_family_for_role(preferred_role)]["focus"],
                "example_role": title,
                "role_evidence": title,
                "snippet": snippet,
                "discovery_query": query,
                "source_url": source_url,
                "company_website": "",
                "company_domain": _domain_from_website(source_url),
            }
            item = _with_sponsorship(item)
            key = company_key(company)
            existing = by_key.get(key)
            if not existing or item["score"] > existing.get("score", 0):
                by_key[key] = item

    if include_startups:
        for company in role_specific_startup_fallbacks(preferred_role):
            key = company_key(company)
            if key not in by_key:
                by_key[key] = _with_sponsorship(
                    {
                        "company": company,
                        "source": "startup-curated-verify-open-role",
                        "score": 18,
                        "primary_focus": ROLE_FAMILY_PROFILES[role_family_for_role(preferred_role)]["focus"],
                        "example_role": preferred_role,
                        "role_evidence": f"Startup target for {preferred_role or 'this role family'}; verify current opening from source URL/search.",
                        "snippet": "",
                        "discovery_query": preferred_role,
                        "source_url": f"https://www.google.com/search?q={quote_plus(company + ' ' + preferred_role + ' internship jobs')}",
                        "company_website": "",
                        "company_domain": "",
                    }
                )

    for company in role_specific_fallbacks(preferred_role):
        key = company_key(company)
        if key not in by_key:
            by_key[key] = _with_sponsorship(
                {
                    "company": company,
                    "source": "role-curated",
                    "score": 8,
                    "primary_focus": ROLE_FAMILY_PROFILES[role_family_for_role(preferred_role)]["focus"],
                    "example_role": preferred_role,
                    "role_evidence": "Curated fallback for this role family",
                    "snippet": "",
                    "discovery_query": preferred_role,
                    "source_url": "",
                    "company_website": "",
                    "company_domain": "",
                }
            )

    companies = sorted(by_key.values(), key=lambda item: item.get("score", 0), reverse=True)
    return _blend_discovery_mix(companies, preferred_role, max_results, include_startups)
