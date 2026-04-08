import re
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

ROLE_FALLBACKS = [
    (
        ("data engineer", "data engineering", "etl", "analytics engineer"),
        ["Databricks", "Snowflake", "Capital One", "Bloomberg", "DoorDash", "Stripe", "Visa", "ServiceNow"],
    ),
    (
        ("data scientist", "machine learning", "ml", "ai engineer", "research scientist"),
        ["Databricks", "NVIDIA", "Meta", "Adobe", "Spotify", "Uber", "Airbnb", "Instacart"],
    ),
    (
        ("data analyst", "business analyst", "analytics", "bi analyst", "business intelligence"),
        ["Capital One", "Visa", "Bloomberg", "Deloitte", "Accenture", "Adobe", "Amazon", "American Express"],
    ),
    (
        ("software", "backend", "frontend", "full stack", "swe", "developer"),
        ["Microsoft", "Amazon", "ServiceNow", "Cisco", "Datadog", "Stripe", "Atlassian", "Adobe"],
    ),
    (
        ("product", "product analyst", "product manager", "apm"),
        ["Microsoft", "Adobe", "Intuit", "Atlassian", "ServiceNow", "Capital One", "Visa", "Datadog"],
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
]

STARTUP_ROLE_FALLBACKS = [
    (
        ("data engineer", "data engineering", "etl", "analytics engineer"),
        ["Fivetran", "dbt Labs", "Hightouch", "Census", "MotherDuck", "Monte Carlo", "Hex", "Airbyte", "Prefect", "ClickHouse"],
    ),
    (
        ("data scientist", "machine learning", "ml", "ai engineer", "research scientist"),
        ["Scale AI", "Hugging Face", "Weights & Biases", "Pinecone", "Anyscale", "Cohere", "Perplexity AI", "Runway", "Baseten", "Anthropic"],
    ),
    (
        ("data analyst", "business analyst", "analytics", "bi analyst", "business intelligence"),
        ["Ramp", "Brex", "Mercury", "Modern Treasury", "Plaid", "Hightouch", "Figma", "Notion", "Rippling", "Gusto"],
    ),
    (
        ("software", "backend", "frontend", "full stack", "swe", "developer"),
        ["Rippling", "Figma", "Notion", "Ramp", "Vercel", "Linear", "Retool", "Supabase", "Vanta", "Cursor"],
    ),
    (
        ("product", "product analyst", "product manager", "apm"),
        ["Ramp", "Notion", "Figma", "Linear", "Airtable", "Retool", "Rippling", "Vanta", "Mercury", "Brex"],
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


def _safe_get(url: str, timeout: int = 10) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.ok:
            return resp.text[:160000]
    except Exception:
        return ""
    return ""


def company_key(company: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (company or "").lower())


def sponsorship_signal_for_company(company: str) -> dict:
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


def clean_company_name(value: str, preferred_role: str = "") -> str:
    text = re.sub(r"<[^>]+>", " ", value or "")
    text = re.sub(r"https?://\S+", " ", text)
    text = text.replace("&amp;", "&")
    text = re.sub(r"\s+", " ", text).strip(" -|:,.")

    cleanup_patterns = [
        r"\b(careers?|jobs?|job openings?|open roles?|hiring|apply now)\b",
        r"\b(internships?|intern|new grad|students?|university|campus)\b",
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
        if lower == domain_candidate.lower():
            score += 4
        if any(term in lower.split() for term in role_terms):
            score -= 4
        if any(word in lower for word in GENERIC_COMPANY_WORDS):
            score -= 5
        if 2 <= len(cleaned.split()) <= 4:
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
    role_tokens = [tok for tok in _role_tokens(preferred_role) if tok not in {"intern", "new", "grad"}]
    score = 0
    score += min(sum(1 for tok in role_tokens if tok in text) * 6, 24)
    if any(word in text for word in ("intern", "internship", "new grad", "university", "campus")):
        score += 8
    if any(word in text for word in ("job", "jobs", "careers", "opening", "apply", "hiring")):
        score += 8
    if "open-role" in source:
        score += 12
    if any(startup_host in source_url for startup_host in STARTUP_SOURCE_HOSTS):
        score += 10
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
    role = (preferred_role or "internship").strip()
    loc = (location or "United States Remote").strip()

    queries = [
        f'site:greenhouse.io "{role}" "{loc}"',
        f'site:jobs.lever.co "{role}" "{loc}"',
        f'site:jobs.ashbyhq.com "{role}" "{loc}"',
        f'site:boards.greenhouse.io "{role}" "intern"',
        f'site:job-boards.greenhouse.io "{role}" "intern"',
        f'site:jobs.lever.co "{role}" "intern"',
        f'site:jobs.ashbyhq.com "{role}" "intern"',
    ]

    if include_startups:
        queries.extend(
            [
                f'site:workatastartup.com/jobs "{role}"',
                f'site:ycombinator.com/companies "{role}" "jobs"',
                f'site:wellfound.com/jobs "{role}" "intern"',
                f'site:startup.jobs "{role}" "intern"',
                f'site:simplify.jobs "{role}" "intern"',
                f'site:builtin.com/jobs "{role}" "{loc}"',
                f'"{role}" "startup" "intern" "hiring" "{loc}"',
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

    return queries


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


def _with_sponsorship(item: dict) -> dict:
    return {**item, **sponsorship_signal_for_company(item.get("company", ""))}


def _startup_count(items: list[dict]) -> int:
    return sum(1 for item in items if "startup" in str(item.get("source", "")).lower())


def _ensure_startup_mix(items: list[dict], preferred_role: str, max_results: int, include_startups: bool) -> list[dict]:
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
                    "score": 2,
                    "role_evidence": "Startup target for this role family; verify current opening from source URL/search.",
                    "snippet": "",
                    "discovery_query": preferred_role,
                    "source_url": f"https://www.google.com/search?q={quote_plus(company + ' ' + preferred_role + ' internship jobs')}",
                }
            )
        )
        if len(additions) >= needed:
            break

    if not additions:
        return selected
    return (selected[: max_results - len(additions)] + additions)[:max_results]


def discover_companies_from_web(
    preferred_role: str,
    location: str = "",
    max_results: int = 12,
    *,
    include_startups: bool = True,
    only_open_roles: bool = True,
) -> list[dict]:
    companies: list[dict] = []
    by_key: dict[str, dict] = {}

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
        for result in soup.select(".result")[: max_results * 4]:
            a = result.select_one("a.result__a")
            if not a:
                continue
            title = a.get_text(" ", strip=True)
            snippet_el = result.select_one(".result__snippet")
            snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
            source_url = decode_duckduckgo_href(a.get("href", ""))
            company = company_from_search_result(title, source_url, preferred_role)
            if not company:
                continue
            key = re.sub(r"[^a-z0-9]+", "", company.lower())
            source = source_label_from_url(source_url)
            score = discovery_score(title, snippet, source_url, preferred_role, source)
            item = {
                "company": company,
                "source": source,
                "score": score,
                "role_evidence": title,
                "snippet": snippet,
                "discovery_query": query,
                "source_url": source_url,
            }
            item = _with_sponsorship(item)
            existing = by_key.get(key)
            if not existing or item["score"] > existing.get("score", 0):
                by_key[key] = item

    if include_startups:
        for company in role_specific_startup_fallbacks(preferred_role):
            key = re.sub(r"[^a-z0-9]+", "", company.lower())
            if key not in by_key:
                by_key[key] = _with_sponsorship(
                    {
                        "company": company,
                        "source": "startup-curated-verify-open-role",
                        "score": 2,
                        "role_evidence": "Startup target for this role family; verify current opening from source URL/search.",
                        "snippet": "",
                        "discovery_query": preferred_role,
                        "source_url": f"https://www.google.com/search?q={quote_plus(company + ' ' + preferred_role + ' internship jobs')}",
                    }
                )

    for company in role_specific_fallbacks(preferred_role):
        key = re.sub(r"[^a-z0-9]+", "", company.lower())
        if key not in by_key:
            by_key[key] = _with_sponsorship(
                {
                    "company": company,
                    "source": "role-curated",
                    "score": 1,
                    "role_evidence": "Curated fallback for this role family",
                    "snippet": "",
                    "discovery_query": preferred_role,
                    "source_url": "",
                }
            )

    companies = sorted(by_key.values(), key=lambda item: item.get("score", 0), reverse=True)
    return _ensure_startup_mix(companies, preferred_role, max_results, include_startups)
