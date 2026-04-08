
from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from threading import Lock
from typing import Any, Dict, Iterable, List, Optional

import requests


class HunterAPIError(Exception):
    """Raised when the Hunter API returns an error or invalid response."""


@dataclass
class _CacheEntry:
    expires_at: float
    value: Any


class TTLCache:
    def __init__(self, ttl_seconds: int = 3600, max_items: int = 512):
        self.ttl_seconds = ttl_seconds
        self.max_items = max_items
        self._data: Dict[str, _CacheEntry] = {}
        self._lock = Lock()

    def _prune(self) -> None:
        now = time.time()
        expired = [k for k, v in self._data.items() if v.expires_at <= now]
        for k in expired:
            self._data.pop(k, None)

        if len(self._data) > self.max_items:
            overflow = len(self._data) - self.max_items
            for key in list(self._data.keys())[:overflow]:
                self._data.pop(key, None)

    def get(self, key: str) -> Any:
        with self._lock:
            entry = self._data.get(key)
            if not entry:
                return None
            if entry.expires_at <= time.time():
                self._data.pop(key, None)
                return None
            return entry.value

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._prune()
            self._data[key] = _CacheEntry(
                expires_at=time.time() + self.ttl_seconds,
                value=value,
            )

    def clear(self) -> None:
        with self._lock:
            self._data.clear()


class HunterClient:
    BASE_URL = "https://api.hunter.io/v2"

    # Hunter accepts only these seniority buckets.
    HUNTER_SENIORITY_VALUES = {"junior", "senior", "executive"}

    # App-friendly values mapped to Hunter-supported values.
    SENIORITY_MAP = {
        "intern": "junior",
        "entry": "junior",
        "junior": "junior",
        "associate": "junior",
        "manager": "senior",
        "lead": "senior",
        "senior": "senior",
        "director": "senior",
        "head": "executive",
        "vp": "executive",
        "vice president": "executive",
        "executive": "executive",
        "founder": "executive",
        "owner": "executive",
        "c_suite": "executive",
        "c-suite": "executive",
        "chief": "executive",
    }

    # Hunter department values are stricter than your UI labels.
    DEPARTMENT_MAP = {
        "hr": "hr",
        "human resources": "hr",
        "people": "hr",
        "recruiting": "hr",
        "talent acquisition": "hr",
        "operations": "management",
        "management": "management",
        "executive": "management",
        "engineering": "it",
        "it": "it",
        "finance": "finance",
        "legal": "legal",
        "support": "support",
        "sales": "sales",
        "marketing": "marketing",
        "communication": "communication",
    }

    DEFAULT_DEPARTMENTS = ["hr"]
    DEFAULT_VERIFICATION_STATUS = ("valid", "accept_all", "unknown")
    DEFAULT_REQUIRED_FIELDS = ("full_name", "position")

    def __init__(
        self,
        api_key: Optional[str] = None,
        timeout: int = 30,
        cache_ttl_seconds: int = 3600,
        max_cache_items: int = 512,
    ):
        self.api_key = api_key or os.getenv("HUNTER_API_KEY")
        self.timeout = timeout

        if not self.api_key:
            raise HunterAPIError("Missing HUNTER_API_KEY. Add it to your environment or .env file.")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Accept": "application/json",
                "X-API-KEY": self.api_key,
            }
        )
        self.cache = TTLCache(ttl_seconds=cache_ttl_seconds, max_items=max_cache_items)

    def clear_cache(self) -> None:
        self.cache.clear()

    def _cache_key(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]],
        json_body: Optional[Dict[str, Any]],
    ) -> str:
        payload = json.dumps(
            {
                "method": method,
                "endpoint": endpoint,
                "params": params or {},
                "json": json_body or {},
            },
            sort_keys=True,
            default=str,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        use_cache: bool = True,
        retries: int = 3,
    ) -> Dict[str, Any]:
        url = f"{self.BASE_URL}{endpoint}"
        cache_key = self._cache_key(method, endpoint, params, json_body)

        if use_cache:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached

        last_error: Optional[str] = None

        for attempt in range(retries + 1):
            try:
                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_body,
                    timeout=self.timeout,
                )
            except requests.RequestException as exc:
                last_error = f"Network error while calling Hunter: {exc}"
                if attempt < retries:
                    time.sleep(1.2 * (attempt + 1))
                    continue
                raise HunterAPIError(last_error) from exc

            if response.status_code in (429, 500, 502, 503, 504):
                last_error = f"Hunter temporary error {response.status_code}: {response.text[:300]}"
                if attempt < retries:
                    retry_after = response.headers.get("Retry-After")
                    sleep_for = float(retry_after) if retry_after and retry_after.isdigit() else 1.5 * (attempt + 1)
                    time.sleep(sleep_for)
                    continue

            if response.status_code == 401:
                raise HunterAPIError(
                    "Hunter rejected the API key (401). Confirm HUNTER_API_KEY is correct and restart Streamlit."
                )
            if response.status_code == 403:
                raise HunterAPIError(
                    "Hunter returned 403. Your key is valid but this request is not allowed on the current plan or account."
                )
            if response.status_code == 451:
                raise HunterAPIError(
                    "Hunter returned 451 for a claimed email/person. Do not process that contact further."
                )

            if not response.ok:
                try:
                    payload = response.json()
                except Exception:
                    payload = response.text[:500]
                raise HunterAPIError(f"Hunter API error {response.status_code}: {payload}")

            try:
                payload = response.json()
            except Exception as exc:
                raise HunterAPIError(f"Hunter returned non-JSON content: {response.text[:500]}") from exc

            if use_cache:
                self.cache.set(cache_key, payload)
            return payload

        raise HunterAPIError(last_error or "Unknown Hunter error.")

    @staticmethod
    def _join_csv(values: Optional[Iterable[str]]) -> Optional[str]:
        if not values:
            return None
        cleaned = [str(v).strip() for v in values if str(v).strip()]
        return ",".join(cleaned) if cleaned else None

    @classmethod
    def normalize_seniorities(cls, seniorities: Optional[Iterable[str]]) -> List[str]:
        if not seniorities:
            return []

        normalized: List[str] = []
        for value in seniorities:
            raw = str(value).strip().lower()
            if not raw:
                continue
            mapped = cls.SENIORITY_MAP.get(raw, raw)
            if mapped in cls.HUNTER_SENIORITY_VALUES and mapped not in normalized:
                normalized.append(mapped)
        return normalized

    @classmethod
    def normalize_departments(cls, departments: Optional[Iterable[str]]) -> List[str]:
        if not departments:
            return []

        normalized: List[str] = []
        for value in departments:
            raw = str(value).strip().lower()
            if not raw:
                continue
            mapped = cls.DEPARTMENT_MAP.get(raw)
            if mapped and mapped not in normalized:
                normalized.append(mapped)
        return normalized

    def domain_search(
        self,
        *,
        company: Optional[str] = None,
        domain: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        seniorities: Optional[Iterable[str]] = None,
        departments: Optional[Iterable[str]] = None,
        job_titles: Optional[Iterable[str]] = None,
        verification_status: Optional[Iterable[str]] = DEFAULT_VERIFICATION_STATUS,
        required_fields: Optional[Iterable[str]] = DEFAULT_REQUIRED_FIELDS,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        if not company and not domain:
            raise HunterAPIError("domain_search needs a company or domain.")

        params: Dict[str, Any] = {
            "limit": max(1, min(int(limit), 100)),
            "offset": max(0, int(offset)),
        }
        if company:
            params["company"] = company
        if domain:
            params["domain"] = domain

        normalized_seniorities = self.normalize_seniorities(seniorities)
        joined = self._join_csv(normalized_seniorities)
        if joined:
            params["seniority"] = joined

        normalized_departments = self.normalize_departments(departments)
        joined = self._join_csv(normalized_departments)
        if joined:
            params["department"] = joined

        joined = self._join_csv(job_titles)
        if joined:
            params["job_titles"] = joined

        joined = self._join_csv(verification_status)
        if joined:
            params["verification_status"] = joined

        joined = self._join_csv(required_fields)
        if joined:
            params["required_field"] = joined

        try:
            return self._request("GET", "/domain-search", params=params, use_cache=use_cache)
        except HunterAPIError as exc:
            # Graceful fallback when Hunter rejects strict filters.
            error_text = str(exc).lower()
            fallback_params = dict(params)
            changed = False

            if "invalid_seniority" in error_text and "seniority" in fallback_params:
                fallback_params.pop("seniority", None)
                changed = True

            if "invalid_department" in error_text and "department" in fallback_params:
                fallback_params.pop("department", None)
                changed = True

            if changed:
                return self._request("GET", "/domain-search", params=fallback_params, use_cache=use_cache)

            raise

    def email_finder(
        self,
        *,
        first_name: Optional[str] = None,
        last_name: Optional[str] = None,
        full_name: Optional[str] = None,
        company: Optional[str] = None,
        domain: Optional[str] = None,
        linkedin_handle: Optional[str] = None,
        max_duration: int = 8,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {"max_duration": max(3, min(int(max_duration), 20))}
        if first_name:
            params["first_name"] = first_name
        if last_name:
            params["last_name"] = last_name
        if full_name:
            params["full_name"] = full_name
        if company:
            params["company"] = company
        if domain:
            params["domain"] = domain
        if linkedin_handle:
            params["linkedin_handle"] = linkedin_handle

        return self._request("GET", "/email-finder", params=params, use_cache=use_cache)

    def email_verifier(self, email: str, *, use_cache: bool = True) -> Dict[str, Any]:
        clean_email = email.strip()
        if not clean_email:
            raise HunterAPIError("email_verifier needs an email address.")
        return self._request("GET", "/email-verifier", params={"email": clean_email}, use_cache=use_cache)

    def account_info(self) -> Dict[str, Any]:
        return self._request("GET", "/account", use_cache=True)

    @staticmethod
    def normalize_domain_search_results(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = payload.get("data") or {}
        company_domain = data.get("domain") or ""
        company_name = data.get("organization") or ""
        pattern = data.get("pattern") or ""

        results: List[Dict[str, Any]] = []
        for idx, row in enumerate(data.get("emails") or []):
            first_name = row.get("first_name") or ""
            last_name = row.get("last_name") or ""
            email = row.get("value") or ""
            full_name = (f"{first_name} {last_name}").strip() or email or f"lead-{idx + 1}"

            verification = row.get("verification") or {}
            linkedin_url = row.get("linkedin") or row.get("linkedin_url") or ""

            results.append(
                {
                    "hunter_person_id": f"{company_domain}:{email or idx}",
                    "first_name": first_name,
                    "last_name": last_name,
                    "name": full_name,
                    "title": row.get("position") or row.get("position_raw") or "",
                    "email": email,
                    "email_type": row.get("type") or "",
                    "confidence": int(row.get("confidence") or 0),
                    "linkedin_url": linkedin_url,
                    "twitter": row.get("twitter") or "",
                    "phone_number": row.get("phone_number") or "",
                    "company_name": company_name,
                    "company_domain": company_domain,
                    "email_pattern": pattern,
                    "seniority": row.get("seniority") or "",
                    "department": row.get("department") or "",
                    "verification_status": verification.get("status") or "unknown",
                    "verification_date": verification.get("date") or "",
                    "sources": row.get("sources") or [],
                    "source_count": len(row.get("sources") or []),
                }
            )

        return results

    @staticmethod
    def score_contact(
        contact: Dict[str, Any],
        preferred_role: str = "",
        target_titles: Optional[Iterable[str]] = None,
    ) -> int:
        score = 0
        title = (contact.get("title") or "").lower()
        department = (contact.get("department") or "").lower()
        seniority = (contact.get("seniority") or "").lower()
        verification_status = (contact.get("verification_status") or "").lower()
        email_type = (contact.get("email_type") or "").lower()
        source_count = int(contact.get("source_count") or 0)
        confidence = int(contact.get("confidence") or 0)
        preferred_role_l = (preferred_role or "").lower()

        if any(k in title for k in ["campus recruiter", "university recruiter"]):
            score += 42
        if "recruiter" in title and "campus" not in title and "university" not in title:
            score += 34
        if any(k in title for k in ["talent acquisition", "talent partner", "people partner"]):
            score += 30
        if "hiring manager" in title:
            score += 28
        if any(k in title for k in ["analytics manager", "data science manager", "data manager", "engineering manager"]):
            score += 20

        if department in {"hr", "recruiting", "people"}:
            score += 10
        if seniority in {"manager", "director", "senior"}:
            score += 6
        if seniority in {"executive", "c_suite", "owner", "founder", "vp"}:
            score -= 18

        if email_type == "personal":
            score += 10

        if verification_status == "valid":
            score += 18
        elif verification_status == "accept_all":
            score += 8
        elif verification_status == "unknown":
            score -= 3

        score += min(confidence // 5, 18)
        score += min(source_count * 2, 10)

        if target_titles:
            target_titles_l = [str(t).strip().lower() for t in target_titles if str(t).strip()]
            if any(t in title for t in target_titles_l):
                score += 10

        if any(k in preferred_role_l for k in ["intern", "new grad", "entry"]) and any(k in title for k in ["campus", "university"]):
            score += 8

        if not title:
            score -= 10
        if any(x in title for x in ["sales", "account executive", "customer success"]):
            score -= 20

        return score

    def search_people_low_credit(
        self,
        *,
        company_name: str,
        preferred_role: str,
        titles: Iterable[str],
        company_domain: Optional[str] = None,
        seniorities: Optional[Iterable[str]] = None,
        per_page: int = 8,
    ) -> List[Dict[str, Any]]:
        title_list = [str(t).strip() for t in titles if str(t).strip()]
        limit = min(max(int(per_page), 1), 10)

        all_rows: List[Dict[str, Any]] = []

        if title_list:
            for title in title_list[:5]:
                payload = self.domain_search(
                    company=company_name,
                    domain=company_domain,
                    limit=limit,
                    seniorities=seniorities,
                    departments=self.DEFAULT_DEPARTMENTS,
                    job_titles=[title],
                    use_cache=True,
                )
                all_rows.extend(self.normalize_domain_search_results(payload))
        else:
            payload = self.domain_search(
                company=company_name,
                domain=company_domain,
                limit=limit,
                seniorities=seniorities,
                departments=self.DEFAULT_DEPARTMENTS,
                use_cache=True,
            )
            all_rows.extend(self.normalize_domain_search_results(payload))

        deduped: Dict[str, Dict[str, Any]] = {}
        for row in all_rows:
            key = row.get("email") or row.get("hunter_person_id") or row.get("name")
            existing = deduped.get(key)
            if existing is None:
                deduped[key] = row
                continue

            current_richness = (
                len(row.get("sources") or []),
                int(row.get("confidence") or 0),
                len(row.get("title") or ""),
            )
            existing_richness = (
                len(existing.get("sources") or []),
                int(existing.get("confidence") or 0),
                len(existing.get("title") or ""),
            )
            if current_richness > existing_richness:
                deduped[key] = row

        ranked = list(deduped.values())
        for row in ranked:
            row["local_score"] = self.score_contact(
                row,
                preferred_role=preferred_role,
                target_titles=titles,
            )

        ranked.sort(
            key=lambda r: (
                r.get("local_score") or 0,
                r.get("confidence") or 0,
                r.get("source_count") or 0,
            ),
            reverse=True,
        )
        return ranked[:limit]

    def verify_selected_contacts(
        self,
        contacts: List[Dict[str, Any]],
        max_verifications: int = 2,
    ) -> List[Dict[str, Any]]:
        verified: List[Dict[str, Any]] = []

        for contact in contacts[: max(0, int(max_verifications))]:
            email = (contact.get("email") or "").strip()
            if not email:
                verified.append(dict(contact))
                continue

            payload = self.email_verifier(email)
            data = payload.get("data") or {}

            merged = dict(contact)
            merged["verification_status"] = data.get("status") or merged.get("verification_status") or "unknown"
            merged["verification_result"] = data.get("result") or ""
            merged["verification_score"] = data.get("score") or ""
            merged["accept_all"] = data.get("accept_all") or False
            verified.append(merged)

        return verified

    @staticmethod
    def pick_best_contact(contacts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not contacts:
            return None
        ranked = sorted(
            contacts,
            key=lambda r: (
                r.get("local_score") or 0,
                r.get("confidence") or 0,
                r.get("source_count") or 0,
            ),
            reverse=True,
        )
        return ranked[0]
