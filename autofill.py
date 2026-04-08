import json
import os
from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
_OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
client = OpenAI(api_key=_OPENAI_API_KEY) if _OPENAI_API_KEY else None
AUTOFILL_MODEL = os.getenv("OPENAI_AUTOFILL_MODEL") or os.getenv("OPENAI_MODEL", "gpt-4o-mini")

AUTOFILL_SYSTEM = """Extract tracker fields from a job post.
Return ONLY valid JSON with these exact keys:
company, role, location

Rules:
- If unknown, use empty string.
- Keep values short (no paragraphs).
"""

def extract_fields(job_post_text: str) -> dict:
    if client is None:
        raise RuntimeError("OPENAI_API_KEY is not set.")

    resp = client.chat.completions.create(
        model=AUTOFILL_MODEL,
        messages=[
            {"role": "system", "content": AUTOFILL_SYSTEM},
            {"role": "user", "content": job_post_text[:20000]},
        ],
        temperature=0,
    )
    raw = resp.choices[0].message.content.strip()

    # Safe-ish parse: if model returns extra text, try to isolate JSON
    try:
        return json.loads(raw)
    except Exception:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            return json.loads(raw[start : end + 1])
        return {"company": "", "role": "", "location": ""}
