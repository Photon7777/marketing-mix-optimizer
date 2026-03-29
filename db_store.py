# db_store.py
import uuid
from typing import Dict

import pandas as pd

from db import get_conn

# Columns used by the tracker UI
TEXT_COLS = [
    "Date Applied",
    "Company",
    "Role",
    "Job Link",
    "Location",
    "Status",
    "Fit Score",
    "Priority",
    "Fit Summary",
    "Follow-up Date",
    "Contact Name",
    "Contact Link",
    "Notes",
]


def init_db() -> None:
    """
    Creates tables used by the app (applications + resumes).
    NOTE: users table is created by auth.py, but we keep admin_list_users resilient.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Applications tracker
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS applications (
                    _row_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    "Date Applied" TEXT,
                    "Company" TEXT,
                    "Role" TEXT,
                    "Job Link" TEXT,
                    "Location" TEXT,
                    "Status" TEXT,
                    "Fit Score" TEXT,
                    "Priority" TEXT,
                    "Fit Summary" TEXT,
                    "Follow-up Date" TEXT,
                    "Contact Name" TEXT,
                    "Contact Link" TEXT,
                    "Notes" TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_user_id ON applications(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_app_created_at ON applications(created_at);")

            # Safe migrations for older DBs
            cur.execute('ALTER TABLE applications ADD COLUMN IF NOT EXISTS "Fit Score" TEXT;')
            cur.execute('ALTER TABLE applications ADD COLUMN IF NOT EXISTS "Priority" TEXT;')
            cur.execute('ALTER TABLE applications ADD COLUMN IF NOT EXISTS "Fit Summary" TEXT;')

            # Resume versions library
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS resumes (
                    resume_id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    resume_text TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )
            cur.execute("CREATE INDEX IF NOT EXISTS idx_res_user_id ON resumes(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_res_created_at ON resumes(created_at);")

        conn.commit()


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures TEXT_COLS exist and are strings, and keeps only the expected columns.
    """
    df = df.copy()

    for c in TEXT_COLS:
        if c not in df.columns:
            df[c] = ""

    for c in TEXT_COLS:
        df[c] = df[c].fillna("").astype(str)

    keep = ["_row_id"] + TEXT_COLS
    for c in keep:
        if c not in df.columns:
            df[c] = ""
    return df[keep]


def ensure_row_ids(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "_row_id" not in df.columns:
        df.insert(0, "_row_id", [str(uuid.uuid4()) for _ in range(len(df))])
    else:
        df["_row_id"] = df["_row_id"].fillna("").astype(str)
        missing = df["_row_id"].str.strip() == ""
        if missing.any():
            df.loc[missing, "_row_id"] = [str(uuid.uuid4()) for _ in range(int(missing.sum()))]
    return df


# -------------------------
# TRACKER FUNCTIONS
# -------------------------
def read_tracker_df(user_id: str) -> pd.DataFrame:
    init_db()

    query = """
        SELECT
            _row_id,
            "Date Applied","Company","Role","Job Link","Location","Status",
            "Fit Score","Priority","Fit Summary",
            "Follow-up Date","Contact Name","Contact Link","Notes"
        FROM applications
        WHERE user_id = %s
        ORDER BY created_at DESC
    """

    with get_conn() as conn:
        df = pd.read_sql_query(query, conn, params=(user_id,))

    if df.empty:
        return pd.DataFrame(columns=["_row_id"] + TEXT_COLS)

    df = _normalize_df(df)
    df = ensure_row_ids(df)
    return df


def append_row(user_id: str, row: Dict) -> None:
    init_db()
    row_id = str(uuid.uuid4())
    values = {c: str(row.get(c, "") or "") for c in TEXT_COLS}

    sql = """
        INSERT INTO applications (
            _row_id, user_id,
            "Date Applied","Company","Role","Job Link","Location","Status",
            "Fit Score","Priority","Fit Summary",
            "Follow-up Date","Contact Name","Contact Link","Notes"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    row_id,
                    user_id,
                    values["Date Applied"],
                    values["Company"],
                    values["Role"],
                    values["Job Link"],
                    values["Location"],
                    values["Status"],
                    values["Fit Score"],
                    values["Priority"],
                    values["Fit Summary"],
                    values["Follow-up Date"],
                    values["Contact Name"],
                    values["Contact Link"],
                    values["Notes"],
                ),
            )
        conn.commit()


def overwrite_tracker_for_user(user_id: str, df: pd.DataFrame) -> None:
    """
    Replace the user's entire tracker with rows in df.
    df should contain _row_id + TEXT_COLS (we'll coerce if needed).
    """
    init_db()
    df = ensure_row_ids(_normalize_df(df))

    insert_sql = """
        INSERT INTO applications (
            _row_id, user_id,
            "Date Applied","Company","Role","Job Link","Location","Status",
            "Fit Score","Priority","Fit Summary",
            "Follow-up Date","Contact Name","Contact Link","Notes"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM applications WHERE user_id = %s", (user_id,))

            data = []
            for _, r in df.iterrows():
                data.append(
                    (
                        str(r["_row_id"]),
                        user_id,
                        str(r["Date Applied"] or ""),
                        str(r["Company"] or ""),
                        str(r["Role"] or ""),
                        str(r["Job Link"] or ""),
                        str(r["Location"] or ""),
                        str(r["Status"] or ""),
                        str(r["Fit Score"] or ""),
                        str(r["Priority"] or ""),
                        str(r["Fit Summary"] or ""),
                        str(r["Follow-up Date"] or ""),
                        str(r["Contact Name"] or ""),
                        str(r["Contact Link"] or ""),
                        str(r["Notes"] or ""),
                    )
                )

            if data:
                cur.executemany(insert_sql, data)

        conn.commit()


def merge_uploaded_csv(user_id: str, uploaded_df: pd.DataFrame, dedupe: bool = True) -> None:
    existing = read_tracker_df(user_id)
    uploaded_df = ensure_row_ids(_normalize_df(uploaded_df))
    combined = pd.concat([existing, uploaded_df], ignore_index=True)

    if dedupe:
        key_cols = ["Company", "Role", "Job Link", "Date Applied"]
        for c in key_cols:
            if c not in combined.columns:
                combined[c] = ""
        combined = combined.drop_duplicates(subset=key_cols, keep="first")

    overwrite_tracker_for_user(user_id, combined)


def replace_with_uploaded_csv(user_id: str, uploaded_df: pd.DataFrame) -> None:
    uploaded_df = ensure_row_ids(_normalize_df(uploaded_df))
    overwrite_tracker_for_user(user_id, uploaded_df)


def delete_all_for_user(user_id: str) -> None:
    """
    Deletes ALL tracker entries and resume versions for this user.
    """
    init_db()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM applications WHERE user_id = %s", (user_id,))
            cur.execute("DELETE FROM resumes WHERE user_id = %s", (user_id,))
        conn.commit()


def admin_list_users() -> pd.DataFrame:
    """
    Admin-only: list users + created_at + job counts.
    Assumes users table exists (created by auth.py). If not, returns empty.
    """
    init_db()
    query = """
        SELECT
            u.user_id,
            u.created_at,
            COUNT(a._row_id) AS jobs_count
        FROM users u
        LEFT JOIN applications a ON a.user_id = u.user_id
        GROUP BY u.user_id, u.created_at
        ORDER BY jobs_count DESC, u.user_id ASC
    """

    with get_conn() as conn:
        try:
            return pd.read_sql_query(query, conn)
        except Exception:
            return pd.DataFrame(columns=["user_id", "created_at", "jobs_count"])


# -------------------------
# RESUME LIBRARY FUNCTIONS
# -------------------------
def save_resume_version(user_id: str, name: str, resume_text: str) -> None:
    init_db()
    resume_id = str(uuid.uuid4())
    name = (name or "").strip() or "Resume"
    resume_text = (resume_text or "").strip()

    if not resume_text:
        raise ValueError("Resume text is empty.")

    sql = """
        INSERT INTO resumes (resume_id, user_id, name, resume_text)
        VALUES (%s, %s, %s, %s)
    """

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (resume_id, user_id, name, resume_text))
        conn.commit()


def list_resumes(user_id: str) -> pd.DataFrame:
    init_db()
    query = """
        SELECT resume_id, name, created_at
        FROM resumes
        WHERE user_id = %s
        ORDER BY created_at DESC
    """
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn, params=(user_id,))
    return df


def load_resume_text(user_id: str, resume_id: str) -> str:
    init_db()
    sql = """
        SELECT resume_text
        FROM resumes
        WHERE user_id = %s AND resume_id = %s
        LIMIT 1
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, resume_id))
            row = cur.fetchone()
    if not row:
        raise ValueError("Resume not found.")
    return row[0]


def list_resume_payloads(user_id: str) -> list[dict]:
    init_db()
    query = """
        SELECT resume_id, name, resume_text, created_at
        FROM resumes
        WHERE user_id = %s
        ORDER BY created_at DESC
    """
    with get_conn() as conn:
        df = pd.read_sql_query(query, conn, params=(user_id,))
    if df.empty:
        return []
    return df.fillna("").to_dict(orient="records")


def delete_resume_version(user_id: str, resume_id: str) -> None:
    init_db()
    sql = "DELETE FROM resumes WHERE user_id = %s AND resume_id = %s"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (user_id, resume_id))
        conn.commit()