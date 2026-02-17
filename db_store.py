import uuid
from typing import Dict

import pandas as pd

from db import get_conn

TEXT_COLS = [
    "Date Applied",
    "Company",
    "Role",
    "Job Link",
    "Location",
    "Status",
    "Follow-up Date",
    "Contact Name",
    "Contact Link",
    "Notes",
]


def init_db() -> None:
    """
    Creates the applications table (and a safe users table if it doesn't exist).
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            # Users table (auth.py also creates it, but this avoids admin queries failing)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id TEXT PRIMARY KEY,
                    password_hash BYTEA NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            # Applications table
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
                    "Follow-up Date" TEXT,
                    "Contact Name" TEXT,
                    "Contact Link" TEXT,
                    "Notes" TEXT,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
                """
            )

            cur.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON applications(user_id);")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON applications(created_at);")
        conn.commit()


def _normalize_df(df: pd.DataFrame) -> pd.DataFrame:
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
            df.loc[missing, "_row_id"] = [str(uuid.uuid4()) for _ in range(missing.sum())]
    return df


def read_tracker_df(user_id: str) -> pd.DataFrame:
    init_db()

    query = """
        SELECT
            _row_id,
            "Date Applied","Company","Role","Job Link","Location","Status",
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
            "Follow-up Date","Contact Name","Contact Link","Notes"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    values["Follow-up Date"],
                    values["Contact Name"],
                    values["Contact Link"],
                    values["Notes"],
                ),
            )
        conn.commit()


def overwrite_tracker_for_user(user_id: str, df: pd.DataFrame) -> None:
    init_db()
    df = ensure_row_ids(_normalize_df(df))

    insert_sql = """
        INSERT INTO applications (
            _row_id, user_id,
            "Date Applied","Company","Role","Job Link","Location","Status",
            "Follow-up Date","Contact Name","Contact Link","Notes"
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                        str(r["Follow-up Date"] or ""),
                        str(r["Contact Name"] or ""),
                        str(r["Contact Link"] or ""),
                        str(r["Notes"] or ""),
                    )
                )

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
    init_db()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM applications WHERE user_id = %s", (user_id,))
        conn.commit()


def admin_list_users() -> pd.DataFrame:
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