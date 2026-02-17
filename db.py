import os
from urllib.parse import urlparse

import streamlit as st

# Use st.secrets on Streamlit Cloud, fallback to env var locally
def get_database_url() -> str:
    url = None
    try:
        url = st.secrets.get("DATABASE_URL")
    except Exception:
        pass

    if not url:
        url = os.getenv("DATABASE_URL")

    if not url:
        raise RuntimeError(
            "DATABASE_URL is not set. Add it to Streamlit Secrets (preferred) or as an env var."
        )
    return url


def get_conn():
    """
    Returns a live DB connection to Postgres (Neon).
    Requires: psycopg2-binary
    """
    import psycopg2  # local import to keep module import clean

    db_url = get_database_url()

    # Neon URLs are usually already correct. If you ever see 'postgres://' convert:
    if db_url.startswith("postgres://"):
        db_url = db_url.replace("postgres://", "postgresql://", 1)

    return psycopg2.connect(db_url, connect_timeout=10)