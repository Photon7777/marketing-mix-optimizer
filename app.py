import base64
import io
import os
import re
import mimetypes
from html import escape
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests
from datetime import date, timedelta, datetime

import pandas as pd
from dotenv import load_dotenv
import streamlit as st
import altair as alt
from pypdf import PdfReader
from docx import Document
from autofill import extract_fields

from tools import (
    fetch_job_post,
    safe_clip,
    analyze_fit,
    recommend_follow_up_action,
    rank_resume_versions,
)
from agent import (
    generate_application_materials,
    generate_outreach_materials,
    generate_outreach_next_step,
)
from company_discovery import company_key, discover_companies_from_web, sponsorship_signal_for_company
from dashboard_metrics import (
    FOLLOW_UP_BUCKET_COLORS,
    FOLLOW_UP_BUCKET_ORDER,
    FUNNEL_STAGE_ORDER,
    STATUS_COLOR_DOMAIN,
    STATUS_COLOR_RANGE,
    STATUS_DISPLAY_ORDER,
    build_fit_outcome_df,
    build_follow_up_timeline_df,
    build_pipeline_funnel_df,
    build_resume_performance_df,
    build_weekly_activity_df,
    build_weekly_momentum_summary,
)
from outreach_guardrails import (
    build_outreach_tracker_row,
    classify_outreach_thread,
    contact_identity_keys,
    fallback_outreach_next_step,
    filter_new_contacts,
    is_duplicate_contact,
    normalize_outreach_email,
    tracker_outreach_history,
    unique_send_rows,
)

from db_store import (
    append_row,
    append_outreach_row,
    read_tracker_df,
    read_outreach_tracker_df,
    ensure_row_ids,
    overwrite_tracker_for_user,
    merge_uploaded_csv,
    replace_with_uploaded_csv,
    merge_visible_tracker_edits,
    delete_all_for_user,
    admin_list_users,
    save_resume_version,
    list_resumes,
    load_resume_payload,
    delete_resume_version,
    list_resume_payloads,
    update_outreach_row,
)

from auth import (
    clear_user_gmail_connection,
    clear_user_gmail_oauth_state,
    create_user,
    gmail_oauth_state_matches,
    verify_user,
    change_password,
    delete_user,
    get_user_gmail_connection,
    get_user_gmail_oauth_state,
    get_user_profile,
    save_user_gmail_oauth_state,
    update_user_profile,
)

from hunter_helper import HunterClient
from gmail_sender import GmailSendError, GmailSender

load_dotenv()

MAX_EMAILS_PER_BATCH = int(os.getenv("MAX_EMAILS_PER_BATCH", "25"))
APP_DIR = Path(__file__).resolve().parent
LOGO_PATH = APP_DIR / "assets" / "nextrole-logo.svg"


def file_to_data_uri(path: Path, mime_type: str) -> str:
    try:
        return f"data:{mime_type};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"
    except Exception:
        return ""


LOGO_DATA_URI = file_to_data_uri(LOGO_PATH, "image/svg+xml") if LOGO_PATH.exists() else ""
PAGE_ICON = str(LOGO_PATH) if LOGO_PATH.exists() else "🎯"
LOGO_MARKUP = (
    f'<img src="{LOGO_DATA_URI}" alt="NextRole logo" class="brand-logo" />'
    if LOGO_DATA_URI
    else '<span class="brand-fallback">NR</span>'
)

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(
    page_title="NextRole",
    page_icon=PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded",
)

# -------------------------
# GLOBAL STYLES
# -------------------------
st.markdown(
    """
    <style>
      :root {
        --bg-0: #030816;
        --bg-1: #071527;
        --bg-2: #0a1c33;
        --panel: rgba(9, 20, 38, 0.78);
        --panel-strong: rgba(11, 24, 44, 0.92);
        --panel-soft: rgba(255,255,255,0.045);
        --stroke: rgba(148, 163, 184, 0.16);
        --stroke-strong: rgba(148, 163, 184, 0.24);
        --text-main: #f8fafc;
        --text-muted: rgba(226, 232, 240, 0.74);
        --accent: #7c8cff;
        --accent-2: #42c8ff;
        --accent-3: #38d7c1;
        --shadow-lg: 0 24px 60px rgba(0, 0, 0, 0.34);
        --shadow-md: 0 16px 34px rgba(0, 0, 0, 0.22);
      }

      /* ---------- Hide Streamlit chrome ---------- */
      #MainMenu {visibility: hidden;}
      footer {visibility: hidden;}
      header {visibility: hidden;}

      header[data-testid="stHeader"] {
          height: 0px;
      }

      div[data-testid="stToolbar"] {
          visibility: hidden;
          height: 0%;
          position: fixed;
      }

      /* ---------- App shell ---------- */
      html, body, [class*="css"] {
        font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      }

      .stApp {
        background:
          radial-gradient(circle at top left, rgba(124,140,255,0.16), transparent 24%),
          radial-gradient(circle at 85% 10%, rgba(66,200,255,0.13), transparent 22%),
          radial-gradient(circle at 20% 85%, rgba(56,215,193,0.09), transparent 20%),
          linear-gradient(180deg, var(--bg-0) 0%, var(--bg-1) 40%, #081326 100%);
        color: var(--text-main);
      }

      .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2.8rem;
        max-width: 1320px;
      }

      section[data-testid="stSidebar"] {
        top: 0px;
        border-right: 1px solid rgba(148, 163, 184, 0.10);
        background:
          radial-gradient(circle at top, rgba(124,140,255,0.12), transparent 24%),
          linear-gradient(180deg, rgba(9,20,38,0.92), rgba(8,18,34,0.96));
        backdrop-filter: blur(16px);
      }

      h1, h2, h3 {
        letter-spacing: -0.02em;
        color: var(--text-main);
      }

      p, label, .stCaption, .stMarkdown, .stText, .stAlert {
        color: var(--text-muted);
      }

      /* ---------- Top brand bar ---------- */
      .topbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
        gap: 1rem;
        padding: 0.1rem 0 0.25rem 0;
      }

      .brand-wrap {
        display: flex;
        align-items: center;
        gap: 1rem;
      }

      .brand-badge {
        width: 58px;
        height: 58px;
        border-radius: 20px;
        display: flex;
        align-items: center;
        justify-content: center;
        background:
          linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.02)),
          linear-gradient(135deg, rgba(124,140,255,0.22), rgba(66,200,255,0.18));
        border: 1px solid rgba(148, 163, 184, 0.20);
        box-shadow: var(--shadow-md);
      }

      .brand-logo {
        width: 36px;
        height: 36px;
        display: block;
      }

      .brand-fallback {
        font-size: 1rem;
        font-weight: 800;
        color: var(--text-main);
        letter-spacing: -0.04em;
      }

      .brand-row {
        display: flex;
        align-items: center;
        gap: 0.65rem;
        flex-wrap: wrap;
      }

      .brand-title {
        font-size: 1.75rem;
        font-weight: 800;
        line-height: 1.02;
        letter-spacing: -0.03em;
        color: var(--text-main);
      }

      .brand-chip {
        padding: 0.28rem 0.6rem;
        border-radius: 999px;
        border: 1px solid rgba(124, 140, 255, 0.28);
        background: rgba(124, 140, 255, 0.10);
        color: rgba(224, 231, 255, 0.92);
        font-size: 0.72rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.08em;
      }

      .brand-subtitle {
        font-size: 0.95rem;
        color: rgba(226, 232, 240, 0.70);
        margin-top: 0.18rem;
        max-width: 760px;
      }

      .nav-chip-row {
        display: flex;
        gap: 0.55rem;
        flex-wrap: wrap;
        justify-content: flex-end;
      }

      .nav-chip {
        padding: 0.48rem 0.84rem;
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.14);
        background: rgba(255,255,255,0.04);
        color: rgba(241, 245, 249, 0.82);
        font-size: 0.82rem;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
      }

      /* ---------- Hero ---------- */
      .hero {
        border: 1px solid rgba(148, 163, 184, 0.14);
        border-radius: 28px;
        padding: 28px 28px 26px 28px;
        background:
          radial-gradient(circle at top left, rgba(124,140,255,0.22), transparent 34%),
          radial-gradient(circle at top right, rgba(56,215,193,0.12), transparent 28%),
          linear-gradient(180deg, rgba(10,25,48,0.88), rgba(9,21,40,0.84));
        margin-bottom: 1.2rem;
        box-shadow: var(--shadow-lg);
        position: relative;
        overflow: hidden;
      }

      .hero::after {
        content: "";
        position: absolute;
        inset: 0;
        pointer-events: none;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.03), transparent);
        opacity: 0.6;
      }

      .hero-grid {
        display: grid;
        grid-template-columns: minmax(0, 1fr) 320px;
        gap: 1rem;
        align-items: stretch;
      }

      .hero-eyebrow {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.14em;
        color: rgba(224, 231, 255, 0.68);
        margin-bottom: 0.45rem;
      }

      .hero-title {
        font-size: 2.15rem;
        font-weight: 800;
        line-height: 1.04;
        max-width: 900px;
        margin-bottom: 0.7rem;
        color: var(--text-main);
      }

      .hero-copy {
        font-size: 1rem;
        line-height: 1.58;
        color: rgba(226, 232, 240, 0.82);
        max-width: 920px;
      }

      .hero-pills {
        margin-top: 1rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.55rem;
      }

      .hero-pill {
        display: inline-block;
        padding: 0.4rem 0.78rem;
        border-radius: 999px;
        font-size: 0.83rem;
        border: 1px solid rgba(148, 163, 184, 0.12);
        background: rgba(255,255,255,0.055);
        color: rgba(241, 245, 249, 0.84);
      }

      .hero-side {
        border: 1px solid rgba(148, 163, 184, 0.14);
        border-radius: 22px;
        background: linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
        padding: 18px;
        position: relative;
        z-index: 1;
      }

      .hero-side-title {
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: rgba(224, 231, 255, 0.70);
        margin-bottom: 0.8rem;
      }

      .hero-flow-item {
        display: flex;
        align-items: center;
        gap: 0.72rem;
        padding: 0.72rem 0;
        color: rgba(241, 245, 249, 0.90);
        border-bottom: 1px solid rgba(148, 163, 184, 0.10);
      }

      .hero-flow-item:last-child {
        border-bottom: none;
        padding-bottom: 0;
      }

      .hero-flow-index {
        width: 28px;
        height: 28px;
        flex: 0 0 28px;
        border-radius: 999px;
        background: rgba(124, 140, 255, 0.14);
        border: 1px solid rgba(124, 140, 255, 0.22);
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 0.78rem;
        font-weight: 700;
        color: #dbeafe;
      }

      .hero-flow-copy {
        font-size: 0.92rem;
        line-height: 1.42;
      }

      /* ---------- Cards ---------- */
      .card {
        border: 1px solid rgba(148, 163, 184, 0.14);
        border-radius: 22px;
        padding: 16px 18px;
        background: linear-gradient(180deg, rgba(9,20,38,0.84), rgba(9,20,38,0.72));
        margin-bottom: 0.9rem;
        box-shadow: var(--shadow-md);
        backdrop-filter: blur(14px);
      }

      .soft-card {
        border: 1px solid rgba(148, 163, 184, 0.12);
        border-radius: 18px;
        padding: 14px 16px;
        background: linear-gradient(180deg, rgba(255,255,255,0.045), rgba(255,255,255,0.025));
        margin-bottom: 0.75rem;
        transition: transform 0.18s ease, box-shadow 0.18s ease;
      }

      .soft-card:hover {
        transform: translateY(-1px);
        box-shadow: 0 10px 24px rgba(0,0,0,0.12);
      }

      .muted {
        color: rgba(255,255,255,0.66);
      }

      .small {
        font-size: 0.92rem;
        color: rgba(255,255,255,0.74);
      }

      .section-kicker {
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.10em;
        color: rgba(224, 231, 255, 0.56);
        margin-bottom: 0.28rem;
      }

      .sidebar-app-row {
        display: flex;
        align-items: center;
        gap: 0.75rem;
        margin-bottom: 0.9rem;
      }

      .sidebar-logo-shell {
        width: 40px;
        height: 40px;
        border-radius: 14px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.03));
        border: 1px solid rgba(148, 163, 184, 0.16);
      }

      .sidebar-logo-shell .brand-logo {
        width: 24px;
        height: 24px;
      }

      .sidebar-app-title {
        font-size: 1rem;
        font-weight: 700;
        color: var(--text-main);
        line-height: 1.1;
      }

      /* ---------- Alerts ---------- */
      .alert {
        border-radius: 18px;
        padding: 13px 15px;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(255,255,255,0.05);
        margin: 0.35rem 0 0.95rem 0;
      }

      .alert-danger {
        border-color: rgba(239, 68, 68, 0.40);
        background: rgba(239, 68, 68, 0.10);
      }

      .alert-warn {
        border-color: rgba(245, 158, 11, 0.40);
        background: rgba(245, 158, 11, 0.10);
      }

      .alert-ok {
        border-color: rgba(34, 197, 94, 0.34);
        background: rgba(34, 197, 94, 0.08);
      }

      /* ---------- Pills ---------- */
      .pill {
        display:inline-block;
        padding: 0.22rem 0.62rem;
        border-radius: 999px;
        font-size: 0.83rem;
        border: 1px solid rgba(148, 163, 184, 0.14);
        background: rgba(255,255,255,0.07);
        margin-left: 0.35rem;
        white-space: nowrap;
      }

      .pill-interested { border-color: rgba(56, 189, 248, 0.40); background: rgba(56, 189, 248, 0.12); }
      .pill-applied    { border-color: rgba(59, 130, 246, 0.40); background: rgba(59, 130, 246, 0.12); }
      .pill-oa         { border-color: rgba(168, 85, 247, 0.40); background: rgba(168, 85, 247, 0.12); }
      .pill-interview  { border-color: rgba(245, 158, 11, 0.45); background: rgba(245, 158, 11, 0.14); }
      .pill-offer      { border-color: rgba(34, 197, 94, 0.45); background: rgba(34, 197, 94, 0.14); }
      .pill-rejected   { border-color: rgba(239, 68, 68, 0.45); background: rgba(239, 68, 68, 0.14); }
      .pill-ghosted    { border-color: rgba(148, 163, 184, 0.40); background: rgba(148, 163, 184, 0.10); }

      /* ---------- Agent panel ---------- */
      .agent-card {
        border: 1px solid rgba(124,140,255,0.20);
        border-radius: 22px;
        padding: 18px 18px;
        background: linear-gradient(180deg, rgba(124,140,255,0.11), rgba(66,200,255,0.05));
        margin: 0.8rem 0 1rem 0;
        box-shadow: var(--shadow-md);
      }

      .agent-title {
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: -0.01em;
        color: #f8fafc;
      }

      /* ---------- Tracker cards ---------- */
      .rowcard {
        border-radius: 18px;
        padding: 13px 15px;
        border: 1px solid rgba(148, 163, 184, 0.12);
        background: rgba(255,255,255,0.045);
        margin-bottom: 0.65rem;
      }

      .rowcard-danger {
        border-color: rgba(239, 68, 68, 0.42);
        background: rgba(239, 68, 68, 0.08);
      }

      .rowcard-warn {
        border-color: rgba(245, 158, 11, 0.42);
        background: rgba(245, 158, 11, 0.08);
      }

      /* ---------- Inputs and buttons ---------- */
      div.stButton > button,
      div.stDownloadButton > button,
      div[data-testid="stFormSubmitButton"] > button,
      .stLinkButton > a {
        border-radius: 14px !important;
        padding: 0.68rem 1rem !important;
        border: 1px solid rgba(148, 163, 184, 0.16) !important;
        background: linear-gradient(180deg, rgba(255,255,255,0.08), rgba(255,255,255,0.03)) !important;
        color: var(--text-main) !important;
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
        transition: transform 0.16s ease, box-shadow 0.16s ease, border-color 0.16s ease;
      }

      div.stButton > button:hover,
      div.stDownloadButton > button:hover,
      div[data-testid="stFormSubmitButton"] > button:hover,
      .stLinkButton > a:hover {
        border-color: rgba(124, 140, 255, 0.34) !important;
        transform: translateY(-1px);
        box-shadow: 0 14px 28px rgba(0,0,0,0.16);
      }

      div.stButton > button[kind="primary"],
      div[data-testid="stFormSubmitButton"] > button[kind="primary"] {
        background: linear-gradient(135deg, rgba(124,140,255,0.92), rgba(66,200,255,0.86)) !important;
        border-color: rgba(124, 140, 255, 0.30) !important;
        color: #04111f !important;
        font-weight: 700 !important;
      }

      div.stTextInput > div > div > input,
      div.stTextArea textarea,
      div[data-baseweb="select"] > div,
      div.stDateInput > div > div input {
        border-radius: 14px !important;
        background: rgba(4, 12, 24, 0.72) !important;
        border: 1px solid rgba(148, 163, 184, 0.14) !important;
        color: var(--text-main) !important;
      }

      div.stTextInput > div > div > input:focus,
      div.stTextArea textarea:focus,
      div[data-baseweb="select"] > div:focus-within,
      div.stDateInput > div > div input:focus {
        border-color: rgba(124, 140, 255, 0.40) !important;
        box-shadow: 0 0 0 1px rgba(124,140,255,0.20) !important;
      }

      div[data-baseweb="base-input"] > div,
      div[data-baseweb="textarea"] {
        background: transparent !important;
      }

      div[data-testid="stFileUploader"] section,
      div[data-testid="stFileUploaderDropzone"] {
        border-radius: 18px !important;
        border: 1px dashed rgba(148, 163, 184, 0.18) !important;
        background: rgba(255,255,255,0.03) !important;
      }

      /* ---------- Metrics and data widgets ---------- */
      [data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(9,20,38,0.78), rgba(9,20,38,0.64));
        border: 1px solid rgba(148, 163, 184, 0.14);
        padding: 0.95rem 1rem;
        border-radius: 18px;
        box-shadow: var(--shadow-md);
      }

      [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
        border-radius: 18px;
        overflow: hidden;
        border: 1px solid rgba(148, 163, 184, 0.12);
      }

      div[data-testid="stExpander"] {
        border: 1px solid rgba(148, 163, 184, 0.12);
        border-radius: 18px;
        background: rgba(255,255,255,0.035);
        overflow: hidden;
      }

      /* ---------- Tabs ---------- */
      div[data-baseweb="tab-list"] {
        gap: 0.5rem;
      }

      button[data-baseweb="tab"] {
        font-weight: 600;
        border-radius: 14px;
        background: rgba(255,255,255,0.03);
        border: 1px solid transparent;
        color: rgba(226, 232, 240, 0.76);
        padding: 0.55rem 0.95rem;
      }

      button[data-baseweb="tab"][aria-selected="true"] {
        background: rgba(124, 140, 255, 0.12);
        border-color: rgba(124, 140, 255, 0.26);
        color: var(--text-main);
        box-shadow: inset 0 1px 0 rgba(255,255,255,0.06);
      }

      button[data-baseweb="tab"]:hover {
        color: var(--text-main);
      }

      /* ---------- Responsive ---------- */
      @media (max-width: 1100px) {
        .hero-grid {
          grid-template-columns: 1fr;
        }
      }

      @media (max-width: 900px) {
        .hero-title {
          font-size: 1.55rem;
        }
        .brand-title {
          font-size: 1.28rem;
        }
        .topbar {
          flex-direction: column;
          align-items: flex-start;
        }
        .nav-chip-row {
          justify-content: flex-start;
        }
      }
    </style>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# BRAND BAR
# -------------------------
st.markdown(
    f"""
    <div class="topbar">
      <div class="brand-wrap">
        <div class="brand-badge">{LOGO_MARKUP}</div>
        <div>
          <div class="brand-row">
            <div class="brand-title">NextRole</div>
            <div class="brand-chip">Career Ops OS</div>
          </div>
          <div class="brand-subtitle">AI workflow for targeted applications, recruiter outreach, and follow-up execution.</div>
        </div>
      </div>
      <div class="nav-chip-row">
        <div class="nav-chip">Fit scoring</div>
        <div class="nav-chip">Resume intelligence</div>
        <div class="nav-chip">Application workflow</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tabs = st.tabs(["✨ Job Agent", "📧 Cold Outreach", "📊 Pipeline Tracker"])

st.markdown(
    """
    <div class="hero">
      <div class="hero-grid">
        <div>
          <div class="hero-eyebrow">Agentic job search workflow</div>
          <div class="hero-title">Find the right role. Use the right resume. Take the right next step.</div>
          <div class="hero-copy">
            NextRole analyzes fit, recommends your strongest resume version, generates grounded application materials,
            and keeps your application pipeline organized with priority and follow-up intelligence.
          </div>
          <div class="hero-pills">
            <span class="hero-pill">Fit scoring</span>
            <span class="hero-pill">Resume recommendation</span>
            <span class="hero-pill">Application generation</span>
            <span class="hero-pill">Follow-up intelligence</span>
          </div>
        </div>
        <div class="hero-side">
          <div class="hero-side-title">What the workflow handles</div>
          <div class="hero-flow-item">
            <span class="hero-flow-index">01</span>
            <span class="hero-flow-copy">Prioritize the best-fit roles and strongest resume version.</span>
          </div>
          <div class="hero-flow-item">
            <span class="hero-flow-index">02</span>
            <span class="hero-flow-copy">Generate tailored materials grounded in the actual job post.</span>
          </div>
          <div class="hero-flow-item">
            <span class="hero-flow-index">03</span>
            <span class="hero-flow-copy">Run targeted cold outreach with Gmail-based tracking and dedupe protection.</span>
          </div>
          <div class="hero-flow-item">
            <span class="hero-flow-index">04</span>
            <span class="hero-flow-copy">Sync inbox replies and recommend the next best follow-up step.</span>
          </div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# -------------------------
# AUTH (LOGIN + SIGNUP)
# -------------------------
def auth_box() -> str:
    if st.session_state.get("user_id"):
        return st.session_state["user_id"]

    with st.sidebar:
        st.markdown("### 👤 Account")
        if st.query_params.get("code") and st.query_params.get("state"):
            st.info("Sign in to finish connecting your Gmail account.")
        mode = st.radio("Mode", ["Log in", "Sign up"], horizontal=True)

        username = st.text_input("Username", key="auth_user")
        password = st.text_input("Password", type="password", key="auth_pass")

        if mode == "Sign up":
            password2 = st.text_input("Confirm password", type="password", key="auth_pass2")
            if st.button("Create account", use_container_width=True):
                try:
                    if (username or "").strip() == "" or (password or "").strip() == "":
                        st.error("Username and password are required.")
                        st.stop()
                    if password != password2:
                        st.error("Passwords do not match.")
                        st.stop()
                    create_user(username, password)
                    st.success("Account created ✅ Now switch to Log in.")
                except Exception as e:
                    st.error(str(e))
        else:
            if st.button("Log in", type="primary", use_container_width=True):
                if verify_user(username, password):
                    st.session_state["user_id"] = (username or "").strip()
                    st.rerun()
                else:
                    st.error("Invalid username/password")

    st.stop()


user_id = auth_box()

gmail_sidebar_connection = get_user_gmail_connection(user_id)
gmail_sidebar_label = (
    f"Gmail connected: {gmail_sidebar_connection.get('gmail_email', '')}"
    if gmail_sidebar_connection.get("connected")
    else "Gmail not connected"
)

with st.sidebar:
    st.markdown(
        f"""
        <div class="card">
          <div class="sidebar-app-row">
            <div class="sidebar-logo-shell">{LOGO_MARKUP}</div>
            <div>
              <div class="section-kicker">Workspace</div>
              <div class="sidebar-app-title">NextRole</div>
            </div>
          </div>
          <div class="small">Signed in as</div>
          <div style="font-size:1.05rem;"><b>{user_id}</b></div>
          <div class="muted small">Neon (Postgres) tracker enabled ✅</div>
          <div class="muted small">{gmail_sidebar_label}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Log out", use_container_width=True):
        st.session_state["user_id"] = ""
        st.rerun()


def start_gmail_connect_flow(user_id: str) -> None:
    auth_url, state = GmailSender.begin_user_connect_flow()
    save_user_gmail_oauth_state(user_id, state)
    st.session_state["gmail_oauth_state"] = state
    st.session_state["gmail_oauth_user"] = user_id
    st.session_state["gmail_oauth_url"] = auth_url

# -------------------------
# ACCOUNT SETTINGS
# -------------------------
with st.sidebar.expander("⚙️ Account settings", expanded=False):
    st.markdown("#### Gmail connection")
    gmail_connection = get_user_gmail_connection(user_id)
    if st.session_state.get("gmail_connect_success"):
        st.success(st.session_state["gmail_connect_success"])
        st.session_state["gmail_connect_success"] = ""
    if st.session_state.get("gmail_connect_error"):
        st.error(st.session_state["gmail_connect_error"])
        st.session_state["gmail_connect_error"] = ""

    if gmail_connection.get("connected"):
        connected_email = gmail_connection.get("gmail_email", "")
        connected_at = gmail_connection.get("connected_at")
        connected_at_label = connected_at.strftime("%Y-%m-%d %H:%M") if connected_at else "recently"
        st.info(f"Connected Gmail: {connected_email}\n\nConnected: {connected_at_label}")
        gx1, gx2 = st.columns(2)
        with gx1:
            if st.button("Reconnect Gmail", use_container_width=True):
                try:
                    start_gmail_connect_flow(user_id)
                except Exception as e:
                    st.error(str(e))
        with gx2:
            if st.button("Disconnect Gmail", use_container_width=True):
                try:
                    clear_user_gmail_connection(user_id)
                    clear_user_gmail_oauth_state(user_id)
                    st.session_state["gmail_oauth_state"] = ""
                    st.session_state["gmail_oauth_user"] = ""
                    st.session_state["gmail_oauth_url"] = ""
                    st.success("Disconnected Gmail ✅")
                    st.rerun()
                except Exception as e:
                    st.error(str(e))
    else:
        st.caption("Connect a Google account so this user sends outreach from their own Gmail account.")
        if st.button("Connect Gmail", use_container_width=True):
            try:
                start_gmail_connect_flow(user_id)
            except Exception as e:
                st.error(str(e))

    pending_gmail_url = st.session_state.get("gmail_oauth_url", "")
    if pending_gmail_url and st.session_state.get("gmail_oauth_user") == user_id:
        st.link_button("Continue with Google", pending_gmail_url, use_container_width=True)
        st.caption(
            "After approving Google access, you will be redirected back here and this account will be linked to your app user."
        )

    st.divider()

    st.markdown("#### Change password")
    with st.form("change_pw_form"):
        cur = st.text_input("Current password", type="password", key="pw_cur")
        new = st.text_input("New password", type="password", key="pw_new")
        new2 = st.text_input("Confirm new password", type="password", key="pw_new2")
        ok = st.form_submit_button("Update password")
        if ok:
            try:
                if new != new2:
                    st.error("New passwords do not match.")
                else:
                    change_password(user_id, cur, new)
                    st.success("Password updated ✅")
            except Exception as e:
                st.error(str(e))

    st.divider()

    st.markdown("#### Delete my account")
    st.warning("This permanently deletes your account AND all your tracker entries.")
    with st.form("delete_account_form"):
        del_pw = st.text_input("Password to confirm", type="password", key="del_pw")
        confirm_text = st.text_input('Type DELETE to confirm', key="del_confirm")
        do_del = st.form_submit_button("Delete account permanently")
        if do_del:
            try:
                if confirm_text.strip().upper() != "DELETE":
                    st.error("Confirmation text must be DELETE.")
                elif not verify_user(user_id, del_pw):
                    st.error("Password is incorrect.")
                else:
                    delete_all_for_user(user_id)
                    delete_user(user_id, del_pw)
                    st.session_state["user_id"] = ""
                    st.success("Account deleted ✅")
                    st.rerun()
            except Exception as e:
                st.error(str(e))

# -------------------------
# ADMIN VIEW
# -------------------------
admins = st.secrets.get("admin_users", [])
is_admin = user_id in admins

if is_admin:
    with st.sidebar.expander("🛡️ Admin", expanded=False):
        admin_mode = st.checkbox("Enable admin panel", value=False)

    if admin_mode:
        st.markdown("## 🛡️ Admin panel")
        st.caption("Read-only: users + job counts. Optional: view a user's tracker.")

        users_df = admin_list_users()
        st.dataframe(users_df, use_container_width=True)

        if not users_df.empty and "user_id" in users_df.columns:
            target = st.selectbox("View tracker as user", users_df["user_id"].tolist())
            view_df = read_tracker_df(target).drop(columns=["_row_id"], errors="ignore")
            st.markdown(f"### Tracker for: {target}")
            st.dataframe(view_df, use_container_width=True)
        st.divider()

# -------------------------
# SESSION STATE DEFAULTS
# -------------------------
defaults = {
    "company_input": "",
    "role_input": "",
    "location_input": "",
    "contact_name_input": "",
    "contact_link_input": "",
    "fit_score": 0,
    "fit_priority": "",
    "fit_summary": "",
    "fit_matched": [],
    "fit_missing": [],
}
for k, v in defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

if "resume_text" not in st.session_state:
    st.session_state["resume_text"] = ""
if "resume_name" not in st.session_state:
    st.session_state["resume_name"] = ""
if "resume_bytes" not in st.session_state:
    st.session_state["resume_bytes"] = b""
if "resume_filename" not in st.session_state:
    st.session_state["resume_filename"] = ""
if "resume_mime_type" not in st.session_state:
    st.session_state["resume_mime_type"] = ""
if "show_add_form" not in st.session_state:
    st.session_state["show_add_form"] = False
if "tracker_last_saved" not in st.session_state:
    st.session_state["tracker_last_saved"] = ""

if "apollo_people_results" not in st.session_state:
    st.session_state["apollo_people_results"] = []
if "apollo_selected_people_ids" not in st.session_state:
    st.session_state["apollo_selected_people_ids"] = []
if "apollo_enriched_people" not in st.session_state:
    st.session_state["apollo_enriched_people"] = []
if "apollo_generated_subject" not in st.session_state:
    st.session_state["apollo_generated_subject"] = ""
if "apollo_generated_body" not in st.session_state:
    st.session_state["apollo_generated_body"] = ""
if "apollo_generated_followup" not in st.session_state:
    st.session_state["apollo_generated_followup"] = ""
if "apollo_created_contacts" not in st.session_state:
    st.session_state["apollo_created_contacts"] = []
if "gmail_last_send_result" not in st.session_state:
    st.session_state["gmail_last_send_result"] = None
if "gmail_send_error" not in st.session_state:
    st.session_state["gmail_send_error"] = ""
if "daily_outreach_queue" not in st.session_state:
    st.session_state["daily_outreach_queue"] = []
if "daily_discovery_last_run" not in st.session_state:
    st.session_state["daily_discovery_last_run"] = ""
if "gmail_oauth_state" not in st.session_state:
    st.session_state["gmail_oauth_state"] = ""
if "gmail_oauth_user" not in st.session_state:
    st.session_state["gmail_oauth_user"] = ""
if "gmail_oauth_url" not in st.session_state:
    st.session_state["gmail_oauth_url"] = ""
if "gmail_connect_success" not in st.session_state:
    st.session_state["gmail_connect_success"] = ""
if "gmail_connect_error" not in st.session_state:
    st.session_state["gmail_connect_error"] = ""

if st.session_state.get("profile_loaded_for") != user_id:
    try:
        profile = get_user_profile(user_id)
        profile_session_keys = {
            "target_role": "outreach_preferred_role",
            "target_location": "outreach_target_location",
            "linkedin_url": "apollo_linkedin_profile",
            "portfolio_url": "apollo_portfolio_website",
            "sender_name": "gmail_sender_name",
            "candidate_summary": "outreach_candidate_summary",
        }
        for profile_key, session_key in profile_session_keys.items():
            if profile.get(profile_key):
                st.session_state[session_key] = profile[profile_key]
    except Exception as e:
        st.session_state["profile_load_error"] = str(e)
    st.session_state["profile_loaded_for"] = user_id

# -------------------------
# HELPERS
# -------------------------
def read_pdf(file) -> str:
    reader = PdfReader(file)
    pages = []
    for p in reader.pages:
        pages.append(p.extract_text() or "")
    return "\n".join(pages).strip()


def read_txt(file) -> str:
    return file.read().decode("utf-8", errors="ignore").strip()


def make_docx(text: str) -> bytes:
    doc = Document()
    for line in text.splitlines():
        doc.add_paragraph(line)
    bio = io.BytesIO()
    doc.save(bio)
    return bio.getvalue()


def df_to_xlsx_bytes(df: pd.DataFrame) -> bytes:
    bio = io.BytesIO()
    with pd.ExcelWriter(bio, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Applications")
    return bio.getvalue()


def _coerce_editor_types(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    text_cols = [
        "Company", "Role", "Job Link", "Location", "Status",
        "Fit Score", "Priority", "Fit Summary", "Resume Used",
        "Contact Name", "Contact Link", "Notes", "Next Action"
    ]
    date_cols = ["Date Applied", "Follow-up Date"]

    for c in text_cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)

    for c in date_cols:
        if c in df.columns:
            df[c] = df[c].fillna("").astype(str)

    if "Delete" in df.columns:
        df["Delete"] = df["Delete"].fillna(False).astype(bool)

    return df


def _status_slug(status: str) -> str:
    return (status or "").strip().lower().replace(" ", "")


def render_status_pill(status: str) -> str:
    slug = _status_slug(status)
    klass = f"pill pill-{slug}" if slug else "pill"
    return f'<span class="{klass}">{status}</span>'


def safe_date_parse(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series.astype(str), errors="coerce")
    return dt.dt.date


def priority_rank(priority: str) -> int:
    mapping = {
        "Apply now": 4,
        "Strong consider": 3,
        "Apply if strategic": 2,
        "Low priority": 1,
    }
    return mapping.get((priority or "").strip(), 0)


def style_dashboard_chart(chart: alt.Chart, *, height: int = 260) -> alt.Chart:
    return (
        chart.properties(height=height)
        .configure_view(strokeOpacity=0)
        .configure_axis(
            labelColor="#cbd5e1",
            titleColor="#e2e8f0",
            gridColor="rgba(148,163,184,0.14)",
            domainColor="rgba(148,163,184,0.16)",
            tickColor="rgba(148,163,184,0.16)",
            labelFontSize=12,
            titleFontSize=12,
        )
        .configure_legend(
            labelColor="#cbd5e1",
            titleColor="#e2e8f0",
            orient="top",
            padding=6,
        )
        .configure_title(color="#f8fafc", fontSize=14, anchor="start")
    )


def _query_param_str(name: str) -> str:
    value = st.query_params.get(name)
    if isinstance(value, list):
        return str(value[0]) if value else ""
    return str(value or "")


def clear_gmail_oauth_query_params() -> None:
    for key in ("code", "state", "scope", "authuser", "prompt", "error", "error_subtype", "iss", "hd"):
        try:
            del st.query_params[key]
        except Exception:
            pass

def handle_gmail_oauth_callback(user_id: str) -> None:
    error = _query_param_str("error")
    if error:
        st.session_state["gmail_connect_error"] = f"Google OAuth was not completed: {error}"
        st.session_state["gmail_connect_success"] = ""
        clear_gmail_oauth_query_params()
        st.rerun()

    code = _query_param_str("code")
    state = _query_param_str("state")
    if not code or not state:
        return

    expected_state = st.session_state.get("gmail_oauth_state", "")
    expected_user = st.session_state.get("gmail_oauth_user", "")
    db_expected_state = get_user_gmail_oauth_state(user_id)
    try:
        if not gmail_oauth_state_matches(
            state,
            session_state=expected_state,
            session_user=expected_user,
            db_state=db_expected_state,
            user_id=user_id,
        ):
            raise ValueError("Google OAuth state mismatch. Please start the Gmail connection again.")
        gmail_email = GmailSender.complete_user_connect_flow(user_id, state=state, code=code)
        st.session_state["gmail_connect_success"] = f"Connected Gmail account: {gmail_email}"
        st.session_state["gmail_connect_error"] = ""
    except Exception as e:
        st.session_state["gmail_connect_error"] = str(e)
        st.session_state["gmail_connect_success"] = ""
    finally:
        st.session_state["gmail_oauth_state"] = ""
        st.session_state["gmail_oauth_user"] = ""
        st.session_state["gmail_oauth_url"] = ""
        clear_user_gmail_oauth_state(user_id)
        clear_gmail_oauth_query_params()
        st.rerun()


def render_decision_card(priority: str, fit_score: str, fit_summary: str, next_action: str, resume_name: str = "") -> str:
    priority = priority or "Not scored"
    fit_score = fit_score or "—"
    fit_summary = fit_summary or "No summary available."
    next_action = next_action or "Review manually."
    resume_line = f'<div class="muted small">Recommended resume: <b>{resume_name}</b></div>' if resume_name else ""

    return f"""
    <div class="agent-card">
      <div class="agent-title">🤖 Agent Decision</div>
      <div style="margin-top: 0.45rem;">
        <span class="pill">{priority}</span>
        <span class="pill">Fit Score: {fit_score}</span>
      </div>
      <div style="margin-top: 0.75rem;">{fit_summary}</div>
      <div class="muted small" style="margin-top: 0.65rem;">Next action: <b>{next_action}</b></div>
      {resume_line}
    </div>
    """


handle_gmail_oauth_callback(user_id)


def extract_resume_highlights(resume_text: str, limit: int = 3) -> list:
    lines = []
    for raw in (resume_text or "").splitlines():
        line = raw.strip().lstrip("•").lstrip("-").strip()
        if len(line) < 20:
            continue
        if any(skip in line.lower() for skip in ["education", "experience", "skills", "projects", "summary"]):
            continue
        lines.append(line)
    return lines[:limit]


OUTREACH_FOCUS_RULES = [
    (
        ("data engineer", "analytics engineer", "etl", "pipeline", "warehouse", "dbt", "spark", "airflow"),
        {
            "subject_hook": "Data Pipelines",
            "value_prop": "I am especially interested in teams where dependable pipelines, warehousing, and automation make analytics work possible at scale.",
            "followup_hook": "data pipelines and analytics infrastructure",
        },
    ),
    (
        ("data analyst", "business analyst", "analytics", "dashboard", "reporting", "insight", "bi", "metrics"),
        {
            "subject_hook": "Analytics Workflows",
            "value_prop": "I tend to do my best work where SQL, Python, and reporting workflows need to turn messy data into dependable day-to-day decision support.",
            "followup_hook": "analytics and reporting work",
        },
    ),
    (
        ("machine learning", "data science", "ai", "ml", "llm", "rag", "retrieval", "applied scientist"),
        {
            "subject_hook": "Applied AI",
            "value_prop": "I am drawn to teams that connect solid data work with practical machine learning or AI systems that have to be useful beyond a demo.",
            "followup_hook": "applied AI and data work",
        },
    ),
    (
        ("software engineer", "backend", "frontend", "full stack", "platform", "developer", "api"),
        {
            "subject_hook": "Product Engineering",
            "value_prop": "I enjoy roles that sit between implementation, automation, and product-minded engineering where reliability matters as much as speed.",
            "followup_hook": "product engineering work",
        },
    ),
    (
        ("product", "apm", "product manager", "product analyst", "experimentation", "roadmap"),
        {
            "subject_hook": "Product + Analytics",
            "value_prop": "I am interested in teams where analysis, experimentation, and product thinking are tightly connected to execution.",
            "followup_hook": "product and analytics work",
        },
    ),
]


def _first_sentence(text: str, limit: int = 240) -> str:
    cleaned = re.sub(r"\s+", " ", text or "").strip()
    if not cleaned:
        return ""
    sentence = re.split(r"(?<=[.!?])\s+", cleaned, maxsplit=1)[0].strip()
    if len(sentence) > limit:
        sentence = sentence[:limit].rstrip(" ,;:") + "..."
    return sentence


def choose_outreach_focus(role_title: str, matched_role: str = "", primary_focus: str = "", company_context: str = "") -> dict:
    combined = " ".join(part for part in [role_title, matched_role, primary_focus, company_context] if part).lower()
    for keywords, pack in OUTREACH_FOCUS_RULES:
        if any(keyword in combined for keyword in keywords):
            return pack
    return {
        "subject_hook": "Data + AI",
        "value_prop": "I am looking for roles where analysis, automation, and product-minded execution overlap in a practical way.",
        "followup_hook": "data and AI work",
    }


def recipient_bucket_from_title(title: str) -> str:
    lowered = (title or "").lower()
    if any(token in lowered for token in ("recruit", "talent", "people", "hr")):
        return "recruiting"
    if any(token in lowered for token in ("cto", "chief", "co-founder", "founder", "vp", "head of")):
        return "leadership"
    if "manager" in lowered or "director" in lowered:
        return "manager"
    return "team"


def summarize_candidate_angle(candidate_summary: str, highlights: list) -> str:
    summary = _first_sentence(candidate_summary, limit=260)
    if not summary:
        summary = (
            "I am currently pursuing an MS in Information Systems at UMD and have hands-on experience in analytics, SQL, Python, dashboards, and AI-enabled application work."
        )
    if summary and summary[-1] not in ".!?":
        summary += "."
    if highlights:
        return summary + " Recent work includes " + "; ".join(highlights[:2]) + "."
    return summary


def build_cold_email(
    candidate_summary: str,
    company_name: str,
    role_title: str,
    recipient_name: str,
    recipient_title: str,
    *,
    linkedin_url: str = "",
    portfolio_url: str = "",
    resume_text: str = "",
    company_reason: str = "",
    sender_name: str = "Sai Praneeth",
    company_context: str = "",
    matched_role: str = "",
    primary_focus: str = "",
    discovery_source: str = "",
    role_source_url: str = "",
    use_ai_personalization: bool = True,
) -> dict:
    first_name = (recipient_name or "there").split()[0] if recipient_name else "there"
    company_ref = company_name or "your team"
    role_ref = role_title or "relevant opportunities"
    sender_display = (sender_name or "").strip() or "Sai Praneeth"
    highlights = extract_resume_highlights(resume_text, limit=2)
    focus_pack = choose_outreach_focus(role_ref, matched_role=matched_role, primary_focus=primary_focus, company_context=company_context)
    recipient_bucket = recipient_bucket_from_title(recipient_title)
    opportunity_ref = (matched_role or "").strip() or role_ref
    candidate_line = summarize_candidate_angle(candidate_summary, highlights)

    reason_clean = (company_reason or "").strip()
    if reason_clean and reason_clean[-1] not in ".!?":
        reason_clean += "."
    link_lines = []
    if linkedin_url.strip():
        link_lines.append(f"LinkedIn: {linkedin_url.strip()}")
    if portfolio_url.strip():
        link_lines.append(f"Portfolio: {portfolio_url.strip()}")
    link_block = "\n".join(link_lines)
    if link_block:
        link_block = "\n" + link_block

    if matched_role.strip():
        subject = f"{company_ref}: interest in {opportunity_ref}"
    else:
        subject = f"{company_ref}: {focus_pack['subject_hook']} internship interest"

    ask_line = "If there is a relevant opening now or coming up, I would appreciate the right next step or the best person on the team to speak with."
    if recipient_bucket == "recruiting":
        ask_line = "If there is a current or upcoming fit, I would appreciate the right next step in the process or the best team to connect with."
    elif recipient_bucket == "manager":
        ask_line = "If your team is hiring in this area, I would value any guidance on where this kind of background might fit best."
    elif recipient_bucket == "leadership":
        ask_line = "If there is a team where this background could be useful, I would be grateful for a quick pointer on the right next step."

    role_context_line = ""
    if recipient_title.strip():
        role_context_line = f"Given your role as {recipient_title}, I thought this might be relevant."

    body_lines = [
        f"Hi {first_name},",
        "",
        reason_clean or f"I am reaching out because {company_ref} seems closely aligned with the kind of {role_ref} work I am targeting.",
        "",
        candidate_line,
        focus_pack["value_prop"],
        "",
    ]
    if role_context_line:
        body_lines.extend([role_context_line, ""])
    body_lines.extend([ask_line + link_block, "", "Best,", sender_display])
    body = "\n".join(body_lines)

    followup = (
        f"Hi {first_name},\n\n"
        f"Following up on my note about {opportunity_ref} at {company_ref}. I remain interested because the opportunity seems closely aligned with my background in {focus_pack['followup_hook']}. "
        f"If there is a better next step or person to reach out to, I would appreciate the guidance.{link_block}\n\n"
        f"Best,\n{sender_display}\n"
    )

    fallback = {
        "subject": subject,
        "body": body,
        "followup": followup,
        "personalization_angle": reason_clean or f"Targeted to {company_ref} and {role_ref}.",
        "personalization_source": "fallback",
    }

    if not use_ai_personalization:
        return fallback

    try:
        ai_pack = generate_outreach_materials(
            candidate_summary=candidate_summary,
            resume_text=resume_text,
            company_name=company_ref,
            company_context=company_context,
            company_reason=reason_clean,
            matched_role=matched_role,
            primary_focus=primary_focus,
            discovery_source=discovery_source,
            role_source_url=role_source_url,
            role_title=role_ref,
            recipient_name=recipient_name,
            recipient_title=recipient_title,
            linkedin_url=linkedin_url,
            portfolio_url=portfolio_url,
            sender_name=sender_display,
            fallback_subject=subject,
            fallback_body=body,
            fallback_followup=followup,
        )
        return {
            "subject": ai_pack["subject"],
            "body": ai_pack["body"],
            "followup": ai_pack["followup"],
            "personalization_angle": ai_pack.get("personalization_angle") or fallback["personalization_angle"],
            "personalization_source": "ai",
        }
    except Exception:
        fallback["personalization_angle"] = f"{fallback['personalization_angle']} AI personalization fallback used; please review before sending."
        return fallback
def _guess_company_domain(company_name: str) -> str:
    cleaned = re.sub(r"[^a-z0-9 ]", " ", (company_name or "").lower()).strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    if not cleaned:
        return ""
    token = cleaned.split()[0]
    return f"{token}.com"


def _normalize_domain(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    if raw.startswith("http://") or raw.startswith("https://"):
        raw = urlparse(raw).netloc
    raw = raw.lower().replace("www.", "").strip("/")
    return raw


def _safe_get(url: str, timeout: int = 8) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.ok:
            return resp.text[:120000]
    except Exception:
        return ""
    return ""


def scrape_company_context(company_name: str, company_domain: str = "") -> str:
    domain = _normalize_domain(company_domain) or _normalize_domain(_guess_company_domain(company_name))
    candidates = []
    if domain:
        candidates = [f"https://{domain}", f"https://www.{domain}", f"https://{domain}/careers", f"https://{domain}/about"]
    snippets = []
    for url in candidates[:4]:
        html = _safe_get(url)
        if not html:
            continue
        text = re.sub(r"<script[\s\S]*?</script>", " ", html, flags=re.I)
        text = re.sub(r"<style[\s\S]*?</style>", " ", text, flags=re.I)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"\s+", " ", text).strip()
        if text:
            snippets.append(text[:1200])
        if len(snippets) >= 2:
            break
    return " ".join(snippets)[:2200]


def company_reason_from_context(
    company_name: str,
    preferred_role: str,
    context_text: str,
    *,
    matched_role: str = "",
    primary_focus: str = "",
) -> str:
    if matched_role.strip():
        return (
            f"I noticed {company_name} has been hiring for {matched_role.strip()}, and that feels closely aligned with the kind of {preferred_role or 'internship'} work I am targeting."
        )
    if primary_focus.strip():
        return (
            f"I was interested in {company_name}'s work around {primary_focus.strip()}, which feels closely aligned with the kind of {preferred_role or 'internship'} work I want to keep building toward."
        )
    role_words = [w for w in re.findall(r"[A-Za-z]+", preferred_role or "") if len(w) > 3][:3]
    lower = (context_text or "").lower()
    found = [w for w in role_words if w.lower() in lower]
    if found:
        return f"I was especially interested in {company_name}'s work related to {' / '.join(found[:2])}, which feels closely aligned with my target role."
    if context_text:
        sentence = context_text[:220].strip()
        return f"I was especially interested in {company_name} after reading more about the team and company focus."
    return f"The work happening at {company_name} feels closely aligned with the kind of role I am targeting."


def infer_attachment_mime(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename or "")
    return mime or "application/octet-stream"


def set_active_resume(payload: dict) -> None:
    st.session_state["resume_text"] = payload.get("resume_text", "") or ""
    st.session_state["resume_name"] = payload.get("name") or payload.get("file_name") or ""
    st.session_state["resume_bytes"] = payload.get("file_bytes") or b""
    st.session_state["resume_filename"] = payload.get("file_name") or payload.get("name") or ""
    st.session_state["resume_mime_type"] = payload.get("mime_type") or infer_attachment_mime(
        st.session_state["resume_filename"]
    )


def active_resume_attachment() -> dict:
    attachment_bytes = st.session_state.get("resume_bytes") or b""
    attachment_filename = (
        st.session_state.get("resume_filename")
        or st.session_state.get("resume_name")
        or "resume.pdf"
    )
    if not attachment_bytes:
        return {
            "attachment_bytes": None,
            "attachment_filename": None,
            "attachment_mime_type": None,
        }
    return {
        "attachment_bytes": attachment_bytes,
        "attachment_filename": attachment_filename,
        "attachment_mime_type": st.session_state.get("resume_mime_type")
        or infer_attachment_mime(attachment_filename),
    }


def _session_outreach_set(key: str) -> set:
    current = st.session_state.get(key)
    if isinstance(current, set):
        return current
    normalized = set(current or [])
    st.session_state[key] = normalized
    return normalized


def current_outreach_history(user_id: str) -> tuple[set, set]:
    outreach_history = tracker_outreach_history(read_outreach_tracker_df(user_id))
    legacy_history = tracker_outreach_history(read_tracker_df(user_id))
    session_emails = _session_outreach_set("sent_outreach_emails")
    session_identities = _session_outreach_set("sent_outreach_identity_keys")
    return (
        set(outreach_history["emails"]) | set(legacy_history["emails"]) | set(session_emails),
        set(outreach_history["identities"]) | set(legacy_history["identities"]) | set(session_identities),
    )


def remember_outreach_contact(company: str, contact_name: str, email: str) -> None:
    normalized_email = normalize_outreach_email(email)
    if normalized_email:
        _session_outreach_set("sent_outreach_emails").add(normalized_email)
    _session_outreach_set("sent_outreach_identity_keys").update(
        contact_identity_keys(company, contact_name, email)
    )


def log_sent_outreach_to_tracker(
    user_id: str,
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
    gmail_message_id: str = "",
    gmail_thread_id: str = "",
) -> None:
    append_outreach_row(
        user_id,
        build_outreach_tracker_row(
            company=company,
            preferred_role=preferred_role,
            target_location=target_location,
            contact_name=contact_name,
            contact_link=contact_link,
            email=email,
            subject=subject,
            personalization=personalization,
            sponsorship_signal=sponsorship_signal,
            resume_name=resume_name,
            send_source=send_source,
            role_source_url=role_source_url,
            gmail_message_id=gmail_message_id,
            gmail_thread_id=gmail_thread_id,
        ),
    )


def log_draft_outreach_to_tracker(
    user_id: str,
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
) -> None:
    append_outreach_row(
        user_id,
        {
            "Date": str(date.today()),
            "Company": company,
            "Role": preferred_role or "Cold outreach",
            "Location": target_location,
            "Status": "Drafted",
            "Reply Status": "Drafted",
            "Resume Used": resume_name,
            "Follow-up Date": str(date.today() + timedelta(days=5)),
            "Next Follow-up At": str(date.today() + timedelta(days=5)),
            "Contact Name": contact_name,
            "Contact Link": contact_link,
            "Email": email,
            "Subject": subject,
            "Personalization": personalization,
            "Sponsorship Signal": sponsorship_signal or "Unknown / verify",
            "Send Source": send_source,
            "Role Source URL": role_source_url,
            "Gmail Message ID": "",
            "Gmail Thread ID": "",
            "Last Reply At": "",
            "Last Reply From": "",
            "Last Reply Snippet": "",
            "Last Synced At": "",
            "Sequence Step": "Initial",
            "Paused Reason": "",
            "Notes": "Outreach State: drafted",
        },
    )


def build_outreach_next_step_suggestion(
    row: dict,
    *,
    sender_name: str,
    candidate_summary: str,
    linkedin_url: str = "",
    portfolio_url: str = "",
    use_ai: bool = True,
) -> dict[str, str]:
    fallback = fallback_outreach_next_step(
        reply_status=str(row.get("Reply Status", "") or ""),
        company=str(row.get("Company", "") or ""),
        role=str(row.get("Role", "") or ""),
        contact_name=str(row.get("Contact Name", "") or ""),
        sender_name=sender_name,
        latest_reply_snippet=str(row.get("Last Reply Snippet", "") or ""),
        candidate_summary=candidate_summary,
        personalization=str(row.get("Personalization", "") or ""),
        linkedin_url=linkedin_url,
        portfolio_url=portfolio_url,
    )
    if not use_ai:
        return fallback
    try:
        return generate_outreach_next_step(
            reply_status=str(row.get("Reply Status", "") or ""),
            company_name=str(row.get("Company", "") or ""),
            role_title=str(row.get("Role", "") or ""),
            contact_name=str(row.get("Contact Name", "") or ""),
            subject=str(row.get("Subject", "") or ""),
            personalization=str(row.get("Personalization", "") or ""),
            latest_reply_snippet=str(row.get("Last Reply Snippet", "") or ""),
            sender_name=sender_name,
            candidate_summary=candidate_summary,
            linkedin_url=linkedin_url,
            portfolio_url=portfolio_url,
            fallback_action=fallback["suggested_action"],
            fallback_followup=fallback["suggested_followup"],
        )
    except Exception:
        return fallback


def sync_outreach_inbox(
    user_id: str,
    *,
    sender_name: str,
    candidate_summary: str,
    linkedin_url: str = "",
    portfolio_url: str = "",
    use_ai: bool = True,
) -> dict[str, int]:
    outreach_df = read_outreach_tracker_df(user_id)
    if outreach_df.empty:
        return {"synced": 0, "replied": 0, "bounced": 0, "follow_up_due": 0, "suggested": 0, "skipped": 0}

    candidates = outreach_df[
        outreach_df.get("Gmail Thread ID", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != ""
    ].copy()
    if candidates.empty:
        return {"synced": 0, "replied": 0, "bounced": 0, "follow_up_due": 0, "suggested": 0, "skipped": 0}

    sender = GmailSender.from_user(user_id)
    synced_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    results = {"synced": 0, "replied": 0, "bounced": 0, "follow_up_due": 0, "suggested": 0, "skipped": 0}

    for _, row in candidates.iterrows():
        row_id = str(row.get("_row_id", "") or "").strip()
        thread_id = str(row.get("Gmail Thread ID", "") or "").strip()
        if not row_id or not thread_id:
            results["skipped"] += 1
            continue
        try:
            summary = sender.get_thread_summary(thread_id)
        except GmailSendError as exc:
            message = str(exc)
            if "Reconnect Gmail" in message or "cannot sync replies yet" in message:
                raise
            results["skipped"] += 1
            continue

        updates = classify_outreach_thread(
            summary,
            follow_up_date=str(
                row.get("Next Follow-up At")
                or row.get("Follow-up Date")
                or ""
            ),
            today=date.today(),
        )
        updates["Last Synced At"] = synced_at
        if summary.get("thread_id"):
            updates["Gmail Thread ID"] = str(summary.get("thread_id") or "")
        suggestion_input = {
            **{str(k): row.get(k, "") for k in row.index},
            **updates,
            "Last Reply Snippet": updates.get("Last Reply Snippet", str(row.get("Last Reply Snippet", "") or "")),
        }
        suggestion = build_outreach_next_step_suggestion(
            suggestion_input,
            sender_name=sender_name,
            candidate_summary=candidate_summary,
            linkedin_url=linkedin_url,
            portfolio_url=portfolio_url,
            use_ai=use_ai,
        )
        updates["Suggested Action"] = suggestion["suggested_action"]
        updates["Suggested Follow-up"] = suggestion["suggested_followup"]
        updates["Suggestion Updated At"] = synced_at

        update_outreach_row(user_id, row_id, updates)
        results["synced"] += 1
        if suggestion["suggested_followup"]:
            results["suggested"] += 1
        if updates.get("Reply Status") == "Replied":
            results["replied"] += 1
        elif updates.get("Reply Status") == "Bounced":
            results["bounced"] += 1
        elif updates.get("Reply Status") == "Needs follow-up":
            results["follow_up_due"] += 1

    return results


def refresh_outreach_suggestions(
    user_id: str,
    *,
    sender_name: str,
    candidate_summary: str,
    linkedin_url: str = "",
    portfolio_url: str = "",
    use_ai: bool = True,
) -> int:
    outreach_df = read_outreach_tracker_df(user_id)
    if outreach_df.empty:
        return 0

    suggestion_updated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    refreshed = 0
    for _, row in outreach_df.iterrows():
        row_id = str(row.get("_row_id", "") or "").strip()
        if not row_id:
            continue
        suggestion = build_outreach_next_step_suggestion(
            row.to_dict(),
            sender_name=sender_name,
            candidate_summary=candidate_summary,
            linkedin_url=linkedin_url,
            portfolio_url=portfolio_url,
            use_ai=use_ai,
        )
        update_outreach_row(
            user_id,
            row_id,
            {
                "Suggested Action": suggestion["suggested_action"],
                "Suggested Follow-up": suggestion["suggested_followup"],
                "Suggestion Updated At": suggestion_updated_at,
            },
        )
        refreshed += 1
    return refreshed


def mark_outreach_rows(df: pd.DataFrame, emails: set, status: str) -> pd.DataFrame:
    updated = df.copy()
    if "Send Status" not in updated.columns:
        updated["Send Status"] = ""
    normalized_targets = {normalize_outreach_email(email) for email in emails if normalize_outreach_email(email)}
    if not normalized_targets:
        return updated
    email_series = updated.get("Email", pd.Series(dtype=str)).fillna("").astype(str).map(normalize_outreach_email)
    mask = email_series.isin(normalized_targets)
    if "Send" in updated.columns:
        updated.loc[mask, "Send"] = False
    updated.loc[mask, "Send Status"] = status
    return updated


def attachment_status_label(attachment: dict) -> str:
    return "with resume attached" if attachment.get("attachment_bytes") else "without resume attachment"


def plain_text_to_html(text: str) -> str:
    return escape(text or "").replace("\n", "<br>")


def build_bulk_outreach_rows(
    company_names: list[str],
    preferred_role: str,
    linkedin_url: str,
    portfolio_url: str,
    candidate_summary: str,
    resume_text: str,
    sender_name: str,
    location: str,
    titles: list[str],
    seniorities: list[str],
    max_contacts: int = 6,
    use_ai_personalization: bool = True,
    discovery_metadata: Optional[dict] = None,
    blocked_emails: Optional[set] = None,
    blocked_contact_keys: Optional[set] = None,
) -> list[dict]:
    client = HunterClient()
    discovery_metadata = discovery_metadata or {}
    blocked_emails = set(blocked_emails or set())
    blocked_contact_keys = set(blocked_contact_keys or set())
    rows = []
    seen_companies = set()
    for company in company_names:
        company = company.strip()
        if not company:
            continue
        company_identity = company_key(company)
        if company_identity in seen_companies:
            continue
        seen_companies.add(company_identity)
        meta = discovery_metadata.get(company_key(company), {})
        matched_role = str(meta.get("example_role") or meta.get("role_evidence") or "").strip()
        primary_focus = str(meta.get("primary_focus") or "").strip()
        company_domain = str(meta.get("company_domain") or "").strip()
        company_website = str(meta.get("company_website") or "").strip()
        sponsorship = {
            "sponsorship_signal": meta.get("sponsorship_signal"),
            "sponsorship_source": meta.get("sponsorship_source"),
            "sponsorship_lookup_url": meta.get("sponsorship_lookup_url"),
        }
        if not sponsorship["sponsorship_signal"]:
            sponsorship = sponsorship_signal_for_company(company)
        try:
            contacts = client.search_people_low_credit(
                company_name=company,
                company_domain=None,
                preferred_role=preferred_role,
                titles=titles,
                seniorities=seniorities,
                per_page=max_contacts,
            )
            filtered_contacts, skipped_duplicates = filter_new_contacts(
                company,
                contacts,
                blocked_emails=blocked_emails,
                blocked_identity_keys=blocked_contact_keys,
            )
            best = client.pick_best_contact(list(filtered_contacts)) or {}
        except Exception:
            contacts = []
            best = {}
            skipped_duplicates = 0
        company_domain = best.get("company_domain", "") or company_domain
        context_parts = [
            primary_focus,
            matched_role,
            meta.get("role_evidence", ""),
            meta.get("snippet", ""),
        ]
        context = " ".join(str(part).strip() for part in context_parts if str(part).strip())[:1400]
        if len(context) < 400:
            scraped_context = scrape_company_context(company, company_domain or company_website)
            context = " ".join(part for part in [context, scraped_context] if part).strip()[:2200]
        reason = company_reason_from_context(
            company,
            preferred_role,
            context,
            matched_role=matched_role,
            primary_focus=primary_focus,
        )
        pack = build_cold_email(
            candidate_summary=candidate_summary,
            company_name=company,
            role_title=preferred_role,
            recipient_name=best.get("name", ""),
            recipient_title=best.get("title", ""),
            linkedin_url=linkedin_url,
            portfolio_url=portfolio_url,
            resume_text=resume_text,
            company_reason=reason,
            sender_name=sender_name,
            company_context=context,
            matched_role=matched_role,
            primary_focus=primary_focus,
            discovery_source=str(meta.get("source", "")),
            role_source_url=str(meta.get("source_url", "")),
            use_ai_personalization=use_ai_personalization,
        )
        duplicate_check = "New contact"
        if contacts and not best:
            duplicate_check = "All matching contacts were already contacted or queued"
        elif skipped_duplicates:
            duplicate_check = f"Filtered {skipped_duplicates} duplicate contact(s)"

        selected_email = normalize_outreach_email(best.get("email", ""))
        if selected_email:
            blocked_emails.add(selected_email)
        blocked_contact_keys.update(contact_identity_keys(company, best.get("name", ""), best.get("email", "")))
        rows.append({
            "Send": bool(best.get("email")),
            "Company": company,
            "Company Domain": company_domain,
            "Company Website": company_website,
            "Matched Role": matched_role,
            "Discovery Focus": primary_focus,
            "Contact Name": best.get("name", ""),
            "Contact Title": best.get("title", ""),
            "Email": best.get("email", ""),
            "Verification": best.get("verification_status", "not found"),
            "Local Score": best.get("local_score", 0),
            "Subject": pack["subject"],
            "Body": pack["body"],
            "Follow-up": pack["followup"],
            "Personalization": pack.get("personalization_angle", reason),
            "Company Reason": reason,
            "Discovery Source": meta.get("source", "manual"),
            "Open Role Evidence": meta.get("role_evidence", ""),
            "Role Source URL": meta.get("source_url", ""),
            "Duplicate Check": duplicate_check,
            "Sponsorship Signal": sponsorship["sponsorship_signal"],
            "Sponsorship Source": sponsorship["sponsorship_source"],
            "Sponsorship Lookup": sponsorship["sponsorship_lookup_url"],
            "Source": f"hunter+web+{pack.get('personalization_source', 'fallback')}",
        })
    return rows

# -------------------------
# TAB 1: JOB AGENT
# -------------------------
with tabs[0]:
    left, right = st.columns([1, 1])

    with left:
        st.markdown("### 1. Add a job to analyze")
        st.caption("Paste a job description or URL, analyze fit, then generate tailored assets.")
        st.markdown('<div class="card">', unsafe_allow_html=True)
        job_url = st.text_input("Job posting URL (optional)", placeholder="https://…")
        job_desc = st.text_area("Or paste job description", height=170, placeholder="Paste the job description…")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("### 2. Choose your resume strategy")
        st.markdown('<div class="card">', unsafe_allow_html=True)

        resumes_df = list_resumes(user_id)
        saved_resume_payloads = list_resume_payloads(user_id)

        if job_desc.strip() or job_url.strip():
            with st.expander("🎯 Best resume recommendation", expanded=False):
                recommendation_source = job_desc.strip()
                if not recommendation_source and job_url:
                    try:
                        recommendation_source = fetch_job_post(job_url)
                    except Exception:
                        recommendation_source = ""

                if saved_resume_payloads and recommendation_source:
                    ranked_resumes = rank_resume_versions(recommendation_source, saved_resume_payloads)
                    best_resume = ranked_resumes[0]

                    st.markdown(
                        f"""
                        <div class="soft-card">
                          <div class="section-kicker">Recommended resume</div>
                          <div><b>{best_resume['name']}</b></div>
                          <div class="muted small">Match score: <b>{best_resume['score']} / 100</b></div>
                          <div class="muted small">Recommendation: <b>{best_resume['priority']}</b></div>
                          <div class="muted small">{best_resume['summary']}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

                    if st.button("✅ Use recommended resume", use_container_width=True):
                        set_active_resume(load_resume_payload(user_id, best_resume["resume_id"]))
                        st.success(f"Loaded recommended resume: {best_resume['name']}")
                        st.rerun()

                elif not saved_resume_payloads:
                    st.caption("Save at least one resume version to get automatic resume recommendations.")
                else:
                    st.caption("Paste a job description or URL to compare your saved resume versions.")

        if not resumes_df.empty:
            rid_list = resumes_df["resume_id"].tolist()
            selected_rid = st.selectbox(
                "Saved resumes",
                rid_list,
                format_func=lambda rid: resumes_df.loc[resumes_df["resume_id"] == rid, "name"].values[0],
            )

            cA, cB, cC = st.columns([1, 1, 1])
            with cA:
                if st.button("Use selected", use_container_width=True):
                    set_active_resume(load_resume_payload(user_id, selected_rid))
                    st.success("Loaded ✅")
                    st.rerun()
            with cB:
                if st.button("Delete selected", use_container_width=True):
                    delete_resume_version(user_id, selected_rid)
                    st.success("Deleted ✅")
                    st.rerun()
            with cC:
                st.caption("Tip: store multiple versions (DS / DE / Analyst).")

        st.divider()
        resume_file = st.file_uploader("Upload resume (PDF or TXT)", type=["pdf", "txt"])

        if resume_file is not None:
            try:
                resume_bytes = resume_file.getvalue()
                if resume_file.type == "application/pdf":
                    parsed = read_pdf(io.BytesIO(resume_bytes))
                else:
                    parsed = resume_bytes.decode("utf-8", errors="ignore").strip()

                st.session_state["resume_text"] = parsed
                st.session_state["resume_name"] = resume_file.name
                st.session_state["resume_bytes"] = resume_bytes
                st.session_state["resume_filename"] = resume_file.name
                st.session_state["resume_mime_type"] = resume_file.type or infer_attachment_mime(resume_file.name)

                if st.button("Save as new version", use_container_width=True):
                    save_resume_version(
                        user_id,
                        resume_file.name,
                        parsed,
                        file_name=resume_file.name,
                        file_bytes=resume_bytes,
                        mime_type=st.session_state["resume_mime_type"],
                    )
                    st.success("Saved ✅")
                    st.rerun()

            except Exception as e:
                st.error(f"Could not read resume file. Error: {e}")

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("🧹 Clear active resume", use_container_width=True):
                st.session_state["resume_text"] = ""
                st.session_state["resume_name"] = ""
                st.session_state["resume_bytes"] = b""
                st.session_state["resume_filename"] = ""
                st.session_state["resume_mime_type"] = ""
                st.success("Cleared ✅")
                st.rerun()
        with c2:
            if st.session_state["resume_name"]:
                attachable = "attachable" if st.session_state.get("resume_bytes") else "text-only"
                st.caption(f"Active: **{st.session_state['resume_name']}** ({attachable})")
            else:
                st.caption("No active resume selected.")

        extra_notes = st.text_area("Extra notes (optional)", height=100, placeholder="Anything to emphasize…")
        st.markdown("</div>", unsafe_allow_html=True)

        st.markdown("### 3. Review agent decisions")
        with st.expander("📌 Application settings and analysis", expanded=True):
            autofill_clicked = st.button("✨ Auto-fill from job post")

            autofill_source_text = job_desc.strip()
            if not autofill_source_text and job_url:
                try:
                    autofill_source_text = fetch_job_post(job_url)
                except Exception:
                    autofill_source_text = ""

            if autofill_clicked:
                if not autofill_source_text:
                    st.warning("Paste a job description or provide a fetchable URL to auto-fill.")
                else:
                    with st.spinner("Extracting company / role / location…"):
                        fields = extract_fields(autofill_source_text)
                    st.session_state["company_input"] = fields.get("company", "") or ""
                    st.session_state["role_input"] = fields.get("role", "") or ""
                    st.session_state["location_input"] = fields.get("location", "") or ""
                    st.success("Auto-filled ✅")
                    st.rerun()

            company = st.text_input("Company", key="company_input")
            role = st.text_input("Role / Title", key="role_input")
            location = st.text_input("Location (optional)", key="location_input")

            colA, colB = st.columns(2)
            with colA:
                status = st.selectbox(
                    "Status",
                    ["Interested", "Applied", "OA", "Interview", "Offer", "Rejected", "Ghosted"],
                    index=0,
                )
                date_applied = st.date_input("Date Applied", value=date.today())
            with colB:
                follow_up_date = st.date_input("Follow-up Date", value=date.today() + timedelta(days=7))
                log_to_tracker = st.checkbox("Log this to your tracker", value=True)

            contact_name = st.text_input("Contact Name (optional)", key="contact_name_input")
            contact_link = st.text_input("Contact Link (optional)", key="contact_link_input")

            fit_col1, fit_col2 = st.columns([1, 1])
            with fit_col1:
                analyze_clicked = st.button("📊 Analyze job fit", use_container_width=True)
            with fit_col2:
                st.caption("Scores are heuristic + keyword based, useful for prioritization.")

            fit_source_text = job_desc.strip()
            if not fit_source_text and job_url:
                try:
                    fit_source_text = fetch_job_post(job_url)
                except Exception:
                    fit_source_text = ""

            if analyze_clicked:
                resume_text = (st.session_state.get("resume_text") or "").strip()
                if not resume_text:
                    st.warning("Load a resume first to analyze fit.")
                elif not fit_source_text:
                    st.warning("Paste a job description or provide a fetchable URL to analyze fit.")
                else:
                    fit = analyze_fit(fit_source_text, resume_text)
                    st.session_state["fit_score"] = fit["score"]
                    st.session_state["fit_priority"] = fit["priority"]
                    st.session_state["fit_summary"] = fit["summary"]
                    st.session_state["fit_matched"] = fit["matched_skills"]
                    st.session_state["fit_missing"] = fit["missing_skills"]

            if st.session_state.get("fit_priority"):
                score = int(st.session_state.get("fit_score") or 0)
                st.markdown(f"**Fit Score:** {score}/100  |  **Priority:** {st.session_state.get('fit_priority', '')}")
                st.caption(st.session_state.get("fit_summary", ""))
                mskills = st.session_state.get("fit_matched") or []
                xskills = st.session_state.get("fit_missing") or []
                if mskills:
                    st.write("**Matched skills:** " + ", ".join(mskills))
                if xskills:
                    st.write("**Missing / weaker skills:** " + ", ".join(xskills))

        run = st.button("🚀 Generate Application Pack", type="primary")

    with right:
        st.markdown("### Generated application assets")
        st.caption("Your tailored output and agent recommendation will appear here.")

        if run:
            resume_text = (st.session_state.get("resume_text") or "").strip()
            if not resume_text:
                st.error("No active resume. Upload or select one from the library.")
                st.stop()

            job_post_text = ""
            if job_url:
                try:
                    job_post_text = fetch_job_post(job_url)
                except Exception as e:
                    st.warning(f"Could not fetch job URL. Paste description instead. Error: {e}")

            if not job_post_text:
                job_post_text = job_desc.strip()

            if not job_post_text:
                st.error("Provide a job URL that can be fetched OR paste the job description.")
                st.stop()

            company_eff = (st.session_state.get("company_input") or "").strip()
            role_eff = (st.session_state.get("role_input") or "").strip()
            location_eff = (st.session_state.get("location_input") or "").strip()

            if not company_eff or not role_eff or not location_eff:
                with st.spinner("Auto-detecting Company/Role/Location from job post…"):
                    fields = extract_fields(job_post_text)

                company_eff = company_eff or (fields.get("company", "") or "").strip()
                role_eff = role_eff or (fields.get("role", "") or "").strip()
                location_eff = location_eff or (fields.get("location", "") or "").strip()

                if not company_eff or not role_eff or not location_eff:
                    st.warning(
                        "Could not confidently detect Company/Role/Location. "
                        "You can still generate, but logging may be incomplete."
                    )

            if not st.session_state.get("fit_priority"):
                auto_fit = analyze_fit(job_post_text, resume_text)
                st.session_state["fit_score"] = auto_fit["score"]
                st.session_state["fit_priority"] = auto_fit["priority"]
                st.session_state["fit_summary"] = auto_fit["summary"]
                st.session_state["fit_matched"] = auto_fit["matched_skills"]
                st.session_state["fit_missing"] = auto_fit["missing_skills"]

            with st.spinner("Generating…"):
                output = generate_application_materials(
                    job_post=safe_clip(job_post_text, 20000),
                    resume_text=safe_clip(resume_text, 20000),
                    extra_notes=extra_notes,
                )

            st.success("Generated ✅")

            next_action_preview = recommend_follow_up_action(status, str(follow_up_date), today=date.today())

            st.markdown(
                render_decision_card(
                    priority=st.session_state.get("fit_priority", ""),
                    fit_score=str(st.session_state.get("fit_score", "")),
                    fit_summary=st.session_state.get("fit_summary", ""),
                    next_action=next_action_preview,
                    resume_name=st.session_state.get("resume_name", ""),
                ),
                unsafe_allow_html=True,
            )

            with st.expander("📄 Application Pack", expanded=True):
                st.text_area("", value=output, height=520)

            d1, d2 = st.columns(2)
            with d1:
                st.download_button(
                    "⬇️ Download .txt",
                    data=output.encode("utf-8"),
                    file_name="application_pack.txt",
                    mime="text/plain",
                    use_container_width=True,
                )
            with d2:
                st.download_button(
                    "⬇️ Download .docx",
                    data=make_docx(output),
                    file_name="application_pack.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True,
                )

            if log_to_tracker:
                try:
                    append_row(
                        user_id,
                        {
                            "Date Applied": str(date_applied),
                            "Company": company_eff,
                            "Role": role_eff,
                            "Job Link": job_url or "",
                            "Location": location_eff,
                            "Status": status,
                            "Fit Score": str(st.session_state.get("fit_score", "")),
                            "Priority": st.session_state.get("fit_priority", ""),
                            "Fit Summary": st.session_state.get("fit_summary", ""),
                            "Resume Used": st.session_state.get("resume_name", ""),
                            "Follow-up Date": str(follow_up_date),
                            "Contact Name": contact_name,
                            "Contact Link": contact_link,
                            "Notes": extra_notes,
                        },
                    )
                    st.success("Logged to your tracker ✅")
                except Exception as e:
                    st.error(f"Could not write to tracker DB. Error: {e}")
        else:
            st.info("Add a job, choose a resume, and click Generate Application Pack.")


# ---------# -------------------------
# TAB 2: COLD OUTREACH
# -------------------------
with tabs[1]:
    st.markdown("### 📧 Cold Outreach Campaigns")
    st.caption("Attach your resume, personalize emails to each company, and run individual, bulk, or daily company discovery using Hunter plus hybrid structured/open-web role discovery.")
    st.caption(f"Safety guardrail: bulk sends are capped at {MAX_EMAILS_PER_BATCH} emails per click.")
    cold_outreach_gmail = get_user_gmail_connection(user_id)
    if cold_outreach_gmail.get("connected"):
        st.caption(f"Sending Gmail: {cold_outreach_gmail.get('gmail_email','')}")
    else:
        st.warning("Connect Gmail in Account settings before sending outreach. Each user now sends from their own connected Gmail account.")

    if "outreach_preferred_role" not in st.session_state:
        st.session_state["outreach_preferred_role"] = st.session_state.get("role_input", "")
    if "outreach_target_location" not in st.session_state:
        st.session_state["outreach_target_location"] = st.session_state.get("location_input", "United States, Remote")
    if "outreach_companies_text" not in st.session_state:
        st.session_state["outreach_companies_text"] = "Adobe\nVisa\nMicrosoft\nAmazon"
    if "apollo_linkedin_profile" not in st.session_state:
        st.session_state["apollo_linkedin_profile"] = ""
    if "apollo_portfolio_website" not in st.session_state:
        st.session_state["apollo_portfolio_website"] = ""
    if "gmail_sender_name" not in st.session_state:
        st.session_state["gmail_sender_name"] = "Sai Praneeth"

    profile_left, profile_right = st.columns([1.1, 1])
    with profile_left:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        preferred_role = st.text_input("Preferred role", key="outreach_preferred_role")
        target_location = st.text_input("Target location", key="outreach_target_location")
        outreach_titles = st.multiselect(
            "Who should Hunter search for?",
            ["Campus Recruiter", "University Recruiter", "Recruiter", "Talent Acquisition Partner", "Hiring Manager", "Analytics Manager", "Data Science Manager", "Engineering Manager"],
            default=["Campus Recruiter", "Recruiter", "Talent Acquisition Partner", "Hiring Manager"],
            key="outreach_titles",
        )
        outreach_seniority = st.multiselect(
            "Seniority filter",
            ["intern", "junior", "manager", "director", "vp", "executive"],
            default=["manager", "director"],
            key="outreach_seniority",
        )
        discovery_c1, discovery_c2, discovery_c3 = st.columns([1, 1, 1])
        with discovery_c1:
            include_startups = st.checkbox("Include startups", value=True, key="include_startups_discovery")
        with discovery_c2:
            only_open_roles = st.checkbox("Open roles only", value=True, key="open_roles_only_discovery")
        with discovery_c3:
            discovery_limit = st.slider("Companies", min_value=8, max_value=30, value=16, step=2, key="discovery_limit")
        if st.button("🔎 Discover companies for this role", use_container_width=True):
            if not preferred_role.strip():
                st.error("Enter a preferred role first.")
            else:
                with st.spinner("Searching job boards and startup sources for matching open roles..."):
                    discovered_companies = discover_companies_from_web(
                        preferred_role,
                        target_location,
                        max_results=discovery_limit,
                        include_startups=include_startups,
                        only_open_roles=only_open_roles,
                    )
                st.session_state["role_discovery_results"] = discovered_companies
                if discovered_companies:
                    st.session_state["outreach_companies_text"] = "\n".join(
                        item["company"] for item in discovered_companies
                    )
                    startup_count = sum("startup" in str(item.get("source", "")).lower() for item in discovered_companies)
                    sponsor_count = sum(item.get("sponsorship_signal") == "Likely historical H-1B sponsor" for item in discovered_companies)
                    st.success(
                        f"Found {len(discovered_companies)} companies, including {startup_count} startup target(s) and {sponsor_count} likely sponsor signal(s)."
                    )
                else:
                    st.warning("No companies found. Try broadening the role or location.")
        companies_text = st.text_area(
            "Target companies (one per line or comma-separated)",
            height=140,
            key="outreach_companies_text",
        )
        role_discovery_results = st.session_state.get("role_discovery_results") or []
        if role_discovery_results:
            source_counts = pd.Series([item.get("source", "unknown") for item in role_discovery_results]).value_counts()
            st.caption(
                "Last discovery: "
                + ", ".join(f"{source}: {count}" for source, count in source_counts.items())
            )
            with st.expander("View discovered company evidence", expanded=False):
                evidence_df = pd.DataFrame(role_discovery_results)
                visible_cols = [
                    "company",
                    "source",
                    "score",
                    "primary_focus",
                    "example_role",
                    "sponsorship_signal",
                    "sponsorship_lookup_url",
                    "role_evidence",
                    "source_url",
                ]
                st.dataframe(
                    evidence_df[[col for col in visible_cols if col in evidence_df.columns]],
                    use_container_width=True,
                    hide_index=True,
                )
        st.markdown('</div>', unsafe_allow_html=True)

    with profile_right:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        default_summary = "MS in Information Systems at UMD with experience in analytics, SQL, Python, dashboards, and AI-enabled applications."
        if st.session_state.get("resume_text"):
            highlights = extract_resume_highlights(st.session_state.get("resume_text", ""), 2)
            if highlights:
                default_summary += " Relevant highlights: " + " | ".join(highlights)
        if "outreach_candidate_summary" not in st.session_state:
            st.session_state["outreach_candidate_summary"] = default_summary
        candidate_summary = st.text_area("Candidate summary", height=130, key="outreach_candidate_summary")
        linkedin_url = st.text_input("LinkedIn profile URL", key="apollo_linkedin_profile")
        portfolio_url = st.text_input("Portfolio website URL", key="apollo_portfolio_website")
        sender_name = st.text_input("Sender name", key="gmail_sender_name")
        if st.button("💾 Save outreach profile", use_container_width=True):
            try:
                with st.spinner("Saving outreach profile defaults..."):
                    update_user_profile(
                        user_id,
                        sender_name=sender_name,
                        linkedin_url=linkedin_url,
                        portfolio_url=portfolio_url,
                        target_role=preferred_role,
                        target_location=target_location,
                        candidate_summary=candidate_summary,
                    )
                st.success("Profile saved ✅")
            except Exception as e:
                st.error(f"Could not save profile. Error: {e}")
        st.caption("Your active uploaded resume will be attached automatically when an email is sent.")
        st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    st.markdown("#### 📎 Outreach resume attachment")
    st.caption("Choose the exact resume that should be attached to outreach emails. This can be different from the resume you used in the Job Agent tab.")
    attach_left, attach_right = st.columns([1.2, 1])
    with attach_left:
        outreach_resume_file = st.file_uploader(
            "Upload resume for outreach (PDF or TXT)",
            type=["pdf", "txt"],
            key="outreach_resume_upload",
        )
        if outreach_resume_file is not None:
            try:
                with st.spinner("Preparing resume attachment..."):
                    resume_bytes = outreach_resume_file.getvalue()
                    if outreach_resume_file.type == "application/pdf":
                        parsed = read_pdf(io.BytesIO(resume_bytes))
                    else:
                        parsed = resume_bytes.decode("utf-8", errors="ignore").strip()

                    set_active_resume(
                        {
                            "name": outreach_resume_file.name,
                            "resume_text": parsed,
                            "file_name": outreach_resume_file.name,
                            "file_bytes": resume_bytes,
                            "mime_type": outreach_resume_file.type or infer_attachment_mime(outreach_resume_file.name),
                        }
                    )
                st.success(f"Using {outreach_resume_file.name} for outreach attachments ✅")
                if st.button("Save outreach resume to library", use_container_width=True):
                    save_resume_version(
                        user_id,
                        outreach_resume_file.name,
                        parsed,
                        file_name=outreach_resume_file.name,
                        file_bytes=resume_bytes,
                        mime_type=st.session_state.get("resume_mime_type", ""),
                    )
                    st.success("Saved to resume library ✅")
            except Exception as e:
                st.error(f"Could not prepare resume attachment. Error: {e}")
    with attach_right:
        outreach_resumes_df = list_resumes(user_id)
        if not outreach_resumes_df.empty:
            outreach_rid = st.selectbox(
                "Or use a saved resume",
                outreach_resumes_df["resume_id"].tolist(),
                format_func=lambda rid: outreach_resumes_df.loc[outreach_resumes_df["resume_id"] == rid, "name"].values[0],
                key="outreach_saved_resume_select",
            )
            if st.button("Use saved resume for outreach", use_container_width=True):
                with st.spinner("Loading saved resume attachment..."):
                    set_active_resume(load_resume_payload(user_id, outreach_rid))
                st.success("Outreach resume loaded ✅")
                st.rerun()
        else:
            st.caption("No saved resumes yet. Upload one here or save a resume from the Job Agent tab.")

        if st.session_state.get("resume_name"):
            attachment_label = "will be attached" if st.session_state.get("resume_bytes") else "is text-only and cannot be attached"
            st.info(f"Current outreach resume: {st.session_state['resume_name']} ({attachment_label}).")
        else:
            st.warning("No outreach resume selected yet. Emails can still send, but no resume will be attached.")
    st.markdown('</div>', unsafe_allow_html=True)

    st.markdown('<div class="card">', unsafe_allow_html=True)
    niche_enabled = st.checkbox(
        "Generate a niche, company-specific draft for every recipient",
        value=True,
        help="Uses the company context, recipient title, target role, and resume highlights. If AI drafting fails, the app falls back to a safe tailored template.",
    )
    st.caption("When enabled, bulk and daily campaign building may take longer because each company gets its own draft.")
    st.markdown('</div>', unsafe_allow_html=True)

    mode_tabs = st.tabs(["Individual outreach", "Bulk campaign", "Daily discovery", "Outreach Tracker"])

    with mode_tabs[0]:
        col1, col2 = st.columns([1.05, 1])
        with col1:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            individual_company = st.text_input("Company", key="individual_company", value=st.session_state.get("company_input", ""))
            individual_domain = st.text_input("Company domain (optional)", key="individual_domain", placeholder="example.com")
            find_individual = st.button("🔎 Find best contact", type="primary", use_container_width=True)
            if find_individual:
                if not individual_company.strip() and not individual_domain.strip():
                    st.error("Enter a company or domain.")
                else:
                    try:
                        with st.spinner("Searching Hunter for the best contact..."):
                            client = HunterClient()
                            results = client.search_people_low_credit(
                                company_name=individual_company.strip() or individual_domain.strip(),
                                company_domain=individual_domain.strip() or None,
                                preferred_role=preferred_role,
                                titles=outreach_titles,
                                seniorities=outreach_seniority,
                                per_page=6,
                            )
                        history_emails, history_keys = current_outreach_history(user_id)
                        company_label = individual_company.strip() or individual_domain.strip()
                        filtered_results, skipped_duplicates = filter_new_contacts(
                            company_label,
                            results,
                            blocked_emails=history_emails,
                            blocked_identity_keys=history_keys,
                        )
                        st.session_state["individual_contacts"] = list(filtered_results)
                        if filtered_results:
                            st.success(f"Found {len(filtered_results)} new contact(s).")
                        else:
                            st.warning("Hunter only returned contacts that were already contacted or queued.")
                        if skipped_duplicates:
                            st.info(f"Filtered out {skipped_duplicates} duplicate or previously-contacted contact(s).")
                    except Exception as e:
                        st.error(str(e))

            individual_contacts = st.session_state.get("individual_contacts", [])
            if individual_contacts:
                labels = {}
                for p in individual_contacts:
                    key = p.get("email") or p.get("hunter_person_id") or p.get("name")
                    labels[f"{p.get('name','Unknown')} — {p.get('title','Unknown')} | {p.get('verification_status','unknown')} | score {p.get('local_score',0)}"] = key
                chosen = st.selectbox("Choose contact", list(labels.keys()), key="individual_contact_choice")
                chosen_key = labels[chosen]
                lead = next((p for p in individual_contacts if (p.get("email") or p.get("hunter_person_id") or p.get("name")) == chosen_key), {})
                st.session_state["individual_best_contact"] = lead
                if st.button("📝 Generate personalized draft", use_container_width=True):
                    with st.spinner("Researching the company and drafting a personalized note..."):
                        context = scrape_company_context(individual_company or lead.get("company_name", ""), individual_domain or lead.get("company_domain", ""))
                        reason = company_reason_from_context(individual_company or lead.get("company_name", ""), preferred_role, context)
                        pack = build_cold_email(
                            candidate_summary=candidate_summary,
                            company_name=individual_company or lead.get("company_name", ""),
                            role_title=preferred_role,
                            recipient_name=lead.get("name", ""),
                            recipient_title=lead.get("title", ""),
                            linkedin_url=linkedin_url,
                            portfolio_url=portfolio_url,
                            resume_text=st.session_state.get("resume_text", ""),
                            company_reason=reason,
                            sender_name=sender_name,
                            company_context=context,
                            use_ai_personalization=niche_enabled,
                        )
                    st.session_state["individual_subject"] = pack["subject"]
                    st.session_state["individual_body"] = pack["body"]
                    st.session_state["individual_followup"] = pack["followup"]
                    st.session_state["individual_company_reason"] = pack.get("personalization_angle", reason)
                    st.success("Draft ready.")
            st.markdown('</div>', unsafe_allow_html=True)

        with col2:
            st.markdown('<div class="card">', unsafe_allow_html=True)
            st.text_input("Subject", key="individual_subject")
            st.text_area("Email body", key="individual_body", height=240)
            st.text_area("Follow-up", key="individual_followup", height=120)
            st.text_area("Company-specific reason used", key="individual_company_reason", height=80)
            if st.button("📨 Send individual email", use_container_width=True):
                lead = st.session_state.get("individual_best_contact", {})
                recipient_email = (lead.get("email") or "").strip()
                if not recipient_email:
                    st.error("Selected contact has no email.")
                else:
                    company_label = individual_company or lead.get("company_name", "")
                    history_emails, history_keys = current_outreach_history(user_id)
                    if is_duplicate_contact(
                        company_label,
                        lead.get("name", ""),
                        recipient_email,
                        blocked_emails=history_emails,
                        blocked_identity_keys=history_keys,
                    ):
                        st.error("This contact has already been emailed or queued. Pick a new contact to avoid duplicates.")
                        st.stop()
                    try:
                        log_failed = False
                        with st.spinner("Sending email through Gmail..."):
                            sender = GmailSender.from_user(user_id)
                            attachment = active_resume_attachment()
                            if not attachment.get("attachment_bytes"):
                                st.warning("No attachable resume file is active; sending without an attachment.")
                            send_result = sender.send_email(
                                to=recipient_email,
                                subject=st.session_state.get("individual_subject", ""),
                                body_text=st.session_state.get("individual_body", ""),
                                body_html=plain_text_to_html(st.session_state.get("individual_body", "")),
                                **attachment,
                            )
                        try:
                            log_sent_outreach_to_tracker(
                                user_id,
                                company=company_label,
                                preferred_role=preferred_role,
                                target_location=target_location,
                                contact_name=str(lead.get("name", "")),
                                contact_link=str(lead.get("linkedin") or lead.get("linkedin_url") or ""),
                                email=recipient_email,
                                subject=st.session_state.get("individual_subject", ""),
                                personalization=st.session_state.get("individual_company_reason", ""),
                                sponsorship_signal="Unknown / verify",
                                resume_name=st.session_state.get("resume_name", ""),
                                send_source="individual_outreach",
                                gmail_message_id=str(send_result.get("id", "") or ""),
                                gmail_thread_id=str(send_result.get("threadId", "") or ""),
                            )
                        except Exception:
                            log_failed = True
                        remember_outreach_contact(company_label, lead.get("name", ""), recipient_email)
                        st.success(f"Sent to {recipient_email} {attachment_status_label(attachment)} ✅")
                        if log_failed:
                            st.warning("The email sent successfully, but outreach tracker logging failed. Duplicate protection will still work for this session.")
                    except Exception as e:
                        st.error(str(e))
            st.markdown('</div>', unsafe_allow_html=True)

    with mode_tabs[1]:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        companies = [x.strip() for chunk in companies_text.splitlines() for x in chunk.split(",") if x.strip()]
        role_discovery_metadata = {
            company_key(item.get("company", "")): item
            for item in st.session_state.get("role_discovery_results", [])
        }
        build_bulk = st.button("🚀 Build bulk campaign", type="primary", use_container_width=True)
        if build_bulk:
            try:
                with st.spinner("Building campaign: searching contacts, researching companies, and drafting emails..."):
                    history_emails, history_keys = current_outreach_history(user_id)
                    rows = build_bulk_outreach_rows(
                        company_names=companies,
                        preferred_role=preferred_role,
                        linkedin_url=linkedin_url,
                        portfolio_url=portfolio_url,
                        candidate_summary=candidate_summary,
                        resume_text=st.session_state.get("resume_text", ""),
                        sender_name=sender_name,
                        location=target_location,
                        titles=outreach_titles,
                        seniorities=outreach_seniority,
                        max_contacts=6,
                        use_ai_personalization=niche_enabled,
                        discovery_metadata=role_discovery_metadata,
                        blocked_emails=history_emails,
                        blocked_contact_keys=history_keys,
                    )
                st.session_state["bulk_queue_df"] = pd.DataFrame(rows)
                st.success(f"Built campaign for {len(rows)} companies.")
            except Exception as e:
                st.error(str(e))

        bulk_df = st.session_state.get("bulk_queue_df")
        if isinstance(bulk_df, pd.DataFrame) and not bulk_df.empty:
            edited_bulk = st.data_editor(
                bulk_df,
                use_container_width=True,
                num_rows="fixed",
                column_config={
                    "Company Website": st.column_config.LinkColumn("Company Website"),
                    "Role Source URL": st.column_config.LinkColumn("Role Source URL"),
                    "Sponsorship Lookup": st.column_config.LinkColumn("Sponsorship Lookup"),
                },
                key="bulk_campaign_editor",
            )
            c1, c2 = st.columns(2)
            with c1:
                if st.button("📨 Send selected emails", use_container_width=True):
                    try:
                        selected_rows = [
                            row for _, row in edited_bulk.iterrows()
                            if bool(row.get("Send")) and str(row.get("Email", "")).strip()
                        ]
                        history_emails, _ = current_outreach_history(user_id)
                        send_rows, skipped_rows = unique_send_rows(
                            selected_rows,
                            already_contacted_emails=history_emails,
                        )
                        if not send_rows:
                            st.warning("No new emails to send. The selected rows were duplicates or were already contacted.")
                            st.stop()
                        if len(send_rows) > MAX_EMAILS_PER_BATCH:
                            st.error(f"Select {MAX_EMAILS_PER_BATCH} or fewer emails per batch to reduce accidental bulk sends.")
                            st.stop()
                        with st.spinner("Sending selected emails through Gmail..."):
                            sender = GmailSender.from_user(user_id)
                            attachment = active_resume_attachment()
                            if not attachment.get("attachment_bytes"):
                                st.warning("No attachable resume file is active; selected emails will send without attachments.")
                            sent = 0
                            sent_emails = set()
                            log_failures = 0
                            for row in send_rows:
                                email = str(row.get("Email", "")).strip()
                                send_result = sender.send_email(
                                    to=email,
                                    subject=str(row.get("Subject", "")),
                                    body_text=str(row.get("Body", "")),
                                    body_html=plain_text_to_html(str(row.get("Body", ""))),
                                    **attachment,
                                )
                                try:
                                    log_sent_outreach_to_tracker(
                                        user_id,
                                        company=str(row.get("Company", "")),
                                        preferred_role=preferred_role,
                                        target_location=target_location,
                                        contact_name=str(row.get("Contact Name", "")),
                                        contact_link="",
                                        email=email,
                                        subject=str(row.get("Subject", "")),
                                        personalization=str(row.get("Personalization", row.get("Company Reason", ""))),
                                        sponsorship_signal=str(row.get("Sponsorship Signal", "Unknown / verify")),
                                        resume_name=st.session_state.get("resume_name", ""),
                                        send_source="bulk_campaign",
                                        role_source_url=str(row.get("Role Source URL", "")),
                                        gmail_message_id=str(send_result.get("id", "") or ""),
                                        gmail_thread_id=str(send_result.get("threadId", "") or ""),
                                    )
                                except Exception:
                                    log_failures += 1
                                remember_outreach_contact(str(row.get("Company", "")), str(row.get("Contact Name", "")), email)
                                sent_emails.add(email)
                                sent += 1
                        st.session_state["bulk_queue_df"] = mark_outreach_rows(
                            edited_bulk,
                            sent_emails,
                            "Sent this session",
                        )
                        st.success(f"Sent {sent} email(s) {attachment_status_label(attachment)} ✅")
                        if skipped_rows:
                            st.warning(f"Skipped {len(skipped_rows)} duplicate or already-contacted email(s).")
                        if log_failures:
                            st.warning(f"{log_failures} sent email(s) could not be auto-logged to the outreach tracker.")
                    except Exception as e:
                        st.error(str(e))
            with c2:
                if st.button("💾 Save selected to outreach tracker", use_container_width=True):
                    with st.spinner("Saving selected outreach rows to the outreach tracker..."):
                        saved = 0
                        skipped = 0
                        history_emails, history_keys = current_outreach_history(user_id)
                        for _, row in edited_bulk.iterrows():
                            if not bool(row.get("Send")):
                                continue
                            email = str(row.get("Email", "")).strip()
                            company_name = str(row.get("Company", ""))
                            contact_name = str(row.get("Contact Name", ""))
                            if is_duplicate_contact(
                                company_name,
                                contact_name,
                                email,
                                blocked_emails=history_emails,
                                blocked_identity_keys=history_keys,
                            ):
                                skipped += 1
                                continue
                            try:
                                log_draft_outreach_to_tracker(
                                    user_id,
                                    company=company_name,
                                    preferred_role=preferred_role,
                                    target_location=target_location,
                                    contact_name=contact_name,
                                    contact_link="",
                                    email=email,
                                    subject=str(row.get("Subject", "")),
                                    personalization=str(row.get("Personalization", row.get("Company Reason", ""))),
                                    sponsorship_signal=str(row.get("Sponsorship Signal", "Unknown / verify")),
                                    resume_name=st.session_state.get("resume_name", ""),
                                    send_source="bulk_campaign",
                                    role_source_url=str(row.get("Role Source URL", "")),
                                )
                                if email:
                                    history_emails.add(normalize_outreach_email(email))
                                history_keys.update(contact_identity_keys(company_name, contact_name, email))
                                saved += 1
                            except Exception:
                                skipped += 1
                    st.success(f"Saved {saved} row(s) to the outreach tracker ✅")
                    if skipped:
                        st.warning(f"Skipped {skipped} row(s) because they could not be saved.")
        st.markdown('</div>', unsafe_allow_html=True)

    with mode_tabs[2]:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.caption("This refreshes only when you click the button, so it will not spend Hunter/API credits on page load.")
        auto_discover = st.button("🔄 Run daily discovery now", type="primary", use_container_width=True)
        today_key = str(date.today())
        if st.session_state.get("daily_discovery_last_run", "") != today_key:
            st.caption("Daily discovery has not been refreshed today. Click the button above when you want to spend search/API credits.")
        if auto_discover:
            with st.spinner("Discovering companies, finding contacts, and drafting outreach..."):
                discovered = discover_companies_from_web(
                    preferred_role,
                    target_location,
                    max_results=min(discovery_limit, 16),
                    include_startups=include_startups,
                    only_open_roles=only_open_roles,
                )
                company_names = [d["company"] for d in discovered]
                daily_discovery_metadata = {company_key(item.get("company", "")): item for item in discovered}
                history_emails, history_keys = current_outreach_history(user_id)
                rows = build_bulk_outreach_rows(
                    company_names=company_names,
                    preferred_role=preferred_role,
                    linkedin_url=linkedin_url,
                    portfolio_url=portfolio_url,
                    candidate_summary=candidate_summary,
                    resume_text=st.session_state.get("resume_text", ""),
                    sender_name=sender_name,
                    location=target_location,
                    titles=outreach_titles,
                    seniorities=outreach_seniority,
                    max_contacts=5,
                    use_ai_personalization=niche_enabled,
                    discovery_metadata=daily_discovery_metadata,
                    blocked_emails=history_emails,
                    blocked_contact_keys=history_keys,
                )
            st.session_state["daily_outreach_queue"] = rows
            st.session_state["daily_discovery_last_run"] = today_key

        daily_rows = st.session_state.get("daily_outreach_queue", [])
        if daily_rows:
            daily_df = pd.DataFrame(daily_rows)
            edited_daily = st.data_editor(
                daily_df,
                use_container_width=True,
                column_config={
                    "Company Website": st.column_config.LinkColumn("Company Website"),
                    "Role Source URL": st.column_config.LinkColumn("Role Source URL"),
                    "Sponsorship Lookup": st.column_config.LinkColumn("Sponsorship Lookup"),
                },
                key="daily_queue_editor",
            )
            if st.button("📨 Send selected daily emails", use_container_width=True):
                try:
                    selected_rows = [
                        row for _, row in edited_daily.iterrows()
                        if bool(row.get("Send")) and str(row.get("Email", "")).strip()
                    ]
                    history_emails, _ = current_outreach_history(user_id)
                    send_rows, skipped_rows = unique_send_rows(
                        selected_rows,
                        already_contacted_emails=history_emails,
                    )
                    if not send_rows:
                        st.warning("No new daily emails to send. The selected rows were duplicates or were already contacted.")
                        st.stop()
                    if len(send_rows) > MAX_EMAILS_PER_BATCH:
                        st.error(f"Select {MAX_EMAILS_PER_BATCH} or fewer emails per batch to reduce accidental bulk sends.")
                        st.stop()
                    with st.spinner("Sending selected daily discovery emails through Gmail..."):
                        sender = GmailSender.from_user(user_id)
                        attachment = active_resume_attachment()
                        if not attachment.get("attachment_bytes"):
                            st.warning("No attachable resume file is active; selected emails will send without attachments.")
                        sent = 0
                        sent_emails = set()
                        log_failures = 0
                        for row in send_rows:
                            email = str(row.get("Email", "")).strip()
                            send_result = sender.send_email(
                                to=email,
                                subject=str(row.get("Subject", "")),
                                body_text=str(row.get("Body", "")),
                                body_html=plain_text_to_html(str(row.get("Body", ""))),
                                **attachment,
                            )
                            try:
                                log_sent_outreach_to_tracker(
                                    user_id,
                                    company=str(row.get("Company", "")),
                                    preferred_role=preferred_role,
                                    target_location=target_location,
                                    contact_name=str(row.get("Contact Name", "")),
                                    contact_link="",
                                    email=email,
                                    subject=str(row.get("Subject", "")),
                                    personalization=str(row.get("Personalization", row.get("Company Reason", ""))),
                                    sponsorship_signal=str(row.get("Sponsorship Signal", "Unknown / verify")),
                                    resume_name=st.session_state.get("resume_name", ""),
                                    send_source="daily_discovery",
                                    role_source_url=str(row.get("Role Source URL", "")),
                                    gmail_message_id=str(send_result.get("id", "") or ""),
                                    gmail_thread_id=str(send_result.get("threadId", "") or ""),
                                )
                            except Exception:
                                log_failures += 1
                            remember_outreach_contact(str(row.get("Company", "")), str(row.get("Contact Name", "")), email)
                            sent_emails.add(email)
                            sent += 1
                    st.session_state["daily_outreach_queue"] = mark_outreach_rows(
                        edited_daily,
                        sent_emails,
                        "Sent this session",
                    ).to_dict("records")
                    st.success(f"Sent {sent} daily discovery email(s) {attachment_status_label(attachment)} ✅")
                    if skipped_rows:
                        st.warning(f"Skipped {len(skipped_rows)} duplicate or already-contacted daily email(s).")
                    if log_failures:
                        st.warning(f"{log_failures} sent daily email(s) could not be auto-logged to the outreach tracker.")
                except Exception as e:
                    st.error(str(e))
        st.markdown('</div>', unsafe_allow_html=True)

    with mode_tabs[3]:
        st.markdown('<div class="card">', unsafe_allow_html=True)
        st.caption("This tracker is separate from the job application pipeline so cold outreach history stays isolated and easier to review.")
        use_ai_suggestions = st.checkbox(
            "Use AI to improve follow-up suggestions",
            value=True,
            help="If disabled, the tracker uses deterministic fallback suggestions based on reply state.",
            key="outreach_tracker_ai_suggestions",
        )
        sync_col, refresh_col, sync_help_col = st.columns([0.9, 1.0, 1.6])
        with sync_col:
            if st.button("🔄 Sync inbox now", type="primary", use_container_width=True):
                try:
                    with st.spinner("Syncing Gmail threads and checking for replies..."):
                        sync_results = sync_outreach_inbox(
                            user_id,
                            sender_name=sender_name,
                            candidate_summary=candidate_summary,
                            linkedin_url=linkedin_url,
                            portfolio_url=portfolio_url,
                            use_ai=use_ai_suggestions,
                        )
                    st.success(f"Synced {sync_results['synced']} tracked thread(s).")
                    if (
                        sync_results["replied"]
                        or sync_results["bounced"]
                        or sync_results["follow_up_due"]
                        or sync_results["suggested"]
                    ):
                        st.info(
                            "Detected "
                            f"{sync_results['replied']} replie(s), "
                            f"{sync_results['bounced']} bounce(s), and "
                            f"{sync_results['follow_up_due']} thread(s) needing follow-up. "
                            f"Updated {sync_results['suggested']} suggestion(s)."
                        )
                except Exception as e:
                    st.error(str(e))
        with refresh_col:
            if st.button("✨ Refresh suggestions", use_container_width=True):
                try:
                    with st.spinner("Generating outreach next-step suggestions..."):
                        refreshed = refresh_outreach_suggestions(
                            user_id,
                            sender_name=sender_name,
                            candidate_summary=candidate_summary,
                            linkedin_url=linkedin_url,
                            portfolio_url=portfolio_url,
                            use_ai=use_ai_suggestions,
                        )
                    st.success(f"Updated {refreshed} follow-up suggestion(s).")
                except Exception as e:
                    st.error(str(e))
        with sync_help_col:
            st.caption(
                "Inbox sync uses Gmail thread IDs captured after each send. "
                "If sync asks you to reconnect Gmail, your older token likely needs inbox-read access."
            )
        outreach_df = read_outreach_tracker_df(user_id)
        if outreach_df.empty:
            st.info("No outreach rows yet. Sent emails and saved drafts from Cold Outreach will appear here.")
        else:
            status_series = outreach_df["Status"].fillna("").astype(str)
            reply_status_series = outreach_df.get("Reply Status", pd.Series(dtype=str)).fillna("").astype(str)
            tracked_threads = outreach_df.get("Gmail Thread ID", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != ""
            suggestions_ready = outreach_df.get("Suggested Follow-up", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != ""
            followup_source = outreach_df.get("Next Follow-up At", outreach_df.get("Follow-up Date", pd.Series(dtype=str)))
            followup_dates = pd.to_datetime(followup_source, errors="coerce")
            today_ts = pd.Timestamp(date.today())
            o1, o2, o3, o4, o5, o6 = st.columns(6)
            o1.metric("Outreach rows", len(outreach_df))
            o2.metric("Tracked threads", int(tracked_threads.sum()))
            o3.metric("Replies detected", int((reply_status_series == "Replied").sum()))
            o4.metric("Needs follow-up", int((reply_status_series == "Needs follow-up").sum()))
            o5.metric("Bounces", int((reply_status_series == "Bounced").sum()))
            o6.metric("Suggestions ready", int(suggestions_ready.sum()))

            status_filter = st.multiselect(
                "Filter outreach status",
                sorted([status for status in status_series.unique().tolist() if status.strip()]),
                key="outreach_tracker_status_filter",
            )
            reply_filter = st.multiselect(
                "Filter reply status",
                sorted([status for status in reply_status_series.unique().tolist() if status.strip()]),
                key="outreach_tracker_reply_filter",
            )
            f1, f2, f3 = st.columns(3)
            with f1:
                only_replied = st.checkbox("Only show replied", key="outreach_only_replied")
            with f2:
                only_follow_up_due = st.checkbox("Only show follow-up due", key="outreach_only_followup_due")
            with f3:
                only_unsynced = st.checkbox("Only show unsynced", key="outreach_only_unsynced")
            filtered_outreach = outreach_df.copy()
            if status_filter:
                filtered_outreach = filtered_outreach[filtered_outreach["Status"].isin(status_filter)]
            if reply_filter:
                filtered_outreach = filtered_outreach[filtered_outreach["Reply Status"].isin(reply_filter)]
            if only_replied:
                filtered_outreach = filtered_outreach[filtered_outreach["Reply Status"] == "Replied"]
            if only_follow_up_due:
                due_mask = (
                    pd.to_datetime(filtered_outreach.get("Next Follow-up At", filtered_outreach.get("Follow-up Date", pd.Series(dtype=str))), errors="coerce") < today_ts
                ) & (~filtered_outreach["Reply Status"].isin(["Replied", "Bounced"]))
                filtered_outreach = filtered_outreach[due_mask.fillna(False)]
            if only_unsynced:
                last_synced = filtered_outreach.get("Last Synced At", pd.Series(dtype=str)).fillna("").astype(str).str.strip()
                has_thread = filtered_outreach.get("Gmail Thread ID", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != ""
                filtered_outreach = filtered_outreach[(last_synced == "") & has_thread]

            st.dataframe(
                filtered_outreach.drop(columns=["_row_id"], errors="ignore"),
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Role Source URL": st.column_config.LinkColumn("Role Source URL"),
                    "Contact Link": st.column_config.LinkColumn("Contact Link"),
                    "Suggested Action": st.column_config.TextColumn("Suggested Action", width="medium"),
                    "Suggested Follow-up": st.column_config.TextColumn("Suggested Follow-up", width="large"),
                    "Last Reply Snippet": st.column_config.TextColumn("Last Reply Snippet", width="large"),
                },
            )
            suggestion_rows = filtered_outreach[
                filtered_outreach.get("Suggested Follow-up", pd.Series(dtype=str)).fillna("").astype(str).str.strip() != ""
            ].head(5)
            if not suggestion_rows.empty:
                st.markdown("#### Suggested next steps")
                for _, suggestion_row in suggestion_rows.iterrows():
                    company_label = str(suggestion_row.get("Company", "") or "Unknown company")
                    reply_label = str(suggestion_row.get("Reply Status", "") or "No reply")
                    action_label = str(suggestion_row.get("Suggested Action", "") or "Next step suggestion")
                    with st.expander(f"{company_label} — {reply_label} — {action_label}", expanded=False):
                        if str(suggestion_row.get("Last Reply Snippet", "")).strip():
                            st.caption(f"Latest reply: {suggestion_row.get('Last Reply Snippet', '')}")
                        st.text_area(
                            "Suggested follow-up",
                            value=str(suggestion_row.get("Suggested Follow-up", "") or ""),
                            height=160,
                            key=f"suggested_followup_preview_{suggestion_row.get('_row_id', '')}",
                        )
            st.download_button(
                "⬇️ Download outreach tracker CSV",
                data=filtered_outreach.drop(columns=["_row_id"], errors="ignore").to_csv(index=False).encode("utf-8"),
                file_name="outreach_tracker.csv",
                mime="text/csv",
                use_container_width=True,
            )
        st.markdown('</div>', unsafe_allow_html=True)

# -------------------------
# TAB 3: PIPELINE TRACKER
# -------------------------
with tabs[2]:

    st.markdown(f"### 📊 Application Pipeline — {user_id}")
    st.caption("Track priorities, fit strength, follow-ups, and the next best action.")

    try:
        df = read_tracker_df(user_id)
    except Exception as e:
        st.error(f"Could not read tracker DB. Error: {e}")
        st.stop()

    topA, topB = st.columns([1, 3])
    with topA:
        if st.button("➕ Add job", type="primary", use_container_width=True):
            st.session_state["show_add_form"] = True
    with topB:
        if st.session_state.get("tracker_last_saved"):
            st.caption(f"Last saved: {st.session_state['tracker_last_saved']}")

    if st.session_state["show_add_form"]:
        with st.expander("Add a job", expanded=True):
            with st.form("add_job_form", clear_on_submit=True):
                a1, a2 = st.columns(2)
                with a1:
                    new_company = st.text_input("Company")
                    new_role = st.text_input("Role / Title")
                    new_job_link = st.text_input("Job Link")
                    new_location = st.text_input("Location")
                    new_status = st.selectbox(
                        "Status",
                        ["Interested", "Applied", "OA", "Interview", "Offer", "Rejected", "Ghosted"],
                        index=0,
                        key="manual_status",
                    )
                with a2:
                    new_date_applied = st.date_input("Date Applied", value=date.today(), key="manual_date_applied")
                    new_follow_up = st.date_input("Follow-up Date", value=date.today() + timedelta(days=7), key="manual_followup")
                    new_contact_name = st.text_input("Contact Name")
                    new_contact_link = st.text_input("Contact Link")
                    new_notes = st.text_area("Notes", height=90)

                cadd, ccancel = st.columns(2)
                with cadd:
                    submitted = st.form_submit_button("Add to tracker")
                with ccancel:
                    cancel = st.form_submit_button("Cancel")

                if cancel:
                    st.session_state["show_add_form"] = False
                    st.rerun()

                if submitted:
                    append_row(
                        user_id,
                        {
                            "Date Applied": str(new_date_applied),
                            "Company": new_company,
                            "Role": new_role,
                            "Job Link": new_job_link,
                            "Location": new_location,
                            "Status": new_status,
                            "Fit Score": "",
                            "Priority": "",
                            "Fit Summary": "",
                            "Resume Used": "",
                            "Follow-up Date": str(new_follow_up),
                            "Contact Name": new_contact_name,
                            "Contact Link": new_contact_link,
                            "Notes": new_notes,
                        },
                    )
                    st.session_state["show_add_form"] = False
                    st.success("Added ✅")
                    st.rerun()

    st.divider()

    if df.empty:
        st.info("Your tracker is empty. Add a job or generate a pack and log it.")
        st.stop()

    df = ensure_row_ids(df)

    today = date.today()
    df["_followup_dt"] = safe_date_parse(df.get("Follow-up Date", pd.Series([], dtype=str)))
    df["_due_today"] = df["_followup_dt"].apply(lambda d: d == today if pd.notna(d) else False)

    closed_statuses = {"Rejected", "Offer"}
    df["_overdue"] = df.apply(
        lambda r: (pd.notna(r["_followup_dt"]) and r["_followup_dt"] < today and str(r.get("Status", "")) not in closed_statuses),
        axis=1,
    )

    due_today_count = int(df["_due_today"].sum())
    overdue_count = int(df["_overdue"].sum())
    df["Next Action"] = df.apply(
        lambda r: recommend_follow_up_action(r.get("Status", ""), r.get("Follow-up Date", ""), today=today),
        axis=1,
    )

    if overdue_count > 0:
        st.markdown(
            f"""
            <div class="alert alert-danger">
              <b>⚠️ Follow-ups overdue:</b> {overdue_count}
              <div class="muted small">Open “Due & Overdue” below to view them.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif due_today_count > 0:
        st.markdown(
            f"""
            <div class="alert alert-warn">
              <b>⏰ Follow-ups due today:</b> {due_today_count}
              <div class="muted small">Open “Due & Overdue” below to view them.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """
            <div class="alert alert-ok">
              <b>✅ You're on track.</b>
              <div class="muted small">No follow-ups due today or overdue.</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("#### 📊 Overview")
    fit_numeric = pd.to_numeric(df.get("Fit Score", pd.Series([], dtype=str)), errors="coerce")
    avg_fit = round(float(fit_numeric.dropna().mean()), 1) if fit_numeric.notna().any() else None
    weekly_momentum = build_weekly_momentum_summary(df, today=today)
    weekly_activity_df = build_weekly_activity_df(df, today=today, weeks=8)
    funnel_df = build_pipeline_funnel_df(df)
    fit_outcome_df = build_fit_outcome_df(df)
    resume_performance_df = build_resume_performance_df(df)
    follow_up_timeline_df = build_follow_up_timeline_df(df, today=today)
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total", len(df))
    m2.metric("Applied", int((df["Status"].astype(str) == "Applied").sum()))
    m3.metric("Interview", int((df["Status"].astype(str) == "Interview").sum()))
    m4.metric("Offers", int((df["Status"].astype(str) == "Offer").sum()))
    m5.metric("Overdue", overdue_count)
    m6.metric("Avg fit", avg_fit if avg_fit is not None else "—")

    st.markdown("#### ⚡ Weekly momentum")
    week_label = f"{weekly_momentum['week_start']} to {weekly_momentum['week_end']}"
    st.caption(f"Current week window: {week_label}")
    week_delta = weekly_momentum["applications_this_week"] - weekly_momentum["applications_last_week"]
    followup_delta = weekly_momentum["followups_this_week"] - weekly_momentum["followups_last_week"]
    wm1, wm2, wm3, wm4 = st.columns(4)
    wm1.metric("Applied this week", weekly_momentum["applications_this_week"], delta=f"{week_delta:+d} vs last week")
    wm2.metric("Follow-ups due this week", weekly_momentum["followups_this_week"], delta=f"{followup_delta:+d} vs last week")
    wm3.metric("Interviews active", weekly_momentum["interviews_active"])
    wm4.metric("Offers total", weekly_momentum["offers_total"])

    weekly_area = alt.Chart(weekly_activity_df).mark_area(
        line={"color": "#7c8cff"},
        color=alt.Gradient(
            gradient="linear",
            stops=[
                alt.GradientStop(color="#7c8cff", offset=0),
                alt.GradientStop(color="rgba(124,140,255,0.08)", offset=1),
            ],
            x1=1,
            x2=1,
            y1=1,
            y2=0,
        ),
    ).encode(
        x=alt.X("Week Start:T", title="Week"),
        y=alt.Y("Applications:Q", title="Applications"),
        tooltip=[
            alt.Tooltip("Week Label:N", title="Week of"),
            alt.Tooltip("Applications:Q", title="Applications"),
        ],
    )
    weekly_points = alt.Chart(weekly_activity_df).mark_circle(size=90, color="#42c8ff").encode(
        x="Week Start:T",
        y="Applications:Q",
        tooltip=[
            alt.Tooltip("Week Label:N", title="Week of"),
            alt.Tooltip("Applications:Q", title="Applications"),
        ],
    )
    st.altair_chart(style_dashboard_chart(weekly_area + weekly_points, height=220), use_container_width=True)

    st.markdown("#### 🧭 Pipeline health")
    health_left, health_right = st.columns([1.05, 0.95])
    with health_left:
        funnel_max = max(int(funnel_df["Count"].max()), 1)
        funnel_base = alt.Chart(funnel_df).encode(
            y=alt.Y("Stage:N", sort=FUNNEL_STAGE_ORDER, title=None),
            x=alt.X(
                "Count:Q",
                title="Opportunities",
                scale=alt.Scale(domain=[0, funnel_max * 1.25]),
            ),
            color=alt.Color(
                "Stage:N",
                scale=alt.Scale(domain=FUNNEL_STAGE_ORDER, range=STATUS_COLOR_RANGE[: len(FUNNEL_STAGE_ORDER)]),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Stage:N", title="Stage"),
                alt.Tooltip("Count:Q", title="Count"),
                alt.Tooltip("Conversion Label:N", title="Conversion"),
            ],
        )
        funnel_chart = funnel_base.mark_bar(cornerRadiusEnd=8, size=26)
        funnel_labels = funnel_base.mark_text(
            align="left",
            baseline="middle",
            dx=8,
            color="#e2e8f0",
            fontWeight="bold",
        ).encode(text="Label:N")
        st.altair_chart(style_dashboard_chart(funnel_chart + funnel_labels, height=250), use_container_width=True)
        applied_count = int(funnel_df.loc[funnel_df["Stage"] == "Applied", "Count"].iloc[0]) if not funnel_df.empty else 0
        interview_count = int(funnel_df.loc[funnel_df["Stage"] == "Interview", "Count"].iloc[0]) if not funnel_df.empty else 0
        applied_to_interview = round((interview_count / applied_count) * 100, 1) if applied_count else 0.0
        st.caption(f"Applied to interview conversion in the current tracker: {applied_to_interview}%.")

    with health_right:
        timeline_chart = alt.Chart(follow_up_timeline_df).mark_bar(cornerRadiusEnd=8, size=28).encode(
            y=alt.Y("Bucket:N", sort=FOLLOW_UP_BUCKET_ORDER, title=None),
            x=alt.X("Count:Q", title="Jobs"),
            color=alt.Color(
                "Bucket:N",
                scale=alt.Scale(domain=FOLLOW_UP_BUCKET_ORDER, range=FOLLOW_UP_BUCKET_COLORS),
                legend=None,
            ),
            tooltip=[
                alt.Tooltip("Bucket:N", title="Window"),
                alt.Tooltip("Count:Q", title="Jobs"),
            ],
        )
        timeline_labels = alt.Chart(follow_up_timeline_df).mark_text(
            align="left",
            baseline="middle",
            dx=8,
            color="#e2e8f0",
            fontWeight="bold",
        ).encode(
            y=alt.Y("Bucket:N", sort=FOLLOW_UP_BUCKET_ORDER),
            x="Count:Q",
            text="Count:Q",
        )
        st.altair_chart(style_dashboard_chart(timeline_chart + timeline_labels, height=250), use_container_width=True)
        next_seven_days = int(
            follow_up_timeline_df[
                follow_up_timeline_df["Bucket"].isin(["Overdue", "Today", "Next 3 days", "Next 7 days"])
            ]["Count"].sum()
        )
        st.caption(f"{next_seven_days} opportunity(ies) need follow-up attention in the next 7 days.")

    st.markdown("#### 📈 Performance insights")
    perf_left, perf_right = st.columns(2)
    with perf_left:
        if fit_outcome_df.empty:
            st.caption("Fit score analytics will appear once rows contain fit scores.")
        else:
            mean_fit_score = round(float(fit_outcome_df["Fit Score"].mean()), 1)
            fit_rule = alt.Chart(pd.DataFrame({"Fit Score": [mean_fit_score]})).mark_rule(
                color="#94a3b8",
                strokeDash=[6, 4],
            ).encode(x="Fit Score:Q")
            fit_points = alt.Chart(fit_outcome_df).mark_circle(
                opacity=0.82,
                stroke="#081326",
                strokeWidth=0.8,
            ).encode(
                x=alt.X("Fit Score:Q", scale=alt.Scale(domain=[0, 100]), title="Fit score"),
                y=alt.Y("Status:N", sort=STATUS_DISPLAY_ORDER, title="Current stage"),
                color=alt.Color(
                    "Status:N",
                    scale=alt.Scale(domain=STATUS_COLOR_DOMAIN, range=STATUS_COLOR_RANGE),
                    legend=alt.Legend(title=None),
                ),
                size=alt.Size("Priority Score:Q", scale=alt.Scale(domain=[1, 4], range=[80, 260]), legend=None),
                tooltip=[
                    alt.Tooltip("Company:N", title="Company"),
                    alt.Tooltip("Role:N", title="Role"),
                    alt.Tooltip("Status:N", title="Status"),
                    alt.Tooltip("Fit Score:Q", title="Fit score", format=".1f"),
                    alt.Tooltip("Priority:N", title="Priority"),
                ],
            )
            st.altair_chart(style_dashboard_chart(fit_rule + fit_points, height=320), use_container_width=True)
            progressed = fit_outcome_df[fit_outcome_df["Status"].isin(["Interview", "Offer"])]
            if not progressed.empty:
                progressed_avg_fit = round(float(progressed["Fit Score"].mean()), 1)
                st.caption(f"Interview and offer stage roles currently average a {progressed_avg_fit} fit score.")
            else:
                st.caption(f"Average tracked fit score: {mean_fit_score}.")

    with perf_right:
        if resume_performance_df.empty:
            st.caption("Resume performance will appear once applications are logged with a resume name.")
        else:
            resume_long = resume_performance_df.melt(
                id_vars=["Resume", "Interview Rate"],
                value_vars=["Applications", "Interviews", "Offers"],
                var_name="Metric",
                value_name="Count",
            )
            ordered_resumes = resume_performance_df["Resume"].tolist()
            resume_chart = alt.Chart(resume_long).mark_bar(cornerRadiusEnd=7).encode(
                x=alt.X("Count:Q", title="Count"),
                y=alt.Y("Resume:N", sort=ordered_resumes, title=None),
                yOffset=alt.YOffset("Metric:N"),
                color=alt.Color(
                    "Metric:N",
                    scale=alt.Scale(
                        domain=["Applications", "Interviews", "Offers"],
                        range=["#7c8cff", "#42c8ff", "#38d7c1"],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("Resume:N", title="Resume"),
                    alt.Tooltip("Metric:N", title="Metric"),
                    alt.Tooltip("Count:Q", title="Count"),
                    alt.Tooltip("Interview Rate:Q", title="Interview rate", format=".1f"),
                ],
            )
            resume_chart_height = max(220, len(ordered_resumes) * 56)
            st.altair_chart(style_dashboard_chart(resume_chart, height=resume_chart_height), use_container_width=True)
            reliable_resumes = resume_performance_df[resume_performance_df["Applications"] >= 2].copy()
            best_resume_row = (
                reliable_resumes.sort_values(["Interview Rate", "Applications"], ascending=[False, False]).iloc[0]
                if not reliable_resumes.empty
                else resume_performance_df.iloc[0]
            )
            st.caption(
                f"Strongest tracked resume so far: {best_resume_row['Resume']} "
                f"({best_resume_row['Interview Rate']}% interview rate across {best_resume_row['Applications']} applications)."
            )

    st.markdown("#### 🚀 Top Opportunities")

    top_df = df.copy()
    top_df["__priority_rank"] = top_df.get("Priority", pd.Series("", index=top_df.index)).apply(priority_rank)
    top_df["__fit_num"] = pd.to_numeric(top_df.get("Fit Score", pd.Series("", index=top_df.index)), errors="coerce").fillna(0)

    top_df = top_df.sort_values(
        by=["__priority_rank", "__fit_num", "_overdue", "_due_today"],
        ascending=[False, False, False, False]
    )

    top_candidates = top_df.head(5)

    if top_candidates.empty:
        st.caption("No opportunities yet.")
    else:
        for _, r in top_candidates.iterrows():
            st.markdown(
                f"""
                <div class="soft-card">
                  <div><b>{r.get('Company', '')}</b> — {r.get('Role', '')} {render_status_pill(r.get('Status', ''))}</div>
                  <div class="muted small">
                    Priority: <b>{r.get('Priority', '') or '—'}</b> |
                    Fit Score: <b>{r.get('Fit Score', '') or '—'}</b>
                  </div>
                  <div class="muted small">Next action: <b>{r.get('Next Action', '')}</b></div>
                </div>
                """,
                unsafe_allow_html=True,
            )

    with st.expander("🧠 Pipeline insights", expanded=False):
        active_statuses = {"Interested", "Applied", "OA", "Interview"}
        active_count = int(df["Status"].astype(str).isin(active_statuses).sum())
        interview_like = int(df["Status"].astype(str).isin({"Interview", "Offer"}).sum())
        applied_like = int(df["Status"].astype(str).isin({"Applied", "OA", "Interview", "Offer", "Rejected", "Ghosted"}).sum())
        conversion_rate = round((interview_like / applied_like) * 100, 1) if applied_like else 0
        i1, i2, i3, i4 = st.columns(4)
        i1.metric("Active pipeline", active_count)
        i2.metric("Interview / offer rate", f"{conversion_rate}%")
        i3.metric("Follow-ups due", due_today_count + overdue_count)
        i4.metric("Applications this week", weekly_momentum["applications_this_week"])

        if not resume_performance_df.empty:
            reliable_resumes = resume_performance_df[resume_performance_df["Applications"] >= 2].copy()
            best_resume_row = (
                reliable_resumes.sort_values(["Interview Rate", "Applications"], ascending=[False, False]).iloc[0]
                if not reliable_resumes.empty
                else resume_performance_df.iloc[0]
            )
            st.caption(
                f"Best performing resume in the tracker: {best_resume_row['Resume']} "
                f"with an interview rate of {best_resume_row['Interview Rate']}%."
            )
        else:
            st.caption("Resume usage insights will appear once applications are logged with an active resume.")

    with st.expander("📅 Due & Overdue", expanded=True):
        colL, colR = st.columns(2)

        with colL:
            st.markdown("##### ⚠️ Overdue")
            overdue_df = df[df["_overdue"]].copy().sort_values("_followup_dt", ascending=True)
            if overdue_df.empty:
                st.caption("None 🎉")
            else:
                for _, r in overdue_df.head(15).iterrows():
                    st.markdown(
                        f"""
                        <div class="rowcard rowcard-danger">
                          <div><b>{r.get('Company','')}</b> — {r.get('Role','')} {render_status_pill(r.get('Status',''))}</div>
                          <div class="muted small">Follow-up date: <b>{r.get('Follow-up Date','')}</b></div>
                          <div class="muted small">Next action: <b>{r.get('Next Action','')}</b></div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        with colR:
            st.markdown("##### ⏰ Due today")
            due_df = df[df["_due_today"]].copy().sort_values("_followup_dt", ascending=True)
            if due_df.empty:
                st.caption("None")
            else:
                for _, r in due_df.head(15).iterrows():
                    st.markdown(
                        f"""
                        <div class="rowcard rowcard-warn">
                          <div><b>{r.get('Company','')}</b> — {r.get('Role','')} {render_status_pill(r.get('Status',''))}</div>
                          <div class="muted small">Follow-up date: <b>{r.get('Follow-up Date','')}</b></div>
                          <div class="muted small">Next action: <b>{r.get('Next Action','')}</b></div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        st.caption("Tip: Offers/Rejected are not flagged as overdue.")

    st.divider()

    st.markdown("#### 🔎 Filter")
    f1, f2, f3, f4, f5 = st.columns(5)
    with f1:
        statuses = sorted([s for s in df["Status"].dropna().unique().tolist() if str(s).strip() != ""])
        status_filter = st.multiselect("Status", statuses)
    with f2:
        priorities = sorted([p for p in df.get("Priority", pd.Series([], dtype=str)).dropna().unique().tolist() if str(p).strip() != ""])
        priority_filter = st.multiselect("Priority", priorities)
    with f3:
        company_search = st.text_input("Company contains")
    with f4:
        role_search = st.text_input("Role contains")
    with f5:
        followup_filter = st.selectbox("Follow-up", ["All", "Overdue only", "Due today only"], index=0)

    filtered = df.copy()
    if status_filter:
        filtered = filtered[filtered["Status"].isin(status_filter)]
    if priority_filter:
        filtered = filtered[filtered["Priority"].isin(priority_filter)]
    if company_search:
        filtered = filtered[filtered["Company"].astype(str).str.contains(company_search, case=False, na=False)]
    if role_search:
        filtered = filtered[filtered["Role"].astype(str).str.contains(role_search, case=False, na=False)]

    if followup_filter == "Overdue only":
        filtered = filtered[filtered["_overdue"]]
    elif followup_filter == "Due today only":
        filtered = filtered[filtered["_due_today"]]

    st.markdown("#### 🧾 Tracker table")
    st.markdown('<div class="card">', unsafe_allow_html=True)

    editor_df = filtered.drop(columns=["_followup_dt", "_due_today", "_overdue"], errors="ignore").copy()
    if "Delete" not in editor_df.columns:
        editor_df["Delete"] = False

    editor_df = _coerce_editor_types(editor_df)
    editor_df = editor_df.set_index("_row_id", drop=True)

    edited = st.data_editor(
        editor_df,
        use_container_width=True,
        hide_index=True,
        num_rows="fixed",
        column_config={
            "Delete": st.column_config.CheckboxColumn(
                "Delete",
                help="Tick and click Save changes to remove the row.",
                default=False,
            ),
            "Job Link": st.column_config.LinkColumn("Job Link"),
            "Contact Link": st.column_config.LinkColumn("Contact Link"),
            "Status": st.column_config.SelectboxColumn(
                "Status",
                options=["Interested", "Applied", "OA", "Interview", "Offer", "Rejected", "Ghosted"],
                required=False,
            ),
            "Priority": st.column_config.SelectboxColumn(
                "Priority",
                options=["Apply now", "Strong consider", "Apply if strategic", "Low priority"],
                required=False,
            ),
            "Next Action": st.column_config.TextColumn("Next Action", disabled=True),
        },
        key="tracker_editor",
    )

    st.markdown("</div>", unsafe_allow_html=True)

    a, b, c = st.columns([1, 1, 2])
    with a:
        if st.button("💾 Save changes", type="primary", use_container_width=True):
            try:
                with st.spinner("Saving tracker changes..."):
                    out = edited.reset_index().rename(columns={"index": "_row_id"})
                    out = out.drop(columns=["Next Action"], errors="ignore")
                    visible_row_ids = set(out["_row_id"].fillna("").astype(str))
                    if "Delete" in out.columns:
                        out = out[out["Delete"] != True].copy()
                        out = out.drop(columns=["Delete"], errors="ignore")

                    out = out.fillna("").astype(str)
                    merged_rows = merge_visible_tracker_edits(df, out, visible_row_ids)
                    overwrite_tracker_for_user(user_id, merged_rows)

                st.session_state["tracker_last_saved"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                st.success("Saved ✅")
                st.rerun()
            except Exception as e:
                st.error(f"Save failed. Error: {e}")

    with b:
        if st.button("🔄 Refresh", use_container_width=True):
            st.rerun()

    with c:
        st.caption("Tip: Use filters above to find overdue/due items quickly.")

    st.divider()
    download_df = filtered.drop(columns=["_row_id", "_followup_dt", "_due_today", "_overdue"], errors="ignore")
    d1, d2 = st.columns(2)
    with d1:
        st.download_button(
            "⬇️ Download CSV",
            data=download_df.to_csv(index=False).encode("utf-8"),
            file_name=f"{user_id}_tracker.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with d2:
        st.download_button(
            "⬇️ Download Excel (.xlsx)",
            data=df_to_xlsx_bytes(download_df),
            file_name=f"{user_id}_tracker.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
        )

    st.divider()
    st.markdown("#### 📥 Import")
    upload = st.file_uploader("Upload CSV", type=["csv"], key="tracker_upload")
    mode = st.radio("Import mode", ["Merge into tracker", "Replace tracker"], horizontal=True)
    dedupe = st.checkbox("De-dupe after merge (Company + Role + Job Link + Date Applied)", value=True)

    if st.button("Import now", use_container_width=True):
        if not upload:
            st.error("Please upload a CSV file first.")
        else:
            try:
                with st.spinner("Importing tracker CSV..."):
                    uploaded_df = pd.read_csv(upload)
                    if mode.startswith("Merge"):
                        merge_uploaded_csv(user_id, uploaded_df, dedupe=dedupe)
                        st.success("Imported via merge ✅")
                    else:
                        replace_with_uploaded_csv(user_id, uploaded_df)
                        st.success("Replaced tracker ✅")
                st.rerun()
            except Exception as e:
                st.error(f"Import failed. Make sure it’s a valid CSV. Error: {e}")
