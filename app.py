import base64
import io
import mimetypes
from html import escape
from pathlib import Path
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
from agent import generate_application_materials
from dashboard_metrics import (
    FIT_FILTER_OPTIONS,
    FOLLOW_UP_BUCKET_COLORS,
    FOLLOW_UP_BUCKET_ORDER,
    FUNNEL_STAGE_ORDER,
    STATUS_COLOR_DOMAIN,
    STATUS_COLOR_RANGE,
    STATUS_DISPLAY_ORDER,
    build_dashboard_trend_badges,
    build_fit_outcome_df,
    build_follow_up_timeline_df,
    build_pipeline_funnel_df,
    build_resume_performance_df,
    build_weekly_activity_df,
    build_weekly_momentum_summary,
    filter_tracker_rows,
    pick_best_resume_summary,
)
from db_store import (
    append_row,
    read_tracker_df,
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
)

from auth import (
    create_user,
    verify_user,
    change_password,
    delete_user,
)


load_dotenv()

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

      .dashboard-badge-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.72rem;
        margin: 0.45rem 0 0.9rem 0;
      }

      .dashboard-badge {
        min-width: 188px;
        flex: 1 1 188px;
        border-radius: 18px;
        border: 1px solid rgba(148, 163, 184, 0.14);
        padding: 0.85rem 0.95rem;
        background: linear-gradient(180deg, rgba(255,255,255,0.05), rgba(255,255,255,0.025));
        box-shadow: 0 12px 24px rgba(0,0,0,0.14);
      }

      .dashboard-badge-label {
        font-size: 0.72rem;
        text-transform: uppercase;
        letter-spacing: 0.1em;
        color: rgba(224, 231, 255, 0.62);
        margin-bottom: 0.38rem;
      }

      .dashboard-badge-value {
        font-size: 0.98rem;
        font-weight: 700;
        line-height: 1.35;
        color: rgba(248, 250, 252, 0.95);
      }

      .dashboard-badge-positive {
        border-color: rgba(56, 215, 193, 0.26);
        background: linear-gradient(180deg, rgba(56,215,193,0.13), rgba(255,255,255,0.025));
      }

      .dashboard-badge-warn {
        border-color: rgba(245, 158, 11, 0.30);
        background: linear-gradient(180deg, rgba(245,158,11,0.13), rgba(255,255,255,0.025));
      }

      .dashboard-badge-accent {
        border-color: rgba(124, 140, 255, 0.30);
        background: linear-gradient(180deg, rgba(124,140,255,0.14), rgba(255,255,255,0.025));
      }

      .dashboard-badge-neutral {
        border-color: rgba(148, 163, 184, 0.18);
      }

      .dashboard-toolbar-note {
        font-size: 0.86rem;
        color: rgba(226, 232, 240, 0.7);
        margin: 0.12rem 0 0.55rem 0;
      }

      .filter-chip-row {
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        margin: 0.35rem 0 0.3rem 0;
      }

      .filter-chip {
        display: inline-flex;
        align-items: center;
        gap: 0.38rem;
        padding: 0.42rem 0.74rem;
        border-radius: 999px;
        border: 1px solid rgba(148, 163, 184, 0.16);
        background: rgba(255,255,255,0.05);
        color: rgba(241, 245, 249, 0.88);
        font-size: 0.82rem;
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
          <div class="brand-subtitle">AI workflow for targeted applications, resume strategy, and follow-up execution.</div>
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

tabs = st.tabs(["✨ Job Agent", "📊 Pipeline Tracker"])

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
            <span class="hero-flow-copy">Track applications, follow-up dates, resume usage, and pipeline momentum.</span>
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
        </div>
        """,
        unsafe_allow_html=True,
    )

    if st.button("Log out", use_container_width=True):
        st.session_state["user_id"] = ""
        st.rerun()


# -------------------------
# ACCOUNT SETTINGS
# -------------------------
with st.sidebar.expander("⚙️ Account settings", expanded=False):
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


def render_dashboard_badges(badges: list[dict[str, str]]) -> None:
    if not badges:
        return
    html = ['<div class="dashboard-badge-row">']
    for badge in badges:
        tone = escape(str(badge.get("tone", "neutral")))
        label = escape(str(badge.get("label", "")))
        value = escape(str(badge.get("value", "")))
        html.append(
            f'<div class="dashboard-badge dashboard-badge-{tone}">'
            f'<div class="dashboard-badge-label">{label}</div>'
            f'<div class="dashboard-badge-value">{value}</div>'
            f"</div>"
        )
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)


def apply_tracker_filter_preset(preset: str) -> None:
    state = {
        "tracker_status_filter": [],
        "tracker_priority_filter": [],
        "tracker_company_search": "",
        "tracker_role_search": "",
        "tracker_followup_filter": "All",
        "tracker_fit_filter": "All",
    }
    if preset == "applied":
        state["tracker_status_filter"] = ["Applied"]
    elif preset == "interview":
        state["tracker_status_filter"] = ["Interview"]
    elif preset == "offers":
        state["tracker_status_filter"] = ["Offer"]
    elif preset == "overdue":
        state["tracker_followup_filter"] = "Overdue only"
    elif preset == "strong_fit":
        state["tracker_fit_filter"] = "Strong fit (80+)"

    for key, value in state.items():
        st.session_state[key] = value


def render_active_tracker_filters(
    *,
    status_filter: list[str],
    priority_filter: list[str],
    company_search: str,
    role_search: str,
    followup_filter: str,
    fit_filter: str,
) -> None:
    chips: list[str] = []
    if status_filter:
        chips.append(f"Status: {', '.join(status_filter)}")
    if priority_filter:
        chips.append(f"Priority: {', '.join(priority_filter)}")
    if company_search:
        chips.append(f"Company: {company_search}")
    if role_search:
        chips.append(f"Role: {role_search}")
    if followup_filter != "All":
        chips.append(f"Follow-up: {followup_filter}")
    if fit_filter != "All":
        chips.append(f"Fit: {fit_filter}")

    if not chips:
        st.caption("No active filters. The full pipeline is shown below.")
        return

    html = ['<div class="filter-chip-row">']
    for chip in chips:
        html.append(f'<span class="filter-chip">{chip}</span>')
    html.append("</div>")
    st.markdown("".join(html), unsafe_allow_html=True)



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



# -------------------------
# TAB 2: PIPELINE TRACKER
# -------------------------
with tabs[1]:

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
    best_resume_summary = pick_best_resume_summary(resume_performance_df)
    dashboard_badges = build_dashboard_trend_badges(
        weekly_momentum,
        funnel_df,
        follow_up_timeline_df,
        resume_performance_df,
    )
    applied_count = int((df["Status"].astype(str) == "Applied").sum())
    interview_count = int((df["Status"].astype(str) == "Interview").sum())
    offer_count = int((df["Status"].astype(str) == "Offer").sum())
    high_fit_count = int((fit_numeric >= 80).sum()) if fit_numeric.notna().any() else 0
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total", len(df))
    m2.metric("Applied", applied_count)
    m3.metric("Interview", interview_count)
    m4.metric("Offers", offer_count)
    m5.metric("Overdue", overdue_count)
    m6.metric("Avg fit", avg_fit if avg_fit is not None else "—")
    render_dashboard_badges(dashboard_badges)
    st.markdown('<div class="dashboard-toolbar-note">Click a quick view to prefilter the tracker table below.</div>', unsafe_allow_html=True)
    q1, q2, q3, q4, q5, q6 = st.columns(6)
    with q1:
        if st.button(f"All • {len(df)}", key="tracker_preset_all", use_container_width=True):
            apply_tracker_filter_preset("all")
            st.rerun()
    with q2:
        if st.button(f"Applied • {applied_count}", key="tracker_preset_applied", use_container_width=True):
            apply_tracker_filter_preset("applied")
            st.rerun()
    with q3:
        if st.button(f"Interview • {interview_count}", key="tracker_preset_interview", use_container_width=True):
            apply_tracker_filter_preset("interview")
            st.rerun()
    with q4:
        if st.button(f"Offers • {offer_count}", key="tracker_preset_offers", use_container_width=True):
            apply_tracker_filter_preset("offers")
            st.rerun()
    with q5:
        if st.button(f"Overdue • {overdue_count}", key="tracker_preset_overdue", use_container_width=True):
            apply_tracker_filter_preset("overdue")
            st.rerun()
    with q6:
        if st.button(f"Strong Fit • {high_fit_count}", key="tracker_preset_strong_fit", use_container_width=True):
            apply_tracker_filter_preset("strong_fit")
            st.rerun()

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
        funnel_applied_count = int(funnel_df.loc[funnel_df["Stage"] == "Applied", "Count"].iloc[0]) if not funnel_df.empty else 0
        funnel_interview_count = int(funnel_df.loc[funnel_df["Stage"] == "Interview", "Count"].iloc[0]) if not funnel_df.empty else 0
        applied_to_interview = round((funnel_interview_count / funnel_applied_count) * 100, 1) if funnel_applied_count else 0.0
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
            st.caption(
                f"Strongest tracked resume so far: {best_resume_summary['Resume']} "
                f"({best_resume_summary['Interview Rate']:.1f}% interview rate across {best_resume_summary['Applications']} applications)."
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

        if best_resume_summary:
            st.caption(
                f"Best performing resume in the tracker: {best_resume_summary['Resume']} "
                f"with an interview rate of {best_resume_summary['Interview Rate']:.1f}%."
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

    st.markdown("#### 🔎 Table filters")
    st.caption("Use the controls below for precise views, or start with the quick filters above.")
    f1, f2, f3, f4, f5, f6 = st.columns(6)
    with f1:
        statuses = sorted([s for s in df["Status"].dropna().unique().tolist() if str(s).strip() != ""])
        status_filter = st.multiselect("Status", statuses, key="tracker_status_filter")
    with f2:
        priorities = sorted([p for p in df.get("Priority", pd.Series([], dtype=str)).dropna().unique().tolist() if str(p).strip() != ""])
        priority_filter = st.multiselect("Priority", priorities, key="tracker_priority_filter")
    with f3:
        company_search = st.text_input("Company contains", key="tracker_company_search")
    with f4:
        role_search = st.text_input("Role contains", key="tracker_role_search")
    with f5:
        followup_filter = st.selectbox(
            "Follow-up",
            ["All", "Overdue only", "Due today only"],
            index=0,
            key="tracker_followup_filter",
        )
    with f6:
        fit_filter = st.selectbox("Fit band", FIT_FILTER_OPTIONS, index=0, key="tracker_fit_filter")

    filter_a, filter_b = st.columns([5, 1])
    with filter_a:
        render_active_tracker_filters(
            status_filter=status_filter,
            priority_filter=priority_filter,
            company_search=company_search,
            role_search=role_search,
            followup_filter=followup_filter,
            fit_filter=fit_filter,
        )
    with filter_b:
        if st.button("Clear filters", key="tracker_clear_filters", use_container_width=True):
            apply_tracker_filter_preset("all")
            st.rerun()

    filtered = filter_tracker_rows(
        df,
        status_filter=status_filter,
        priority_filter=priority_filter,
        company_search=company_search,
        role_search=role_search,
        followup_filter=followup_filter,
        fit_filter=fit_filter,
    )

    st.markdown("#### 🧾 Tracker table")
    st.caption(f"Showing {len(filtered)} of {len(df)} tracked opportunity(ies).")
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
