import io
from datetime import date, timedelta, datetime

import pandas as pd
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

from db_store import (
    append_row,
    read_tracker_df,
    ensure_row_ids,
    overwrite_tracker_for_user,
    merge_uploaded_csv,
    replace_with_uploaded_csv,
    delete_all_for_user,
    admin_list_users,
    save_resume_version,
    list_resumes,
    load_resume_text,
    delete_resume_version,
    list_resume_payloads,
)

from auth import (
    create_user,
    verify_user,
    change_password,
    delete_user,
)

# -------------------------
# PAGE CONFIG
# -------------------------
st.set_page_config(
    page_title="NextRole",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
      .block-container {
        padding-top: 1.2rem;
        padding-bottom: 2.25rem;
        max-width: 1280px;
      }

      section[data-testid="stSidebar"] {
        padding-top: 1rem;
        border-right: 1px solid rgba(255,255,255,0.06);
      }

      h1, h2, h3 {
        letter-spacing: -0.02em;
      }

      .topbar {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 1rem;
      }

      .brand-wrap {
        display: flex;
        align-items: center;
        gap: 0.85rem;
      }

      .brand-badge {
        width: 46px;
        height: 46px;
        border-radius: 14px;
        display: flex;
        align-items: center;
        justify-content: center;
        font-size: 1.15rem;
        background: linear-gradient(135deg, rgba(59,130,246,0.22), rgba(99,102,241,0.20));
        border: 1px solid rgba(255,255,255,0.10);
        box-shadow: 0 10px 30px rgba(0,0,0,0.18);
      }

      .brand-title {
        font-size: 1.5rem;
        font-weight: 800;
        line-height: 1.05;
        letter-spacing: -0.03em;
      }

      .brand-subtitle {
        font-size: 0.93rem;
        color: rgba(255,255,255,0.68);
        margin-top: 0.08rem;
      }

      .hero {
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 24px;
        padding: 24px 24px 22px 24px;
        background:
          radial-gradient(circle at top left, rgba(59,130,246,0.20), transparent 38%),
          radial-gradient(circle at top right, rgba(16,185,129,0.12), transparent 34%),
          linear-gradient(180deg, rgba(255,255,255,0.06), rgba(255,255,255,0.03));
        margin-bottom: 1.15rem;
        box-shadow: 0 14px 40px rgba(0,0,0,0.16);
      }

      .hero-eyebrow {
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.12em;
        color: rgba(255,255,255,0.62);
        margin-bottom: 0.45rem;
      }

      .hero-title {
        font-size: 2rem;
        font-weight: 800;
        line-height: 1.08;
        max-width: 860px;
        margin-bottom: 0.6rem;
      }

      .hero-copy {
        font-size: 1rem;
        line-height: 1.55;
        color: rgba(255,255,255,0.76);
        max-width: 900px;
      }

      .hero-pills {
        margin-top: 1rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
      }

      .hero-pill {
        display: inline-block;
        padding: 0.34rem 0.7rem;
        border-radius: 999px;
        font-size: 0.83rem;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.05);
      }

      .card {
        border: 1px solid rgba(255,255,255,0.10);
        border-radius: 18px;
        padding: 16px 18px;
        background: rgba(255,255,255,0.035);
        margin-bottom: 0.85rem;
        box-shadow: 0 10px 30px rgba(0,0,0,0.10);
      }

      .soft-card {
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 18px;
        padding: 14px 16px;
        background: rgba(255,255,255,0.03);
        margin-bottom: 0.75rem;
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
        color: rgba(255,255,255,0.55);
        margin-bottom: 0.25rem;
      }

      .alert {
        border-radius: 18px;
        padding: 13px 15px;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(255,255,255,0.05);
        margin: 0.35rem 0 0.95rem 0;
      }

      .alert-danger {
        border-color: rgba(239, 68, 68, 0.42);
        background: rgba(239, 68, 68, 0.10);
      }

      .alert-warn {
        border-color: rgba(245, 158, 11, 0.42);
        background: rgba(245, 158, 11, 0.10);
      }

      .alert-ok {
        border-color: rgba(34, 197, 94, 0.34);
        background: rgba(34, 197, 94, 0.08);
      }

      .pill {
        display:inline-block;
        padding: 0.22rem 0.62rem;
        border-radius: 999px;
        font-size: 0.83rem;
        border: 1px solid rgba(255,255,255,0.12);
        background: rgba(255,255,255,0.06);
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

      .agent-card {
        border: 1px solid rgba(99,102,241,0.22);
        border-radius: 20px;
        padding: 18px 18px;
        background:
          linear-gradient(180deg, rgba(99,102,241,0.09), rgba(99,102,241,0.05));
        margin: 0.8rem 0 1rem 0;
        box-shadow: 0 12px 30px rgba(0,0,0,0.12);
      }

      .agent-title {
        font-size: 1rem;
        font-weight: 700;
        letter-spacing: -0.01em;
      }

      .rowcard {
        border-radius: 18px;
        padding: 13px 15px;
        border: 1px solid rgba(255,255,255,0.10);
        background: rgba(255,255,255,0.04);
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

      div.stButton > button,
      div.stDownloadButton > button {
        border-radius: 14px;
        padding: 0.62rem 1rem;
        border: 1px solid rgba(255,255,255,0.12);
      }

      div.stTextInput > div > div > input,
      div.stTextArea textarea,
      div[data-baseweb="select"] > div,
      div.stDateInput > div > div input {
        border-radius: 14px !important;
      }

      [data-testid="stMetric"] {
        background: rgba(255,255,255,0.035);
        border: 1px solid rgba(255,255,255,0.08);
        padding: 0.9rem 0.95rem;
        border-radius: 18px;
      }

      [data-testid="stDataFrame"], [data-testid="stDataEditor"] {
        border-radius: 18px;
        overflow: hidden;
      }

      @media (max-width: 900px) {
        .hero-title {
          font-size: 1.45rem;
        }
        .brand-title {
          font-size: 1.28rem;
        }
      }
    </style>
    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    <div class="topbar">
      <div class="brand-wrap">
        <div class="brand-badge">🎯</div>
        <div>
          <div class="brand-title">NextRole</div>
          <div class="brand-subtitle">AI career operations agent for smarter applications</div>
        </div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

tabs = st.tabs(["✨ Job Agent", "📊 Pipeline Tracker"])

st.markdown(
    """
    <div class="hero">
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
        "Fit Score", "Priority", "Fit Summary",
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
                        st.session_state["resume_text"] = best_resume["resume_text"]
                        st.session_state["resume_name"] = best_resume["name"]
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
                    txt = load_resume_text(user_id, selected_rid)
                    st.session_state["resume_text"] = txt
                    st.session_state["resume_name"] = resumes_df.loc[
                        resumes_df["resume_id"] == selected_rid, "name"
                    ].values[0]
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
                if resume_file.type == "application/pdf":
                    parsed = read_pdf(resume_file)
                else:
                    parsed = read_txt(resume_file)

                st.session_state["resume_text"] = parsed
                st.session_state["resume_name"] = resume_file.name

                if st.button("Save as new version", use_container_width=True):
                    save_resume_version(user_id, resume_file.name, parsed)
                    st.success("Saved ✅")
                    st.rerun()

            except Exception as e:
                st.error(f"Could not read resume file. Error: {e}")

        c1, c2 = st.columns([1, 1])
        with c1:
            if st.button("🧹 Clear active resume", use_container_width=True):
                st.session_state["resume_text"] = ""
                st.session_state["resume_name"] = ""
                st.success("Cleared ✅")
                st.rerun()
        with c2:
            st.caption(
                f"Active: **{st.session_state['resume_name']}**"
                if st.session_state["resume_name"]
                else "No active resume selected."
            )

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
    m1, m2, m3, m4, m5, m6 = st.columns(6)
    m1.metric("Total", len(df))
    m2.metric("Applied", int((df["Status"].astype(str) == "Applied").sum()))
    m3.metric("Interview", int((df["Status"].astype(str) == "Interview").sum()))
    m4.metric("Offers", int((df["Status"].astype(str) == "Offer").sum()))
    m5.metric("Overdue", overdue_count)
    m6.metric("Avg fit", avg_fit if avg_fit is not None else "—")

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

    st.markdown("#### 📈 Trends")

    status_counts = df["Status"].fillna("").astype(str).replace("", "Unknown").value_counts().reset_index()
    status_counts.columns = ["Status", "Count"]

    st.altair_chart(
        alt.Chart(status_counts)
        .mark_bar()
        .encode(x=alt.X("Status:N", sort="-y"), y="Count:Q", tooltip=["Status", "Count"]),
        use_container_width=True,
    )

    tmp = df.copy()
    tmp["Date Applied"] = pd.to_datetime(tmp["Date Applied"], errors="coerce")
    tmp = tmp.dropna(subset=["Date Applied"])
    if not tmp.empty:
        daily = tmp.groupby(tmp["Date Applied"].dt.date).size().reset_index(name="Count")
        daily.columns = ["Date", "Count"]
        st.altair_chart(
            alt.Chart(daily)
            .mark_line(point=True)
            .encode(x="Date:T", y="Count:Q", tooltip=["Date", "Count"]),
            use_container_width=True,
        )

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
                out = edited.reset_index().rename(columns={"index": "_row_id"})
                out = out.drop(columns=["Next Action"], errors="ignore")
                if "Delete" in out.columns:
                    out = out[out["Delete"] != True].copy()
                    out = out.drop(columns=["Delete"], errors="ignore")

                out = out.fillna("").astype(str)
                overwrite_tracker_for_user(user_id, out)

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