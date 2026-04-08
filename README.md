# NextRole

AI-powered career operations app for internship and early-career job search workflows.

NextRole helps users choose the best resume version, analyze fit against a job post, generate tailored application materials, manage a private application tracker, and run recruiter outreach campaigns.

## Features

- Secure login, signup, password change, and account deletion with bcrypt hashing.
- Per-user Postgres tracker for applications, statuses, priorities, fit scores, follow-ups, contacts, notes, and resume used.
- Resume library with extracted text plus original file storage for email attachments.
- Job fit scoring and resume-version recommendations.
- AI-generated application pack with role summary, gaps, tailored resume bullets, cover letter paragraph, networking message, and interview talking points.
- Cold outreach workflows for individual contacts, bulk campaigns, role-based web company discovery, startup/open-role discovery, and explicit daily discovery runs.
- Company-specific outreach drafting so each recipient can receive a niche message based on company context, recipient title, role target, and resume highlights.
- Sponsorship signal columns with conservative historical H-1B sponsor lookup hints; users should verify current role-level sponsorship before applying.
- Gmail sending with optional resume attachment.
- Hunter contact discovery with local contact ranking.
- CSV and Excel export, CSV import, de-duplication, inline editing, and admin read-only overview.
- Pipeline insights for active pipeline, interview/offer rate, follow-up pressure, and resume usage.

## Project Structure

```text
.
|-- app.py              # Main Streamlit UI
|-- agent.py            # OpenAI application-pack generation
|-- auth.py             # Authentication and user profile preferences
|-- autofill.py         # Job field extraction
|-- db.py               # Postgres connection helper
|-- db_store.py         # Tracker and resume persistence
|-- gmail_sender.py     # Gmail API sender
|-- hunter_helper.py    # Hunter API client and contact ranking
|-- tools.py            # Fit scoring, job fetch, resume ranking helpers
|-- requirements.txt
`-- tests/
```

## Local Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Create `.env` locally or configure Streamlit secrets with the required values:

```bash
OPENAI_API_KEY=...
OPENAI_MODEL=gpt-4o-mini
OPENAI_OUTREACH_MODEL=gpt-4o-mini
DATABASE_URL=postgresql://...
HUNTER_API_KEY=...
GMAIL_SENDER_EMAIL=you@example.com
GMAIL_CLIENT_SECRET_FILE=/absolute/path/to/client_secret.json
GMAIL_TOKEN_FILE=/absolute/path/to/gmail_token.json
```

Optional Streamlit secrets:

```toml
admin_users = ["your_username"]
DATABASE_URL = "postgresql://..."
```

Run the app:

```bash
streamlit run app.py
```

## Deployment Notes

- Use Postgres-compatible storage such as Neon, Render Postgres, Railway Postgres, or Supabase Postgres.
- Store `DATABASE_URL`, `OPENAI_API_KEY`, `HUNTER_API_KEY`, and Gmail settings in deployment secrets.
- Do not commit `.env`, Streamlit secrets, Gmail tokens, Google client-secret JSON files, local DB files, or runtime tracker artifacts.
- Gmail OAuth token generation may require a local browser flow; for hosted deployments, pre-provision a token securely or replace the sender with a hosted email provider flow.

## Verification

Run a syntax check:

```bash
venv/bin/python -m py_compile app.py agent.py auth.py autofill.py db.py db_store.py tools.py gmail_sender.py hunter_helper.py
```

Run unit tests:

```bash
venv/bin/python -m unittest discover -s tests
```

## Security Notes

- Passwords are hashed with bcrypt.
- Tracker and resume data is scoped by `user_id`.
- Destructive account deletion verifies the password before wiping tracker/resume data.
- OAuth/client-secret files are ignored by Git; rotate credentials if they were ever exposed.
