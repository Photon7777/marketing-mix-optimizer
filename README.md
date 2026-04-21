# NextRole

AI-powered career operations app for internship and early-career job search workflows.

NextRole helps users choose the best resume version, analyze fit against a job post, generate tailored application materials, and manage a private application tracker.

## Features

- Secure login, signup, password change, and account deletion with bcrypt hashing.
- Per-user Postgres tracker for applications, statuses, priorities, fit scores, follow-ups, contacts, notes, and resume used.
- Resume library with extracted text plus original file storage.
- Job fit scoring and resume-version recommendations.
- AI-generated application pack with role summary, gaps, tailored resume bullets, cover letter paragraph, networking message, and interview talking points.
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
DATABASE_URL=postgresql://...
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
- Store `DATABASE_URL` and `OPENAI_API_KEY` in deployment secrets.
- Do not commit `.env`, Streamlit secrets, local DB files, or runtime tracker artifacts.

## Verification

Run a syntax check:

```bash
venv/bin/python -m py_compile app.py agent.py auth.py autofill.py db.py db_store.py tools.py
```

Run unit tests:

```bash
venv/bin/python -m unittest discover -s tests
```

## Security Notes

- Passwords are hashed with bcrypt.
- Tracker and resume data is scoped by `user_id`.
- Destructive account deletion verifies the password before wiping tracker/resume data.
