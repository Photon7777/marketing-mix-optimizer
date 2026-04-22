# Mixalyzer

AI-powered marketing mix intelligence for budget planning, ROI analysis, and executive-ready recommendations.

Mixalyzer helps growth and finance teams answer a practical business question: which marketing channels are actually driving revenue, and how should next-period budget be allocated?

## Product Highlights

- Marketing mix dashboard with spend, revenue, ROI, contribution, CAC, and channel mix.
- Auto column mapping for uploaded CSV files.
- Data readiness scoring with checks for required fields, date quality, history length, numeric quality, CAC support, and spend coverage.
- Ridge MMM with adstock/carryover, saturation-style features, seasonality, and trend.
- Lightweight Bayesian MMM option with posterior-style confidence intervals.
- Model comparison across average baseline, seasonality baseline, Ridge MMM, and Bayesian MMM.
- Budget simulation with conservative, expected, and optimistic revenue impact ranges.
- Budget optimizer with recommended channel allocation.
- Responsible AI and risk audit for privacy, bias, model reliability, and hallucination controls.
- Executive report and allocation CSV downloads.
- Optional OpenAI-generated recommendation narrative.

## Run Locally

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
streamlit run mmx_app.py
```

The app runs with built-in sample data. You can also upload a CSV and use the auto-mapping panel to align your fields to Mixalyzer's schema.

## Streamlit Cloud

Use these deployment settings:

- Repository: `Photon7777/marketing-mix-optimizer`
- Branch: `main`
- Main file path: `mmx_app.py`

Optional Streamlit secret for the AI narrative toggle:

```toml
OPENAI_API_KEY = "your_key_here"
```

## CSV Schema

Recommended columns:

```text
date
revenue
google_ads
meta_ads
instagram_ads
tv_ads
email_marketing
promotions
new_customers
```

`new_customers` is optional, but it enables CAC reporting.

## Project Structure

```text
.
|-- marketing_mix_model.py          # MMM modeling, readiness, mapping, evaluation, optimization
|-- mmx_app.py                      # Streamlit Mixalyzer product
|-- requirements.txt
`-- tests/
    `-- test_marketing_mix_model.py
```

## Verification

Run syntax checks:

```bash
venv/bin/python -m py_compile marketing_mix_model.py mmx_app.py
```

Run tests:

```bash
venv/bin/python -m unittest discover -s tests
```

## Responsible AI Notes

- Use aggregated weekly marketing data; do not upload customer-level identifiers.
- Treat recommendations as decision support, not automated media-buying instructions.
- Validate major reallocations with experiments, lift studies, or business review.
- Monitor model error and data drift before using the optimizer in production.
